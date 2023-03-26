import os
import sys
import time
ROOT_DIR = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(ROOT_DIR)
import pandas as pd
import sqlite3
import numpy as np
from typing import List, Tuple
from app.storage.sqlite_connector import Database

# Queries

QUERY_BROKER_TX = """
select * from broker_transaction 
--where 종목코드 in ('347770','005930')
"""

QUERY_SETTLEMENT = """
select "날짜", "시간", "종목코드", "현재가", "누적거래대금", "거래대금증감", "전일동시간거래량비율", "체결강도", "시가총액" from settlement 
--where 종목코드 in ('347770','005930')
"""

# Functions for df processing
def calc_hhf(row) -> float:
    w = row / row.sum()
    return (w*w).sum()

def calc_hhf_without_kw(entire_row) -> Tuple[float, float]:
    kw_buy_rank = entire_row["kw_buy_rank"]
    kw_sell_rank = entire_row["kw_sell_rank"]
    kw_buy_index = ["매수거래원수량1", "매수거래원수량2", "매수거래원수량3", "매수거래원수량4", "매수거래원수량5"]
    kw_sell_index = ["매도거래원수량1", "매도거래원수량2", "매도거래원수량3", "매도거래원수량4", "매도거래원수량5"]

    if kw_buy_rank and ~np.isnan(kw_buy_rank):
        kw_buy_index.pop(int(kw_buy_rank)-1)
    if kw_sell_rank and ~np.isnan(kw_sell_rank):
        kw_sell_index.pop(int(kw_sell_rank)-1)
    
    buy_sum, sell_sum = 0,0
    buy_squared_sum, sell_squared_sum = 0,0
    for i in kw_buy_index:
        buy_sum += entire_row[i]
        buy_squared_sum += entire_row[i]*entire_row[i]
    for i in kw_sell_index:
        sell_sum += entire_row[i]
        sell_squared_sum += entire_row[i]*entire_row[i]
    
    return sell_squared_sum/(sell_sum*sell_sum), buy_squared_sum/(buy_sum*buy_sum)

def get_kw_rank(entire_row) -> Tuple[int, int]:
    buy_rank, sell_rank = None, None
    for i in range(1,6):
        if buy_rank != None and sell_rank != None:
            break
        if entire_row[f"매도거래원{i}"] == "키움증권":
            sell_rank = i
        if entire_row[f"매수거래원{i}"] == "키움증권":
            buy_rank = i
    
    return buy_rank, sell_rank

class BrokerTrend:
    def __init__(self, date: str, code: List[str] = []):
        self.date = date.replace("-", "")
        self.codes = code
        self.db: Database = Database(f"{ROOT_DIR}/test/{self.date}.sqlite3")
    
    def find_signals(self) -> pd.DataFrame:
        # set tx volume threshold
        tx_volume_threshold_lower = 20*100000000
        tx_volume_threshold_upper = 100*100000000
        # get broker trend & filter by tx volume
        broker_trend = self.get_broker_trend()
        avg_volume = broker_trend.groupby("종목코드")["전일거래량"].mean()
        target_stocks = avg_volume[(avg_volume > tx_volume_threshold_lower) & (avg_volume < tx_volume_threshold_upper)].index.to_list()
        target_broker_trend = broker_trend[broker_trend["종목코드"].isin(target_stocks)].copy()

        target_broker_trend["hhf_diff"] = target_broker_trend["hhf_buy"] - target_broker_trend["hhf_sell"]
        target_broker_trend["hhf_diff_without_kw"] = target_broker_trend["hhf_buy_without_kw"] - target_broker_trend["hhf_sell_without_kw"]

        # Filter하는 부분 시작
        target_broker_trend["target_metric"] = target_broker_trend["hhf_diff_without_kw"]*target_broker_trend["전일동시간거래량비율"]/100
        target_broker_trend["signal_buy"] =\
            (target_broker_trend["target_metric"]>0.2) &\
            (target_broker_trend["kw_buy_rank"] != 1) &\
            (target_broker_trend["시간"] >= "093000")
        # Filter하는 부분 끝

        target_broker_trend["signal_buy"] = target_broker_trend["signal_buy"].replace(False, None)
        target_broker_trend.update(target_broker_trend.groupby(["종목코드"])["signal_buy"].ffill())

        signals = target_broker_trend[target_broker_trend["signal_buy"].notnull()]
        hhf = signals.groupby('종목코드')[["시가총액","시간","target_metric"]].first()
        first = signals.groupby('종목코드')["현재가"].first().rename("매수가")
        last = signals.groupby('종목코드')["현재가"].last().rename("종가")
        min = signals.groupby('종목코드')["현재가"].min().rename("저가")
        max = signals.groupby('종목코드')["현재가"].max().rename("고가")

        signals = pd.concat([hhf, first, last, min, max], axis=1).reset_index()

        def get_metrics(row):
            r_end = row["종가"] / row["매수가"] -1
            r_max = row["고가"] / row["매수가"] -1
            r_min = row["저가"] / row["매수가"] -1

            return round(r_end*100,2), round(r_max*100,2), round(r_min*100,2)
        
        signals[["종가수익률", "최고수익률", "최저수익률"]] = signals.apply(lambda row: pd.Series(get_metrics(row)), axis=1)
        # sort signals
        signals = signals.sort_values(by=["target_metric"], ascending=False)
        print(signals)
        print(signals[["종가수익률", "최고수익률", "최저수익률"]].mean())
        # signals.to_csv(f'{ROOT_DIR}/result/broker_signals.csv', encoding='utf-8', index=False)

       

    def get_broker_trend(self) -> pd.DataFrame:
        try:
            final_df = pd.read_csv(f'{ROOT_DIR}/result/broker_trend_{self.date}.csv', encoding='utf-8', dtype={'날짜': object, '시간': object, '종목코드': object})
        except FileNotFoundError:
            hhf_df = self.get_broker_hhf()
            settlement = self.read_settlement_from_db()
            # time it
            start = time.time()
            final_df = pd.merge(left = settlement , right = hhf_df, how = "outer", on = ["날짜", "시간", "종목코드"]).sort_values(by=['종목코드', '날짜', '시간']) 
            # group by and ffill final_df and then bfill
            final_df.update(final_df.groupby(['종목코드']).ffill())
            final_df.to_csv(f'{ROOT_DIR}/result/broker_trend_{self.date}.csv', encoding='utf-8', index=False)
            print("merging with settlement data completed in %s seconds ---" % (time.time() - start))
        return final_df
    
    # read from app/result.csv if it exists or read from sqlite3 database
    def get_broker_hhf(self):
        try:
            df = pd.read_csv(f'{ROOT_DIR}/result/broker_hhf_{self.date}.csv', encoding='utf-8', dtype={'날짜': object, '시간': object, '종목코드': object})
        except FileNotFoundError:
            df = self.process_broker_tx()
            df.to_csv(f'{ROOT_DIR}/result/broker_hhf_{self.date}.csv', encoding='utf-8', index=False)
        return df
    
    def read_broker_tx_from_db(self):
        # select from broker_transaction table. if code is not none, select only code
        if self.codes:
            query_broker = QUERY_BROKER_TX + "where 종목코드 in (?)"
        else:
            query_broker = QUERY_BROKER_TX
        # fetch from db using query
        broker_tx =  pd.read_sql_query(query_broker, self.db.conn, params=tuple(self.codes))

        return broker_tx

    def read_settlement_from_db(self):
        # time it
        start = time.time()
        # select from broker_transaction table. if code is not none, select only code
        if self.codes:
            query_settlement = QUERY_SETTLEMENT + "where 종목코드 in (?)"
        else:
            query_settlement = QUERY_SETTLEMENT
        # fetch from db using query
        settlement =  pd.read_sql_query(query_settlement, self.db.conn, params=tuple(self.codes))
        settlement = settlement.drop_duplicates(["날짜", "시간", "종목코드"], keep="last")
        settlement["전일거래량"] = settlement["누적거래대금"]*1000000. - settlement["거래대금증감"]
        # time it
        print("read_settlement_from_db completed in %s seconds ---" % (time.time() - start))

        return settlement
    
    def process_broker_tx(self) -> pd.DataFrame:
        # time it
        start = time.time()
        broker_tx = self.read_broker_tx_from_db()
        broker_tx[["kw_sell_rank", "kw_buy_rank"]] = broker_tx.apply(lambda row: pd.Series(get_kw_rank(row)), axis=1)

        top_4_buy_brokers_except_kw = []
        top_4_sell_brokers_except_kw = []
        for _, row in broker_tx.iterrows():
            if np.isnan(row["kw_sell_rank"]):
                sell_brokers = [row[f"매도거래원수량{i+1}"] for i in range(4)]
            else:
                sell_brokers = [row[f"매도거래원수량{i+1}"] for i in range(5) if i != row["kw_sell_rank"]-1]
            
            top_4_sell_brokers_except_kw.append(sell_brokers)
            
            if np.isnan(row["kw_sell_rank"]):
                buy_brokers = [row[f"매수거래원수량{i+1}"] for i in range(4)]
            else:
                buy_brokers = [row[f"매수거래원수량{i+1}"] for i in range(5) if i != row["kw_sell_rank"]-1]
            
            top_4_buy_brokers_except_kw.append(buy_brokers)
        
        top_4_buy_brokers_except_kw_df = pd.DataFrame(top_4_buy_brokers_except_kw, columns=["매수거래원수량1", "매수거래원수량2", "매수거래원수량3", "매수거래원수량4"])
        top_4_sell_brokers_except_kw_df = pd.DataFrame(top_4_sell_brokers_except_kw, columns=["매도거래원수량1", "매도거래원수량2", "매도거래원수량3", "매도거래원수량4"])

        processed_df: pd.DataFrame = broker_tx[["날짜", "시간", "종목코드", "kw_sell_rank", "kw_buy_rank"]].copy()
        processed_df["hhf_sell"] = broker_tx[["매도거래원수량1", "매도거래원수량2", "매도거래원수량3", "매도거래원수량4", "매도거래원수량5"]].apply(calc_hhf, axis=1)
        processed_df["hhf_buy"] = broker_tx[["매수거래원수량1", "매수거래원수량2", "매수거래원수량3", "매수거래원수량4", "매수거래원수량5"]].apply(calc_hhf, axis=1)
        processed_df["hhf_sell_without_kw"] = top_4_sell_brokers_except_kw_df.apply(calc_hhf, axis=1)
        processed_df["hhf_buy_without_kw"] = top_4_buy_brokers_except_kw_df.apply(calc_hhf, axis=1)

        # time it
        print("process_broker_tx completed in %s seconds ---" % (time.time() - start))
        return processed_df

if __name__ == "__main__":
    # time it
    start_time = time.time()
    bt = BrokerTrend(date="2023-03-03", code=[
        #
    ])
    bt.find_signals()
    print("signal for %s detected in %s seconds ---" % (bt.date, time.time() - start_time))
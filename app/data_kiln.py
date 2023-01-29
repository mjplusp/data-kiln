import os, sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from utils.time_utils import execute_with_timer
from query import (
    CREATE_SECOND_SETTLEMENT_TABLE, CREATE_SECOND_BIDASK_TABLE, CREATE_STOM_TABLE, EXTRACT_DETAILED_TICK, CREATE_DETAILED_TICK_TABLE,
    CALC_SECOND_SETTLEMENT_WITH_RECEIVE_NO, CALC_SECOND_BIDASK, 
    CONVERT_TO_STOM, GENERATE_INDEX
)

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)

from storage.sqlite_connector import Database
from utils.db_utils import insert_df, insert_list

class DataKiln:
    def __init__(self, target_db_dir: str, processed_db_dir: str, target_date: str):
        self.target_date: str = target_date
        self.target_db_dir: str = target_db_dir
        self.target_db: Database = Database(target_db_dir + f"/{target_date}.sqlite3")
        self.processed_db: Database = Database(processed_db_dir + f"/{target_date}_processed.sqlite3")
        self.generated_stom_db: Database = Database(processed_db_dir + f"/{target_date}_stom.db")
        self.prepare_processed_db()
    
    def prepare_processed_db(self):
        self.target_db.cursor.execute(CREATE_SECOND_SETTLEMENT_TABLE)
        self.target_db.cursor.execute(CREATE_SECOND_BIDASK_TABLE)
        self.target_db.cursor.execute(CREATE_DETAILED_TICK_TABLE)
        
    @execute_with_timer
    def process_second_settlement_data(self) -> None:
        second_settlement =  pd.read_sql_query(CALC_SECOND_SETTLEMENT_WITH_RECEIVE_NO, self.target_db.conn)
        self.target_db.cursor.execute("DELETE FROM second_settlement")
        insert_df(second_settlement, self.target_db, "second_settlement")
    
    @execute_with_timer
    def process_second_bidask_data(self) -> None:
        target_cursor = self.target_db.cursor
        query = target_cursor.execute(CALC_SECOND_BIDASK)
        columns = [column[0] for column in query.description]
        result = query.fetchall()
        self.target_db.cursor.execute("DELETE FROM second_bidask")
        insert_list(result, columns, self.target_db, "second_bidask")     

    @execute_with_timer
    def generate_singlestock_stom_backtest_table(self, code: str) -> None:
        self.generated_stom_db.cursor.execute(CREATE_STOM_TABLE % code)
        converted_to_stom_df =  pd.read_sql_query(CONVERT_TO_STOM % (code, code, code, code), self.processed_db.conn)
        converted_to_stom_df[["초당매수수량", "초당매도수량", "초당체결틱수", "초당호가틱수", "초당거래대금"]] = converted_to_stom_df[["초당매수수량", "초당매도수량", "초당체결틱수", "초당호가틱수", "초당거래대금"]].fillna(value = 0)
        converted_to_stom_df = converted_to_stom_df.ffill()

        value_list = converted_to_stom_df.values.tolist()
        columns = list(converted_to_stom_df.columns)

        self.generated_stom_db.cursor.execute("DELETE FROM '" + code + "'")
        insert_list(value_list, columns, self.generated_stom_db, code)
        self.generated_stom_db.cursor.execute(GENERATE_INDEX % (code, code))
    
    @execute_with_timer
    def generate_stom_backtest_db(self) -> None:
        target_stock = ["347770"]
        for code in target_stock:
            print(f"Current Stock: {code}")
            self.generate_singlestock_stom_backtest_table(code)
    
    # Detailed Bidask + settlement data
    @execute_with_timer
    def generate_detailed_singlestock_aggregated_table(self, code: str) -> None:
        print(datetime.now())
        # single_stock_df =  pd.read_sql_query(EXTRACT_DETAILED_TICK.format(code,code), self.target_db.conn)
        target_cursor = self.target_db.cursor
        query = target_cursor.execute(EXTRACT_DETAILED_TICK.format(code,code))
        columns = [column[0] for column in query.description]
        single_stock_df= pd.DataFrame.from_records(data = query.fetchall(), columns = columns)
        print("df 생성 완료")
        print(datetime.now())

        single_stock_df['종목코드'] = code
        
        # Process Settlement Data Null values
        cols = ['현재가', '시가', '고가', '저가', '등락율', '누적거래대금', '체결강도', '거래대금증감', '전일거래량대비', '거래회전율', '전일동시간거래량비율', '시가총액']
        single_stock_df.loc[:,cols] = single_stock_df.loc[:,cols].ffill()
        single_stock_df[['매수거래량']] = single_stock_df[['매수거래량']].fillna(value=0)
        single_stock_df[['매도거래량']] = single_stock_df[['매도거래량']].fillna(value=0)

        # Process Bid / Ask Data Null values        
        bidask_cols = ['매도호가총잔량', '매수호가총잔량', '매도호가총잔량직전대비', '매수호가총잔량직전대비', '순매수잔량', '매수비율', '순매도잔량', '매도비율',
            '매도호가5', '매도호가4', '매도호가3', '매도호가2', '매도호가1', '매수호가1', '매수호가2', '매수호가3', '매수호가4', '매수호가5',
            '매도수량5', '매도수량4', '매도수량3', '매도수량2', '매도수량1', '매수수량1', '매수수량2', '매수수량3', '매수수량4', '매수수량5']

        single_stock_df["process_needed"] = single_stock_df["매도호가총잔량"].apply(lambda x: np.isnan(x))
        single_stock_df.loc[:,bidask_cols] = single_stock_df.loc[:,bidask_cols].ffill()
        def get_col_name(row):    
            b = (single_stock_df[['매도호가5', '매도호가4', '매도호가3', '매도호가2', '매도호가1', '매수호가1', '매수호가2', '매수호가3', '매수호가4', '매수호가5']].loc[row.name] == row['현재가'])
            return b.index[b.argmax()]
        single_stock_df["tx_position"] = single_stock_df.apply(get_col_name, axis=1)
        # Add Bidask difference columns
        c = ['매도수량변화5', '매도수량변화4', '매도수량변화3', '매도수량변화2', '매도수량변화1', '매수수량변화1', '매수수량변화2', '매수수량변화3', '매수수량변화4', '매수수량변화5']
        single_stock_df = single_stock_df.assign(**dict.fromkeys(c, 0))

        print("결측치 forward fill 완료")
        print(datetime.now())

        single_stock_df = single_stock_df.reset_index()

        temp_buy_qty, temp_sell_qty = 0, 0
        for index, row in single_stock_df.iterrows():
            last_row: pd.Series = single_stock_df.iloc[index-1]

            if row['process_needed']: # 호가데이터 없고, 체결데이터만 있는 경우
                temp_sell_qty += row['매도거래량']
                temp_buy_qty += row['매수거래량']

                for col in bidask_cols:
                    row[col] = last_row[col]

                row['매도호가총잔량'] = last_row['매도호가총잔량'] - row['매수거래량']
                row['매수호가총잔량'] = last_row['매수호가총잔량'] - row['매도거래량']

                row['순매수잔량'] = last_row['매수호가총잔량'] - last_row['매도호가총잔량'] + row['매수거래량'] - row['매도거래량']
                row['순매도잔량'] = last_row['매도호가총잔량'] - last_row['매수호가총잔량'] - row['매수거래량'] + row['매도거래량']
                row['매수비율'] = round(row['매수호가총잔량']*100 / row['매도호가총잔량'],2)
                row['매도비율'] = round(row['매도호가총잔량']*100 / row['매수호가총잔량'],2)

                row['매도호가총잔량직전대비'] = -row['매수거래량']
                row['매수호가총잔량직전대비'] = -row['매도거래량']

                tx_happening_column_name: str = row['tx_position'].replace("호가", "수량")

                if tx_happening_column_name.startswith('매도'):
                    row[tx_happening_column_name] = last_row[tx_happening_column_name] - row['매수거래량']
                elif tx_happening_column_name.startswith('매수'):
                    row[tx_happening_column_name] = last_row[tx_happening_column_name] - row['매도거래량']
                
                # print(row)
            
            else:
                row['매도호가총잔량직전대비'] += temp_buy_qty
                row['매수호가총잔량직전대비'] += temp_sell_qty
                temp_buy_qty, temp_sell_qty = 0, 0

                diff_cols = ['매도호가5', '매도호가4', '매도호가3', '매도호가2', '매도호가1', '매수호가1', '매수호가2', '매수호가3', '매수호가4', '매수호가5']
                for col in diff_cols:
                    column_price = row[col]
                    current_qty = row[col.replace("호가", "수량")]
                    last_qty = None

                    for last_col in diff_cols:
                        if last_row[last_col] == column_price:
                            last_qty = last_row[last_col.replace("호가", "수량")]
                            break
                    
                    if last_qty:
                        row[col.replace("호가", "수량변화")] = current_qty - last_qty

            single_stock_df.iloc[index] = row
        
        print("호가변화량 채우기 완료")
        print(datetime.now())
                
        single_stock_df = single_stock_df.drop(columns=['process_needed', 'tx_position', 'index'])
        insert_list(single_stock_df.values.tolist(), single_stock_df.columns, self.target_db, "detailed_tick")    
        # insert_df(single_stock_df, self.target_db, "detailed_tick")

if __name__ == "__main__":

    TEST_PROCESSED_DB_DIR = root_folder + "/test"
    TEST_TARGET_DB_DIR = root_folder + "/test"
    print("Data Kiln Started")

    kiln = DataKiln(TEST_TARGET_DB_DIR, TEST_PROCESSED_DB_DIR, '20230127')

    # print("Processing Settlement Data")
    # kiln.process_second_settlement_data()
    # print("Processing Bidask Data")
    # kiln.process_second_bidask_data()

    # print("Converting to STOM format")
    # kiln.generate_stom_backtest_db()

    kiln.generate_detailed_singlestock_aggregated_table("347770")


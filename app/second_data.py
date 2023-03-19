import os, sys
from pathlib import Path
from collections import defaultdict
import pandas as pd
import numpy as np
from datetime import datetime
from utils.time_utils import execute_with_timer
from query import (
    CREATE_SECOND_SETTLEMENT_TABLE, CREATE_SECOND_BIDASK_TABLE, CREATE_DETAILED_TICK_TABLE, CREATE_DELTA_BIDASK_TABLE,
    CALC_SECOND_SETTLEMENT_WITH_RECEIVE_NO, CALC_SECOND_BIDASK, 
    MERGE_SECOND_DATA, CREATE_SECOND_MERGED_TABLE
)

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)

from storage.sqlite_connector import Database
from utils.db_utils import insert_df, insert_list

class DataKiln:
    def __init__(self, target_db_dir: str, target_date: str):
        self.target_date: str = target_date
        self.target_db_dir: str = target_db_dir
        self.target_db: Database = Database(target_db_dir + f"/{target_date}.sqlite3")
        self.prepare_processed_db()
    
    def prepare_processed_db(self):
        self.target_db.cursor.execute(CREATE_SECOND_SETTLEMENT_TABLE)
        self.target_db.cursor.execute(CREATE_SECOND_BIDASK_TABLE)
        self.target_db.cursor.execute(CREATE_DETAILED_TICK_TABLE)
        self.target_db.cursor.execute(CREATE_DELTA_BIDASK_TABLE)
        self.target_db.cursor.execute(CREATE_SECOND_MERGED_TABLE)
        
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
        converted_to_stom_df =  pd.read_sql_query(MERGE_SECOND_DATA % (code, code, code, code), self.target_db.conn)
        converted_to_stom_df[["초당매수수량", "초당매도수량", "초당체결틱수", "초당호가틱수", "초당거래대금"]] = converted_to_stom_df[["초당매수수량", "초당매도수량", "초당체결틱수", "초당호가틱수", "초당거래대금"]].fillna(value = 0)
        converted_to_stom_df = converted_to_stom_df.ffill()

        value_list = converted_to_stom_df.values.tolist()
        columns = list(converted_to_stom_df.columns)

        self.target_db.cursor.execute("DELETE FROM second_total where 종목코드 = '" + code + "'")
        # insert_list(value_list, columns, self.target_db, "second_total")
    
    def generate_stom_backtest_db(self, target_stock_list) -> None:
        for code in target_stock_list:
            print(f"Current Stock: {code}")
            self.generate_singlestock_stom_backtest_table(code)
    
    
if __name__ == "__main__":
    TEST_TARGET_DB_DIR = root_folder + "/test"
    print("Data Kiln Started")

    kiln = DataKiln(TEST_TARGET_DB_DIR, '20230314')

    # print("Processing Settlement Data")
    # kiln.process_second_settlement_data()
    # print("Processing Bidask Data")
    # kiln.process_second_bidask_data()

    print("Converting to STOM format")
    kiln.generate_stom_backtest_db(target_stock_list=["347770"])


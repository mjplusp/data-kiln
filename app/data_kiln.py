import os, sys
from pathlib import Path
import pandas as pd
from utils.time_utils import execute_with_timer
from query import (
    CREATE_SECOND_SETTLEMENT_TABLE, CREATE_SECOND_BIDASK_TABLE, CREATE_STOM_TABLE,
    CALC_SECOND_SETTLEMENT,CALC_SECOND_BIDASK,
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
        self.processed_db: Database = Database(processed_db_dir + f"/{target_date}_processed.sqlite3")
        self.generated_stom_db: Database = Database(processed_db_dir + f"/{target_date}_stom.db")
        self.prepare_processed_db()
    
    def prepare_processed_db(self):
        self.processed_db.cursor.execute(CREATE_SECOND_SETTLEMENT_TABLE)
        self.processed_db.cursor.execute(CREATE_SECOND_BIDASK_TABLE)
    
    @execute_with_timer
    def process_second_settlement_data(self) -> None:
        target_db = Database(self.target_db_dir + f"/{self.target_date}.sqlite3")
        second_settlement =  pd.read_sql_query(CALC_SECOND_SETTLEMENT, target_db.conn)

        self.processed_db.cursor.execute("DELETE FROM second_settlement")
        insert_df(second_settlement, self.processed_db, "second_settlement")
    
    @execute_with_timer
    def process_second_bidask_data(self) -> None:
        target_cursor = Database(self.target_db_dir + f"/{self.target_date}.sqlite3").cursor
        query = target_cursor.execute(CALC_SECOND_BIDASK)
        columns = [column[0] for column in query.description]
        result = query.fetchall()
        self.processed_db.cursor.execute("DELETE FROM second_bidask")
        insert_list(result, columns, self.processed_db, "second_bidask")     

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
        
if __name__ == "__main__":

    TEST_PROCESSED_DB_DIR = root_folder + "/test"
    TEST_TARGET_DB_DIR = root_folder + "/test"
    print("Data Kiln Started")

    kiln = DataKiln(TEST_TARGET_DB_DIR, TEST_PROCESSED_DB_DIR, '20230104')

    # print("Processing Settlement Data")
    # kiln.process_second_settlement_data()
    # print("Processing Bidask Data")
    # kiln.process_second_bidask_data()

    print("Converting to STOM format")
    kiln.generate_stom_backtest_db()


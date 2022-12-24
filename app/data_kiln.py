import os, sys
from pathlib import Path
import pandas as pd
from utils.time_utils import execute_with_timer
from query import (
    CREATE_SECOND_SETTLEMENT_TABLE, CREATE_SECOND_BIDASK_TABLE,
    CALC_SECOND_SETTLEMENT,CALC_SECOND_BIDASK
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
        
if __name__ == "__main__":

    TEST_PROCESSED_DB_DIR = root_folder + "/test"
    TEST_TARGET_DB_DIR = root_folder + "/test"
    print("Data Kiln Started")

    kiln = DataKiln(TEST_TARGET_DB_DIR, TEST_PROCESSED_DB_DIR, '20221028')

    # print("Processing Settlement Data")
    # kiln.process_second_settlement_data()
    print("Processing Bidask Data")
    kiln.process_second_bidask_data()


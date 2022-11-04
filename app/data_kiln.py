import os, sys
from pathlib import Path
import pandas as pd
from query import CALC_SECOND_CANDLE, CREATE_SECOND_SETTLEMENT_TABLE

root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)

from storage.sqlite_connector import Database
from utils.db_utils import insert_df

TEST_PROCESSED_DB_DIR = root_folder + "/test"
TEST_TARGET_DB_DIR = root_folder + "/test"


class DataKiln:
    def __init__(self, target_db_dir: str, processed_db_dir: str, target_date: str):
        self.target_date: str = target_date
        self.target_db_dir: str = target_db_dir
        self.processed_db: Database = Database(processed_db_dir + f"/{target_date}_processed.sqlite3")
        self.prepare_processed_db()
    
    def prepare_processed_db(self):
        self.processed_db.cursor.execute(CREATE_SECOND_SETTLEMENT_TABLE)
    
    def process_second_settlement_data(self) -> None:
        target_db = Database(self.target_db_dir + f"/{self.target_date}.sqlite3")
        second_candle =  pd.read_sql_query(CALC_SECOND_CANDLE, target_db.conn)

        self.processed_db.cursor.execute("DELETE FROM second_settlement")
        insert_df(second_candle, self.processed_db, "second_settlement")
        

        

if __name__ == "__main__":
    print("Data Kiln Started")

    kiln = DataKiln(TEST_TARGET_DB_DIR, TEST_PROCESSED_DB_DIR, '20221028')
    kiln.process_second_settlement_data()
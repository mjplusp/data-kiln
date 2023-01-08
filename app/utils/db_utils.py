import os, sys
import pandas as pd
root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)
from storage.sqlite_connector import Database

def insert_df(source: pd.DataFrame, db: Database, table: str) -> None:
    db.cursor.execute("BEGIN TRANSACTION")
    for _, row in source.iterrows():       
        db.cursor.execute('INSERT INTO "'+table+'" ("'+ str('", "'.join(source.columns))+ '") VALUES '+ str(tuple(row.values))) 
    db.cursor.execute("END TRANSACTION")

def insert_list(source: list, columns: list, db: Database, table: str) -> None:
    db.cursor.execute("BEGIN TRANSACTION")
    var_string = ', '.join('?' * len(source[0]))
    query_string = 'INSERT INTO "' + table + '" ("'+ str('", "'.join(columns)) + '") VALUES (%s);' % var_string
    for row in source:       
        db.cursor.execute(query_string, row) 
    db.cursor.execute("END TRANSACTION")
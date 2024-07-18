import pymysql
import pandas as pd

class mysql_connector():
    def __init__(self, database_name:str):
        self.database_name=database_name
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='admin',
            database=database_name
        )

    def close(self):
        self.conn.close()

    def execute_query(self,query:str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()

    def get_df (self, query:str) -> pd.DataFrame:
        df=pd.read_sql(query,self.conn)
        return df
    
    def change_table(self, old_table:str, new_table:str):
        rename=f"RENAME TABLE {old_table} TO {new_table} "
        cursor = self.conn.cursor()
        cursor.execute(rename)
        print(f"Table '{old_table}' renombrada a '{new_table}' correctamente.")
        self.conn.commit()
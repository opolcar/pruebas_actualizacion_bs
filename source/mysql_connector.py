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
    # Aquí iría el get_df
    def close(self):
        self.conn.close()

    def execute_query(self,query:str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()

# añadir al connector una funcion que se llamará get_df que va a recibir una query (str) y va a devolver un df

# 1 paso: incluir función a objeto
# 2 definir variables de entrada y salida
# 3 desarrollar la función 

    def get_df (self, query:str) -> pd.DataFrame:
        df=pd.read_sql(query,self.conn)
        return df

            
        
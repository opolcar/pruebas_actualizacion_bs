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

# añadir al connector una funcion que se llamará get_df que va a recibir una query (str) y va a devolver un df

# 1 paso: incluir función a objeto
 
    # Creamos la función q va a recibir una query (str) y va a devolver un df
    def get_df (self, query:str) -> pd.DataFrame: # Definimos variables de Entrada y Salida
        df=pd.read_sql(query,self.conn)
        return df
    
    # Ahora tendremos q añadir esta función al código donde la vamos a usar 
    # (dentro del archivo update_bbdd.py)

    def get_df(self, query:str)-> pd.DataFrame:
        df=pd.read_sql(query,self.conn)  
        return df
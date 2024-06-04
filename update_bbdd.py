import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from source.mysql_connector import mysql_connector

def get_df(conn:mysql_connector)->pd.DataFrame:
    query = "SELECT * FROM productos_jamones"
    df = conn.get_df(query=query)
    return df

def update_df(df:pd.DataFrame)->pd.DataFrame:
    headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'} 
    update_date= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for index, row in df.iterrows():
        url=row['url']
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            nombre_producto = soup.find('h2', class_= 'edgtf-single-product-title')
            nombre_producto = nombre_producto.text.strip()
            precio = float(soup.find('span', class_='woocommerce-Price-amount amount').text.replace('€','').replace(',','.'))
            df.loc[index,'precio']=precio
            df.loc[index,'nombre_producto']=nombre_producto
            df.loc[index,'update_date']=update_date
    return df

def update_bbdd(conn:mysql_connector,df:pd.DataFrame):
    try:
        for index, row in df.iterrows():
            query=f"UPDATE productos_jamones SET nombre_producto = '{row['nombre_producto']}', precio = {row['precio']}, update_date= '{row['update_date']}' WHERE url = '{row['url']}'"
            conn.execute_query(query=query)
        print("Datos actualizados en la tabla MySQL exitosamente.")
    except Exception as e:
        print(f"Error al actualizar la tabla MySQL: {e}")


#### INICIO SOFTWARE #####

conexion=mysql_connector(database_name='clientes_furgonetas')
df_to_update = get_df(conn=conexion)
df_updated = update_df(df=df_to_update)
update_bbdd(conn=conexion, df=df_updated)
conexion.close()

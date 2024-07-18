import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from source.mysql_connector import mysql_connector
import matplotlib.pyplot as plt

def get_df(conn:mysql_connector)->pd.DataFrame:
    query = "SELECT * FROM productos"
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
            query=f"UPDATE productos SET nombre_producto = '{row['nombre_producto']}', precio = {row['precio']}, update_date= '{row['update_date']}' WHERE url = '{row['url']}'"
            conn.execute_query(query=query)
        print("Datos actualizados en la tabla MySQL exitosamente.")
    except Exception as e:
        print(f"Error al actualizar la tabla MySQL: {e}")

def insert_url_new_product(conn:mysql_connector):
    r=input("¿Deseas añadir otro producto para extraer datos? Pulsa 'S' para sí o 'N' para no -> ")
    if r.upper() == 'S':
        url=input("Pega aquí la nueva url -> ")
        query_check=f"SELECT COUNT(*) as check_urls FROM productos WHERE url = '{url}'"
        df=conn.get_df(query_check)
        if df.iloc[0]['check_urls']>0:
            print('La url ya existe')
        else:
            query_insert = f"INSERT INTO productos (url) VALUES ('{url}')"
            conn.execute_query(query_insert)
            print('La url se ha añadido correctamente')
            conn.close()
            
def show_graphics(conn:mysql_connector,query:str)->pd.DataFrame:
    df=conn.cursor.execute(query)
    df.set_index('dni', inplace=True)
    ax = df.plot(kind='bar', figsize=(10, 6), colormap='viridis')
    plt.title('productos pedidos por cliente')
    plt.xlabel('dni')
    plt.ylabel('total_compras')
    plt.xticks(rotation=45)
    
    for container in ax.containers:
        ax.bar_label(container)
        
    plt.tight_layout()
    plt.show()
    
    return df
    
conexion=mysql_connector(database_name='clientes_furgonetas')

df_to_update = get_df(conn=conexion)
df_updated = update_df(df=df_to_update)
update_bbdd(conn=conexion, df=df_updated)
query_total_compras=(
    '''
    SELECT clientes.nombre, clientes.dni, SUM(productos.precio) AS total_compras
    FROM clientes
    JOIN pedidos ON clientes.dni = pedidos.dni
    JOIN productos ON pedidos.id_producto = productos.id
    GROUP BY clientes.dni, clientes.nombre;
'''
)

conexion.close()

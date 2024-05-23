import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
from datetime import datetime

def get_df()->pd.DataFrame:
    try:
        # Conexión a la base de datos
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='admin',
            database='clientes_furgonetas'
        )
        # Consulta SQL para obtener datos de la tabla
        sql = "SELECT * FROM productos_jamones"

        # Leer datos de la base de datos en un DataFrame
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        print(f"Error al leer la base de datos: {e}")
    finally:
        # Cerrar la conexión
        conn.close()

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

def update_bbdd(df:pd.DataFrame):
    try:
        # Conexión a la base de datos
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='admin',
            database='clientes_furgonetas'
        )

        # Cursor para ejecutar consultas SQL
        cursor = conn.cursor()

        # Actualizar con los datos actualizados la tabla
        for index, row in df.iterrows():
            query=f"UPDATE productos_jamones SET nombre_producto = '{row['nombre_producto']}', precio = {row['precio']}, update_date= '{row['update_date']}' WHERE url = '{row['url']}'"
            cursor.execute(query)

        # Confirmar los cambios
        conn.commit()
        print("Datos actualizados en la tabla MySQL exitosamente.")
    except Exception as e:
        print(f"Error al actualizar la tabla MySQL: {e}")
        # Revertir los cambios si hay algún error
        conn.rollback()
    finally:
        # Cerrar el cursor y la conexión
        cursor.close()
        conn.close()

def main():

    df_to_update = get_df()
    df_updated = update_df(df=df_to_update)
    update_bbdd(df=df_updated)

if __name__ == "__main__":
    main()

# Nuevo comentario 17 mayo 2024
# Comentario 23 mayo a las 10:42
# Nuevo comentario 23 mayo a las 11_52
# Otro comentario 11_23
# Comentario 11_30

# Comentario 1_06
# Comentario 1_08 
# y otro más
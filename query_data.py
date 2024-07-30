import psycopg2
from dotenv import load_dotenv
import os

# Cargar las variables de entorno
load_dotenv()

# Configuración de Redshift
REDSHIFT_CONN = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def connect_to_redshift():
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_CONN['dbname'],
            user=REDSHIFT_CONN['user'],
            password=REDSHIFT_CONN['password'],
            host=REDSHIFT_CONN['host'],
            port=REDSHIFT_CONN['port']
        )
        print("Conexión exitosa a Redshift")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error al conectar a Redshift: {e}")
        return None

def query_data(conn):
    cursor = conn.cursor()
    query = """
        SELECT * FROM weather_data
        ORDER BY timestamp DESC;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)
    
    cursor.close()

if __name__ == "__main__":
    conn = connect_to_redshift()
    if conn:
        query_data(conn)
        conn.close()

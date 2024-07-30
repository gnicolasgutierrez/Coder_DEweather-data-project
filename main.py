import requests
import json
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

API_KEY = os.getenv('API_KEY')
CITIES = ['Tunuyán', 'Mendoza', 'Buenos Aires', 'Córdoba', 'Rosario', 'La Plata', 'Mar del Plata', 'San Miguel de Tucumán', 'Salta', 'Santa Fe']
API_URL_TEMPLATE = 'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

# Configuración de Redshift
REDSHIFT_CONN = {
    'dbname': os.getenv('DBNAME'),
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('HOST'),
    'port': os.getenv('PORT')
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

def create_table(conn):
    cursor = conn.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50),
            temperature FLOAT,
            humidity INT,
            pressure INT,
            weather VARCHAR(100),
            timestamp TIMESTAMP
        )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

def insert_data(conn, data):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO weather_data (city, temperature, humidity, pressure, weather, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (data['city'], data['temperature'], data['humidity'], data['pressure'], data['weather'], data['timestamp']))
    conn.commit()
    cursor.close()

# Extracción de datos
def fetch_weather_data():
    weather_data_list = []
    for city in CITIES:
        api_url = API_URL_TEMPLATE.format(city=city, api_key=API_KEY)
        response = requests.get(api_url)
        weather_data = response.json()

        if 'main' in weather_data:
            data = {
                'city': city,
                'temperature': weather_data['main']['temp'] - 273.15,  # Convertir de Kelvin a Celsius
                'humidity': weather_data['main']['humidity'],
                'pressure': weather_data['main']['pressure'],
                'weather': weather_data['weather'][0]['description'],
                'timestamp': datetime.now()
            }
            weather_data_list.append(data)
        else:
            print(f"Error en la respuesta de la API para {city}: {weather_data}")
    return weather_data_list

if __name__ == "__main__":
    conn = connect_to_redshift()
    if conn:
        create_table(conn)
        weather_data_list = fetch_weather_data()
        for data in weather_data_list:
            insert_data(conn, data)
        conn.close()

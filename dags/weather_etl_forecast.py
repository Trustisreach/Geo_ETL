
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import sys

sys.path.append('..')
from weather_module import ConnectionDB, WeatherAPI

client = ConnectionDB()  # Экземпляр класса ConnectionDB
weather_api = WeatherAPI()  # Экземпляр класса WeatherAPI

# Функция get_local()
def get_local( **kwargs):
    return client.get_local()


# Функция для извлечения прогноза погоды
def forecast_w_extract(**kwargs):
    # Получаем местоположения из предыдущей задачи
    locations = kwargs['ti'].xcom_pull(task_ids='get_local')
    forecast_data = []
    
    for location in locations:
        try:
            #полуаем ответ от апи через get_forecast
            result = weather_api.get_forecast(location['latitude'], location['longitude'])
            result['city'] = location['city']
            forecast_data.append(result)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
    
    # Сохраняем данные для следующей задачи
    kwargs['ti'].xcom_push(key='forecast_data', value=forecast_data)

# Функция для преобразования данных прогноза погоды
def forecast_w_transform(**kwargs):
    forecast_data = kwargs['ti'].xcom_pull(key='forecast_data', task_ids='forecast_w_extract')
    transformed_data = []

    for forecast in forecast_data:
        city = forecast['city']
        time = [datetime.fromisoformat(i) for i in forecast['hourly']['time']]
        temperature = forecast['hourly']['temperature_2m']
        
        for t, temp in zip(time, temperature):
            transformed_data.append({
                'city': city,
                'timestamp': t,
                'temperature': temp,
                'upd_time': datetime.now()
            })
     
    # Сохраняем преобразованные данные для следующей задачи
    kwargs['ti'].xcom_push(key='transformed_forecast_data', value=transformed_data)

# Функция для разбиения данных на чанки
def chunk_data(data, chunk_size=50000):
    """Разбивает список данных на чанки по chunk_size"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Функция для загрузки данных в базу данных
def forecast_w_load(**kwargs):
    # Получаем преобразованные данные из предыдущей задачи
    transformed_forecast_data = kwargs['ti'].xcom_pull(key='transformed_forecast_data', task_ids='forecast_w_transform')
    df = pd.DataFrame(transformed_forecast_data)
    # Разбиваем данные на чанки по 50,000 строк
    chunk_size = 50000
    chunks = chunk_data(df.values.tolist(), chunk_size)
    connection = client.client() 

    try:
        for chunk in chunks:
            # Загружаем каждый чанк в ClickHouse
            connection.execute('INSERT INTO weather_forecast_data (city, timestamp, temperature, upd_time) VALUES', chunk)
            print(f"Successfully inserted a chunk of {len(chunk)} rows.")
    except Exception as e:
        print(f"Error inserting forecast data: {e}")
    finally:
        connection.disconnect()


# Создаем даг
with DAG(
    'weather_forecast_etl_2',
    default_args={'owner': 'airflow', 'retries': 1},
    description='ETL for weather forecast data',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 18),
    catchup=False,
) as dag:
    
    # Определяем задачи
    task_get_local = PythonOperator(
        task_id='get_local',
        python_callable=get_local,
    )
    
    task_forecast_extract = PythonOperator(
        task_id='forecast_w_extract',
        python_callable=forecast_w_extract,
    )
    
    task_forecast_transform = PythonOperator(
        task_id='forecast_w_transform',
        python_callable=forecast_w_transform,
    )
    
    task_forecast_load = PythonOperator(
        task_id='forecast_w_load',
        python_callable=forecast_w_load,
    )
    
    # Устанавливаем зависимости
    task_get_local >> task_forecast_extract >> task_forecast_transform >> task_forecast_load















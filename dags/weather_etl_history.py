from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime

import sys
sys.path.append('..')
from weather_module import ConnectionDB, WeatherAPI


client = ConnectionDB()  # Экземпляр класса ConnectionDB
weather_api = WeatherAPI()  # Экземпляр класса WeatherAPI

# Функция для разбиения данных на чанки
def chunk_data(data, chunk_size=50000):
    """Разбивает список данных на чанки по chunk_size"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


# Функция get_local()
def get_local( **kwargs):
    return client.get_local()


def history_w_e(**kwargs):
    locations = get_local()
    history_data = []
    try:
        for location in locations:
            #получаем ответ от апи через get_history
            result = weather_api.get_history(location['latitude'],location['longitude'])
            result['city'] = location['city']
            history_data.append(result)
    except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")

    kwargs['ti'].xcom_push(key='history_data', value=history_data)


def history_w_t(**kwargs):
    history_data = kwargs['ti'].xcom_pull(key='history_data', task_ids='history_w_e')
    data_list = []
    
    for data in history_data:
        city = data['city']
        daily_data = data['daily']    
        time = [datetime.fromisoformat(i) for i in daily_data['time']]
        temperature_max = daily_data['temperature_2m_max']
        temperature_min = daily_data['temperature_2m_min']
        sunrise = [datetime.fromisoformat(i) for i in daily_data['sunrise']]
        sunset = [datetime.fromisoformat(i) for i in daily_data['sunset']]
        daylight_duration = daily_data['daylight_duration']
        sunshine_duration = daily_data['sunshine_duration']
        rain_sum = daily_data['rain_sum']
        snowfall_sum = daily_data['snowfall_sum']
        wind_speed_max = daily_data['wind_speed_10m_max']

        # собираем данные через пакет по городам
        for record in zip(
            time,
            temperature_max,
            temperature_min,
            sunrise,
            sunset,
            daylight_duration,
            sunshine_duration,
            rain_sum,
            snowfall_sum,
            wind_speed_max
        ):
            data_list.append((city, *(
                value if value is not None else 0 for value in record
            )))
    
    # преобразуем в листы 
    data_list = [list(record) for record in data_list]
    
# Сохраняем данные для следующей задачи
    kwargs['ti'].xcom_push(key='history_transformed_data', value=data_list)


def history_w_l(connection=client.client(), **kwargs):
    transformed_history_data = kwargs['ti'].xcom_pull(key='history_transformed_data', task_ids='history_w_t')
    df = pd.DataFrame(transformed_history_data)
    df['upd_time'] = datetime.fromisoformat(str(datetime.now()))

    # Разбиваем данные на чанки по 50,000 строк
    chunk_size = 50000
    chunks = chunk_data(df.values.tolist(), chunk_size)
    df.index.name = 'id'
    df = df.fillna(0).reset_index()
    client = connection

    try:
        for chunk in chunks:
            # Загружаем каждый чанк в ClickHouse
            client.execute(f'INSERT INTO weather_history_data  VALUES', chunk)
            print(f"Successfully inserted a chunk of {len(chunk)} rows.")
    except Exception as e:
        print(f"Error inserting forecast data: {e}")
    finally:
        client.disconnect()    

    
# Создаем даг
with DAG(
    'weather_history_etl',
    default_args={'owner': 'airflow', 'retries': 1},
    description='ETL for weather history data',
    schedule_interval='@daily',  # Можете настроить расписание по вашему усмотрению
    start_date=datetime(2024, 11, 18),
    catchup=False,
) as dag:
    
    # Определяем задачи
    task_get_local = PythonOperator(
        task_id='get_local',
        python_callable=get_local,
    )
    
    task_history_extract = PythonOperator(
        task_id='history_w_e',
        python_callable=history_w_e,
    )
    
    task_history_transform = PythonOperator(
        task_id='history_w_t',
        python_callable=history_w_t,
    )
    
    task_forecast_load = PythonOperator(
        task_id='history_w_l',
        python_callable=history_w_l,
    )
    
    # Устанавливаем зависимости
    task_get_local >> task_history_extract >> task_history_transform >> task_forecast_load
  







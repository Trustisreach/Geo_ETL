import requests
import pandas as pd
import json
from datetime import datetime
import clickhouse_connect

currentdate = datetime.now()
# подключение к бд
def get_location():
    connection = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    database='default'
    )
    try:
        result = connection.query('SELECT * FROM location')
      
        rows = result.result_rows
        #забираю инфк пол локам
        location = [{"city": city, "latitude": lat, "longitude": lon} for city, lat, lon in rows]
        return  location
    except Exception as e:
        print(f"Failed to connect: {e}")
    finally:
        connection.close()   

# получаем данные о текущей погоде
def get_api_current (latitude , longitude):
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
    'latitude':latitude,
    'longitude': longitude,
    "current_weather": "true"
    }
    response  = requests.get(url, params = params)

    if response.status_code == 200:
       return response.json()
    else:
        return('Error', response.status_code)

# получаем данные о прогнозе
def get_api_forecast(latitude , longitude):
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
    'latitude':latitude,
    'longitude': longitude,
    "hourly":'temperature_2m'
    }
    response  = requests.get(url, params = params)

    if response.status_code == 200:
       return response.json()
    else:
        return('Error', response.status_code)

#преобразуем полученные данные по текуще погоде и кладем в бд
def main():
    client = clickhouse_connect.get_client(host='localhost', port=8123, database='default')

    locations = get_location()
    

     #  пробегаемся по городам из бд
    for location in locations:
        weather_data = get_api_current(location['latitude'], location['longitude'])
        if weather_data and 'current_weather' in weather_data:
            current_weather = weather_data['current_weather']
            #забираем нужны столбцы для подгруза
            data_tuple = (
            location['city'],
            datetime.fromisoformat(current_weather['time']),  # Parse time from ISO format
            current_weather['temperature'],
            current_weather['windspeed'],
            current_weather['winddirection'],
            current_weather['is_day']

        )
            print(data_tuple)
            
            client.insert('weather_data', [data_tuple])
            print(f"Data for {location['city']} inserted successfully.")
        else:
            print(f"No current weather data available for {location['city']}.")

    client.close()

    #преобразуем полученные данные по текуще погоде и кладем в бд
def forecast():
    client = clickhouse_connect.get_client(host='localhost', port=8123, database='default')

    data_list = []
    #  пробегаемся по городам из бд
    for location in get_location():
        weather_forecast_data = get_api_forecast(location['latitude'], location['longitude'])
        
        # Assign variables
        city = location['city']
        time = [datetime.fromisoformat(i) for i in weather_forecast_data['hourly']['time']]
        temperature = weather_forecast_data['hourly']['temperature_2m']
        

        # объединяем города, температуру и время
        for timestamp, temp in zip(time, temperature):
            data_list.append((city, timestamp, temp)) 

    # загружаем данные в бд
        try:
            counter = 0
            for entry in data_list:
                client.insert('weather_forecast_data', [entry])
                counter +=1
                print(f"Data for {location['city']} inserted successfully. - {counter}row")
            else:
                print(f"No current weather data available for {location['city']}.")
        except Exception as e:
            print(f"uploading error{e}")
        client.close()

try:
    main() 
    timeout = 60
except Exception as e:
    print(f'main func error{e}')
finally:
    print(f'Current weather updated{currentdate}')
                    

try:
    forecast()
except Exception as e:
    print(f'forecast func error{e}')
finally:
    print(f'forecast weather updated{currentdate}')
                         
          
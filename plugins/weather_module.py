
import requests
from datetime import datetime, timedelta
from clickhouse_driver import Client  # Заменил clickhouse_connect на clickhouse_driver

class ConnectionDB:
    
    def __init__(self, host='clickhouse', port=9000, database='default', user = 'admin', password = 'admin'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password =password
        self.connection = None

    def client(self):
        try:
            # Используем Client вместо clickhouse_connect.get_client
            self.connection = Client(host=self.host, port=self.port, database=self.database,
                                      user = self.user, password = self.password)
            print('Connection successful')
            return self.connection
        except Exception as e:
            print(f'No db connection: {e}')

    def get_local(self, value='SELECT * FROM location', **kwargs):
        try:
            if self.connection:
                # Выполнение запроса с использованием execute
                result = self.connection.execute(value)
                location = [{"city": city, "latitude": lat, "longitude": lon} for city, lat, lon in result]
                return location
        except Exception as e:
            print(f"Failed to fetch data: {e}")
        finally:
            self.connection.disconnect()  # Закрытие соединения вручную


class WeatherAPI:

    def __init__(self):
        self.base_url_current = 'https://api.open-meteo.com/v1/forecast'
        self.base_url_history = 'https://archive-api.open-meteo.com/v1/archive'

    def _get(self, url, params):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return ('Error', response.status_code)

    def get_current_weather(self, latitude, longitude):
        params = {
            'latitude': latitude,
            'longitude': longitude,
            "current_weather": "true"
        }
        return self._get(self.base_url_current, params)
    
    def get_history(self, latitude, longitude):
        last_day = datetime.now().date() - timedelta(days=1)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": "2024-01-01",
            "end_date": last_day,
            "daily": [
                "weather_code", "temperature_2m_max", 
                "temperature_2m_min", "temperature_2m_mean", 
                "sunrise", "sunset", "daylight_duration", 
                "sunshine_duration", "rain_sum", 
                "snowfall_sum", "wind_speed_10m_max"
            ]
        }
        return self._get(self.base_url_history, params)

    def get_forecast(self, latitude, longitude):
        params = {
            'latitude': latitude,
            'longitude': longitude,
            "hourly": 'temperature_2m'
        }
        return self._get(self.base_url_current, params)



location_table =
'''CREATE TABLE IF NOT EXISTS location (
    location String,
    latitude Float32,
    longitude Float32
) ENGINE = MergeTree()
ORDER BY (location)'''

weather_forecast_table =
'''CREATE TABLE IF NOT EXISTS weather_forecast_data (
    city String,
    timestamp DateTime,
    temperature Float32,
    upd_time DateTime
) ENGINE = MergeTree()
ORDER BY (city, timestamp)
'''

weather_history_table =
'''REATE TABLE IF NOT EXISTS weather_history_data (
	city String,
    timestamp DateTime,
    temperature_max Float32,
    temperature_min Float32,
    sunrise_time DateTime,
    sunset_time DateTime,
    daylight_duration_h Float32,
    sunshine_duration_h Float32,
    rain_sum Float32,
    snowfall_sum Float32,
    wind_speed_10m_max Float32,
    upd_time DateTime
) ENGINE = ReplacingMergeTree(upd_time)
ORDER BY (city, timestamp)
'''

weather_current_table= 
'''
CREATE TABLE IF NOT EXISTS weather_data (
    city String,
    timestamp DateTime,
    temperature Float32,
    windspeed Float32,
    winddirection Int32,
    is_day UInt8,
    upd_time DateTime
) ENGINE = MergeTree()
ORDER BY (city, timestamp)
'''
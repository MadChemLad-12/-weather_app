import sqlite3
import time
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

#Setup the Open_meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

DB_name = "weather.db"

def create_database():
    conn = sqlite3.connect(DB_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            timestamp TEXT,
            temperature REAL,
            humidity REAL,
            dew_point REAL,
            precipitation REAL,
            wind_speed REAL
        )
    ''')
    conn.commit()
    conn.close()

def insert_weather_data(city, latitude, longitude, timestamp, temperature, humidity, dew_point, precipitation, wind_speed):
    conn = sqlite3.connect(DB_name)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO weather_data (city, latitude, longitude, timestamp, temperature, humidity, dew_point, precipitation, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (city, latitude, longitude, timestamp, temperature, humidity, dew_point, precipitation, wind_speed))
    conn.commit()
    conn.close()

#Initialise Database    
create_database()

#Define API requests parameters
cities = ["Tokyo", "Sydney", "Brisbane", "Singapore", "Hobart"]
latitudes = [35.6854, -33.8678, -27.4679, 1.2897, -42]
longitudes = [139.7531, 151.2073, 153.0281, 103.8501, 147]

params = {
    "latitude": latitudes,
    "longitude": longitudes,
    "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    "timezone": "Australia/Sydney",
    "past_days": 1  # Reduce past days for testing
}

responses = openmeteo.weather_api("https://api.open-meteo.com/v1/forecast", params=params)


#Process responses and store data in the database
start_time = time.time()
for i, response in enumerate(responses):
    city = cities[i]
    latitude, longitude = response.Latitude(), response.Longitude()
    
    hourly = response.Hourly()
    timestamps = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s",utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left")
    
    for j, timestamp in enumerate(timestamps):
        insert_weather_data(
            city, latitude, longitude, timestamp.isoformat(),
            float(hourly.Variables(0).ValuesAsNumpy()[j]),  # Temperature
            float(hourly.Variables(1).ValuesAsNumpy()[j]),  # Humidity
            float(hourly.Variables(2).ValuesAsNumpy()[j]),  # Dew Point
            float(hourly.Variables(3).ValuesAsNumpy()[j]),  # Precipitation
            float(hourly.Variables(4).ValuesAsNumpy()[j])   # Wind Speed
    )
end_time = time.time()
elapsed = end_time - start_time
print("Weather data successfully stored in the database!")
print(f'Time take {elapsed:.6f} seconds')

quit()

conn = sqlite3.connect(DB_name)
df = pd.read_sql("SELECT * FROM weather_data", conn)
print(df.head())
conn.close()


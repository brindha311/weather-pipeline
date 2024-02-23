import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
import requests




def data_import():

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 48.7823,
        "longitude": 9.177,
        "start_date": "2024-02-21",
        "end_date": "2024-02-21",
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                  "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "sunrise",
                  "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum",
                  "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
                  "shortwave_radiation_sum", "et0_fao_evapotranspiration"]
    }

    # Make the GET request
    response = requests.get(url, params=params)

    # Extracting data from JSON response
    header = response.json()["daily_units"]
    data = response.json()["daily"]

    df = pd.DataFrame(data)
    df.columns = [key + '_' + value.replace(' ', '_').replace('°','deg_').replace('/','_per_').replace('²','sq') for key, value in header.items()]

    print(data)
    print(df)
    print(df.columns)
    print(df.info)




def data_import_1():

    # https: // open - meteo.com / en / docs / historical - weather - api  # latitude=50.1155&longitude=8.6842&hourly=&daily=weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 48.7823,
        "longitude": 9.177,
        "start_date": "2024-02-07",
        "end_date": "2024-02-21",
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                  "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "sunrise",
                  "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum",
                  "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
                  "shortwave_radiation_sum", "et0_fao_evapotranspiration"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_apparent_temperature_max = daily.Variables(4).ValuesAsNumpy()
    daily_apparent_temperature_min = daily.Variables(5).ValuesAsNumpy()
    daily_apparent_temperature_mean = daily.Variables(6).ValuesAsNumpy()
    daily_sunrise = daily.Variables(7).ValuesAsNumpy()
    daily_sunset = daily.Variables(8).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(9).ValuesAsNumpy()
    daily_sunshine_duration = daily.Variables(10).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(11).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(12).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(13).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(14).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(16).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(17).ValuesAsNumpy()
    daily_shortwave_radiation_sum = daily.Variables(18).ValuesAsNumpy()
    daily_et0_fao_evapotranspiration = daily.Variables(19).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
    daily_data["sunrise"] = daily_sunrise
    daily_data["sunset"] = daily_sunset
    daily_data["daylight_duration"] = daily_daylight_duration
    daily_data["sunshine_duration"] = daily_sunshine_duration
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["rain_sum"] = daily_rain_sum
    daily_data["snowfall_sum"] = daily_snowfall_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
    daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
    daily_data["et0_fao_evapotranspiration"] = daily_et0_fao_evapotranspiration
    daily_data["date"] = daily_data["date"].strftime('%Y-%m-%d')
    daily_dataframe = pd.DataFrame(data=daily_data)
    print(daily_dataframe.head())
    return daily_dataframe

data_import()

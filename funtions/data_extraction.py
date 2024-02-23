import pandas as pd
import requests
from geopy.geocoders import Nominatim


def data_import_weather(latitude,longitude,start_date,end_date):

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
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

    return df


def get_lat_long(city_name):

    geolocator = Nominatim(user_agent="city_locator")
    location = geolocator.geocode(city_name)
    if location:
        return location.latitude, location.longitude
    else:
        return None, None



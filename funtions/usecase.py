import pandas as pd
import logging
from funtions import db_connections

def usecase_1():
    usecase_1 = """WITH Max_Temp_Month AS (
    SELECT 
        City,
        YEAR(WeatherDate) AS YearDate,
        MONTH(WeatherDate) AS MonthDate,
        MAX(Max_Temperature_C) AS Max_Temperature
    FROM 
        WeatherData
    GROUP BY 
        City,
        YEAR(WeatherDate),
        MONTH(WeatherDate))
    SELECT 
    W.City,
    W.WeatherDate,
    W.Weather_Code,
    W.Max_Temperature_C,
    W.Min_Temperature_C,
    W.Mean_Temperature_C,
    W.Max_Apparent_Temperature_C,
    W.Min_Apparent_Temperature_C,
    W.Mean_Apparent_Temperature_C,
    W.Sunrise,
    W.Sunset,
    W.Daylight_Duration_s,
    W.Sunshine_Duration_s,
    W.Precipitation_mm,
    W.Rain_mm,
    W.Snowfall_cm,
    W.Precipitation_Hours,
    W.Max_Wind_Speed_kmh,
    W.Max_Wind_Gusts_kmh,
    W.Temperature_Range_C
    FROM 
    WeatherData W
    JOIN 
    Max_Temp_Month MP ON MP.City = W.City
                      AND MP.YearDate = YEAR(W.WeatherDate)
                      AND MP.MonthDate = MONTH(W.WeatherDate)
                      AND MP.Max_Temperature = W.Max_Temperature_C;"""
    result_df = pd.read_sql_query(usecase_1, con=db_connections.database_connection())
    logging.info(result_df.count())
    # Convert timestamp columns to string
    result_df = convert_timestamp_to_str(result_df)
    return result_df

def convert_timestamp_to_str(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].astype(str)
    return df

#print(usecase_1())
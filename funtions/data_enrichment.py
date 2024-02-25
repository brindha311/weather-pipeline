import pandas as pd
import os

# Define a function to clean and enrich the data
def enrich_data(df):

    # Calculate the duration of daylight in hours
    df['Daylight_Hours'] = (df['Sunset'] - df['Sunrise']).dt.total_seconds() / 3600

    # Enrich the data further
    # Calculate the difference between maximum and minimum temperatures
    df['Temperature_Range_C'] = df['Max_Temperature_C'] - df['Min_Temperature_C']

    # Convert time columns to datetime type
    time_columns = ['Date', 'Sunrise', 'Sunset']
    for col in time_columns:
        df[col] = df[col].astype(str)

    return df
import logging
import pandas as pd
import os
"""import time
script_dir = os.path.dirname(os.path.realpath(__file__))
output_path = os.path.join(script_dir, 'error_data')
cur_time = int(time.time())
df = pd.read_csv("weatherdata.csv")"""
# Define column data types
column_types = {
    'City': str,
    'Latitude': float,
    'Longitude': float,
    'Date': pd.Timestamp,
    'Weather_Code': int,
    'Max_Temperature_C': float,
    'Min_Temperature_C': float,
    'Mean_Temperature_C': float,
    'Max_Apparent_Temperature_C': float,
    'Min_Apparent_Temperature_C': float,
    'Mean_Apparent_Temperature_C': float,
    'Sunrise': pd.Timestamp,
    'Sunset': pd.Timestamp,
    'Daylight_Duration_s': float,
    'Sunshine_Duration_s': float,
    'Precipitation_mm': float,
    'Rain_mm': float,
    'Snowfall_cm': float,
    'Precipitation_Hours': float,
    'Max_Wind_Speed_kmh': float,
    'Max_Wind_Gusts_kmh': float,
    'Dominant_Wind_Direction_deg': int,
    'Shortwave_Radiation_MJ_per_msq': float,
    'Evapotranspiration_mm': float
}


class DataValidation:
    def __init__(self, data):
        self.data = data

    def check_missing_values(self):
        return self.data[self.data.isnull().any(axis=1)]

    def check_data_types(self, column_types):
        invalid_rows = pd.DataFrame()
        for column, expected_type in column_types.items():
            invalid_rows = pd.concat([invalid_rows, self.data[~self.data[column].apply(lambda x: isinstance(x, expected_type))]])
        return invalid_rows

    def validate_range(self, column, min_val, max_val):
        return self.data[(self.data[column] < min_val) | (self.data[column] > max_val)]

    def validate_value(self, column, valid_values):
        return self.data[~self.data[column].isin(valid_values)]

    def ensure_one_column_max(self, column1, column2):
        """
        Ensure that column1 always has a value greater than or equal to column2.
        If column1 has a smaller value than column2 in any row, swap the values.
        """
        invalid_rows = self.data[self.data[column1] < self.data[column2]]
        self.data.loc[invalid_rows.index, [column1, column2]] = self.data.loc[invalid_rows.index, [column2, column1]].values
        return invalid_rows

    def check_duplicates(self, subset=None):
        duplicates = self.data[self.data.duplicated(subset=subset, keep='first')]
        return duplicates
    def split_and_write_invalid_rows(self, invalid_data, df, output_path,filename):
        # Separate invalid rows into a different DataFrame
        output_path = os.path.join(output_path,filename)
        invalid_data.to_csv(output_path, index=False)
        return df.drop(invalid_data.index)


def check_dataframe(df,output_path,cur_time):
    # Rename columns for better readability
    df.columns = ['City', 'Latitude', 'Longitude', 'Date', 'Weather_Code', 'Max_Temperature_C', 'Min_Temperature_C',
                  'Mean_Temperature_C', 'Max_Apparent_Temperature_C', 'Min_Apparent_Temperature_C',
                  'Mean_Apparent_Temperature_C', 'Sunrise', 'Sunset', 'Daylight_Duration_s', 'Sunshine_Duration_s',
                  'Precipitation_mm', 'Rain_mm', 'Snowfall_cm', 'Precipitation_Hours', 'Max_Wind_Speed_kmh',
                  'Max_Wind_Gusts_kmh', 'Dominant_Wind_Direction_deg', 'Shortwave_Radiation_MJ_per_msq',
                  'Evapotranspiration_mm']
    # Data Cleaning
    # Convert time columns to datetime type
    time_columns = ['Date', 'Sunrise', 'Sunset']
    for col in time_columns:
        df[col] = pd.to_datetime(df[col])


    validator = DataValidation(df)

    # Check data types and split invalid rows
    invalid_datatype = validator.check_data_types(column_types)

    # Split and write invalid rows to a separate file
    if not invalid_datatype.empty:
        logging.error("invalid Data types")
        df = validator.split_and_write_invalid_rows(invalid_datatype, df, output_path, f'invalid_datatype_{cur_time}.csv')

    # Check for missing values
    missing_values = validator.check_missing_values()
    # Split and write invalid rows to a separate file
    if not missing_values.empty:
        logging.error("invalid missing values")
        # Check for null values in each column
        null_columns = df.columns[df.isnull().any()].tolist()
        if null_columns:
            logging.error("Columns with null values:")
            for col in null_columns:
                logging.error(col)
        df = validator.split_and_write_invalid_rows(missing_values, df, output_path, f'missing_values_{cur_time}.csv')

    # Check for missing values
    duplicates = validator.check_duplicates()

    # Split and write invalid rows to a separate file
    if not duplicates.empty:
        logging.error("invalid missing values")
        df = validator.split_and_write_invalid_rows(duplicates, df, output_path, f'duplicates_{cur_time}.csv')

    # Check for missing values
    Sunset_before_Sunrise = validator.ensure_one_column_max("Sunset", "Sunrise")

    # Split and write invalid rows to a separate file
    if not Sunset_before_Sunrise.empty:
        logging.error("Sunset happens after sunraise but there is some incosistance")
        df = validator.split_and_write_invalid_rows(Sunset_before_Sunrise, df, output_path,
                                                    f'Sunset_before_Sunrise_{cur_time}.csv')

    # Check for missing values
    max_temp_lt_min_temp = validator.ensure_one_column_max("Max_Temperature_C", "Min_Temperature_C")

    # Split and write invalid rows to a separate file
    if not max_temp_lt_min_temp.empty:
        logging.error(
            "minimum temperature is greater than maximum temperature but in theory it is not the  case is some incosistance")
        df = validator.split_and_write_invalid_rows(max_temp_lt_min_temp, df, output_path,
                                                    f'max_temp_lt_min_temp_{cur_time}.csv')

    return df


#check_dataframe(df,output_path,cur_time)
import json
import csv
import os
import logging

import pandas as pd

from funtions import data_extraction, file_process

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


def handler():
    try:
        # Get the current script's directory
        script_dir = os.path.dirname(os.path.realpath(__file__))

        # Create the path for raw data
        raw_data = os.path.join(script_dir, 'raw-data')

        csv_path = os.path.join(raw_data, 'csv-format')
        json_path = os.path.join(raw_data, 'json-format')

        logging.info(json_path)
        logging.info(csv_path)

        # cities
        cities = ['Düsseldorf', 'Stuttgart', 'Frankfurt',
                  'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Dortmund',
                  'Leipzig', 'Essen']

        for city in cities:
            df = pd.DataFrame()
            latitude, longitude = data_extraction.get_lat_long(city)
            df = data_extraction.data_import_weather(latitude, longitude,"2024-01-01","2024-02-21")
            df.insert(0,"city",city)
            df.insert(1,"Latitude",latitude)
            df.insert(2,"Longitude",longitude)

            final_file_path_csv = os.path.join(csv_path, "weatherdata.csv")
            final_file_path_json = os.path.join(json_path, "weatherdata.json")

            file_process.file_create_append(final_file_path_csv, df)
            file_process.file_create_append(final_file_path_json, df, False)
            print(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")
            logging.info(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")


    except Exception as e:
        logging.error(f"An unexpected error occurred in the main block: {e}")
        raise


if __name__ == "__main__":
    handler()

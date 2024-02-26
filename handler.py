import os
import logging
import pandas as pd
import time
from funtions import data_extraction, file_process, data_validation, data_enrichment, db_connections, usecase

logging.basicConfig(filename='logfile.log', level=logging.DEBUG)


def handler():
    try:
        cur_time = int(time.time())
        print(cur_time)
        # Get the current script's directory
        script_dir = os.path.dirname(os.path.realpath(__file__))

        # cities
        cities = ['Düsseldorf', 'Stuttgart', 'Frankfurt',
                  'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Dortmund',
                  'Leipzig', 'Essen']

        for city in cities:
            df = pd.DataFrame()
            latitude, longitude = data_extraction.get_lat_long(city)
            reponse = data_extraction.data_import_weather_response(latitude, longitude,"2024-01-01","2024-02-21")
            df = data_extraction.data_import_weather(reponse)
            df.insert(0,"city",city)
            df.insert(1,"Latitude",latitude)
            df.insert(2,"Longitude",longitude)

            raw_data = os.path.join(script_dir, 'raw-data')
            raw_csv_path = os.path.join(raw_data, 'csv-format')
            raw_json_path = os.path.join(raw_data, 'json-format')
            final_file_path_csv = os.path.join(raw_csv_path, f"weatherdata_{cur_time}.csv")
            final_file_path_json = os.path.join(raw_json_path, f"weatherdata_{cur_time}.json")

            file_process.file_create_append(final_file_path_csv, df)
            file_process.file_create_append(final_file_path_json, df, False)
            print(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")
            logging.info(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")
            error_data = os.path.join(script_dir, 'error_data')

            df = data_validation.check_dataframe(df, error_data, cur_time)
            df = data_enrichment.enrich_data(df)

            # Create the path for raw data
            processed_data = os.path.join(script_dir, 'processed-data')

            processed_csv_path = os.path.join(processed_data, 'csv-format')
            processed_json_path = os.path.join(processed_data, 'json-format')

            final_file_path_csv = os.path.join(processed_csv_path, f"weatherdata_{cur_time}.csv")
            final_file_path_json = os.path.join(processed_json_path, f"weatherdata_{cur_time}.json")

            file_process.file_create_append(final_file_path_csv, df)
            file_process.file_create_append(final_file_path_json, df, False)
            df.to_sql(name="WeatherData", con=db_connections.database_connection(), index=False,
                               if_exists='append')

            print(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")
            logging.info(
                f"{city}: Latitude = {latitude}, Longitude = {longitude} has data {len(df)}"
                f" loaded successfully in the path csv {final_file_path_csv} and json {final_file_path_json}")

        uc1_df = usecase.usecase_1()
        usecase1_path = os.path.join(script_dir, 'load_usecase')
        usecase1_path_csv = os.path.join(usecase1_path, f"usecase1_{cur_time}.csv")
        usecase1_path_json = os.path.join(usecase1_path, f"usecase1_{cur_time}.json")


        file_process.file_create_append(usecase1_path_csv, uc1_df)
        file_process.file_create_append(usecase1_path_json, uc1_df, False)

    except Exception as e:
        logging.error(f"An unexpected error occurred in the main block: {e}")
        raise


if __name__ == "__main__":
    handler()

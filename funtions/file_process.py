import os
import csv
import json


def file_create_append(filename, df_final, csv_df=True):
    records = df_final.to_dict(orient='records')

    append_write = 'a' if os.path.isfile(filename) else 'w'
    if csv_df:
        with open(filename, append_write, newline='', encoding='utf-8') as csv_file:
            fieldnames = records[0].keys() if records else []
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            # Write header if the file is being created
            if append_write == 'w':
                writer.writeheader()
            # Write records
            for record in records:
                writer.writerow(record)

    else:
        # Write list of dictionaries to JSON file
        with open(filename, append_write, encoding='utf-8') as json_file:
            for record in records:
                json.dump(record, json_file, ensure_ascii=False)
                json_file.write('\n')


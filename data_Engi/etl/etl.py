import glob
from datetime import datetime
import os
import pandas as pd
import xml.etree.ElementTree as ET

log_file = "car_etl_log.txt"
target_file = "transformed_car_data.csv"
'''Extract'''
def extract_csv(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
def extract_json(file_path):
    try:
        return pd.read_json(file_path,lines=True)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
def extract_xml(file_path):
    df=pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        for row in root:
            model = row.find('car_model').text
            year = int(row.find('year_of_manufacture').text)
            price = float(row.find('price').text)
            fuel = row.find('fuel').text
            df=pd.concat([df, pd.DataFrame({"car_model": model, "year_of_manufacture": year, "price": price, "fuel": fuel})], ignore_index=True)
        return df
    except ET.ParseError as e:
        print(f"Error parsing XML file {file_path}: {e}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
def extract_files():
    csv_files = glob.glob("datasource/*.csv")
    json_files = glob.glob("datasource/*.json")
    xml_files = glob.glob("datasource/*.xml")

    data_frames = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    for file in csv_files:
        if file!= target_file:
            df = extract_csv(file)
            if df is not None:
                data_frames=pd.concat([data_frames, df], ignore_index=True)
    for file in json_files:
        df = extract_json(file)
        if df is not None:
            data_frames=pd.concat([data_frames, df], ignore_index=True)
    for file in xml_files:
        df = extract_xml(file)
        if df is not None:
            data_frames=pd.concat([data_frames, df], ignore_index=True)

    return data_frames
'''Transform '''
def transform(data):
    data['price'] = data['price'].round(0).astype(int)
    return data
'''Load'''
def load_data(target_file, data):
    data.to_csv(target_file, index=False)
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as log:
        log.write(f"{timestamp}: {message}\n")
'''Main ETL Process'''
def main():
    log_progress("ETL Job Started")
    log_progress("Extract phase Started")
    extracted_data = extract_files()
    log_progress("Extract phase Ended")

    log_progress("Transform phase Started")
    transformed_data = transform(extracted_data)
    log_progress("Transform phase Ended")

    log_progress("Load phase Started")
    load_data(target_file, transformed_data)
    log_progress("Load phase Ended")

    log_progress("ETL Job Ended")

    print("ETL Process completed. Output file:", target_file)
    print(transformed_data)
if __name__ == "__main__":
    main()
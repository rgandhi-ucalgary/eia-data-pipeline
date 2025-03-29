import requests
import json
import pandas as pd
import time
from datetime import datetime
import yaml
import boto3
from botocore.exceptions import ClientError

def load_config():
    try:
        with open("../config.yaml", "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise Exception("config.yaml file not found!")
    except yaml.YAMLError as e:
        raise Exception(f"Error parsing config.yaml: {e}")

config = load_config()

API_KEY = config['eia']['api_key']

def upload_to_s3(file_path, s3_path):
    s3 = boto3.client('s3')
    
    try:
        s3.upload_file(file_path, config['aws']['s3_bucket'], s3_path)
        print(f"Uploaded {file_path} to s3://{config['aws']['s3_bucket']}/{s3_path}")
    except ClientError as e:
        print(f"S3 Upload Error: {e}")
        raise


def fetch_data(url, api_key, headers, max_rows=5000):
    offset = 0
    all_data = []
    attempts = 0
    max_attempts = 3

    while True:
        headers["X-Params"] = json.dumps({
            **json.loads(headers["X-Params"]),
            "offset": offset
        })
        
        try:
            response = requests.get(url, headers=headers, params={"api_key": api_key}, timeout=30)
            if response.status_code == 200:
                data = response.json().get("response", {}).get("data", [])
                if not data:
                    print(f"No more data after offset {offset}")
                    break
                
                all_data.extend(data)
                offset += max_rows
                print(f"Retrieved {len(data)} rows, Total: {len(all_data)} rows")
                time.sleep(2)  # API rate limit handling
            else:
                print(f"API Error {response.status_code}: {response.text}")
                break
        except requests.exceptions.Timeout:
            print(f"Request timeout at offset {offset}. Retrying in 5 seconds...")
            time.sleep(5)
            attempts += 1
            if attempts >= max_attempts:
                print("Max retry attempts reached")
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    return pd.DataFrame(all_data)

# Calculate previous 3 years
current_year = datetime.now().year
years_to_fetch = [current_year - i for i in range(0, 4)]

# API configuration

api_ops_daily = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"

base_params={
"X-Params":json.dumps({
    "frequency": "daily",
    "data": ["value"],
    "facets": {},
    "sort": [
        {
            "column": "period",
            "direction": "asc"
        }
    ],
    "offset": 0,
    "length": 5000
})
}

for year in years_to_fetch:
    if year == current_year:
        year_params = base_params.copy()
        year_params.update({
            "start": f"{year}-01-01",
            "end": "None"
        })
    else:
        year_params = base_params.copy()
        year_params.update({
            "start": f"{year}-01-01",
            "end": f"{year}-12-31"
        })
    
    headers = {"X-Params": json.dumps(year_params)}
    
    operational_daily_df = fetch_data(api_ops_daily, API_KEY, year_params)
    
    # Data type conversion
    operational_daily_df['period'] = pd.to_datetime(operational_daily_df['period'])
    operational_daily_df['value'] = operational_daily_df['value'].apply(pd.to_numeric, errors='coerce')
    
    # CSV file paths
    local_path = f'historical_daily_operations_{year}.csv'
    s3_path = f'historical/daily_operations_{year}.csv'
    
    # Save as CSV
    operational_daily_df.to_csv(
        local_path,
        index=False
    )
    
    # Upload to S3 (assuming upload_to_s3 handles CSV)
    upload_to_s3(local_path, s3_path)

print("CSV data collection and upload complete!")

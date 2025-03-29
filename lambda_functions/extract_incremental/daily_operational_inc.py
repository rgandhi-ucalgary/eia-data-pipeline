#DailyElectricityGenerationLambda

import json
import time
import requests
import pandas as pd
import boto3
import os
import smtplib
from io import BytesIO
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone

def send_email(subject, body):
    sender = 'eiausnotification@gmail.com'
    recipient = ['rgcommerce18@gmail.com']
    smtp_server = 'smtp.gmail.com'
    smtp_port = 465
    smtp_user = 'eiausnotification@gmail.com'  # Email userID
    smtp_password = 'wbcysyrpxbgnklhh'  # Email password
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(sender, recipient, msg.as_string())
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")


def fetch_data(url, api_key, headers, max_rows=5000):
    offset = 0
    all_data = []
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
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
                time.sleep(2)
            else:
                print(f"API Error {response.status_code}: {response.text}")
                break
        except requests.exceptions.Timeout:
            print(f"Request timeout at offset {offset}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    return pd.DataFrame(all_data)

def lambda_handler(event, context):
    url = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
    api_key = "JP9YNN5qaT8qvRhVi92LIDlp2RKoHglNd6fToWUe"
    s3_bucket = "eia-data-ucalgary"
    
    # Calculate previous day's date
    previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    headers = {
        "X-Params": json.dumps({
            "frequency": "daily",
            "data": ["value"],
            "facets": {},
            "start": previous_day,
            "end": previous_day,
            "sort": [{"column": "period", "direction": "asc"}],
            "offset": 0,
            "length": 5000
        })
    }
    
    df = fetch_data(url, api_key, headers)
    if df.empty:
        print("No data retrieved. Exiting.")
        return {
        'statusCode': 200,
        'body': 'No data retrieved.'}
    
    # Convert DataFrame to CSV
    csv_buffer = df.to_csv(index=False)
    
    # S3 Upload Configuration
    s3 = boto3.client('s3')
    filename = f"daily_electricity_data_{previous_day}.csv"
    
    try:
        # Upload CSV to S3
        s3.put_object(
            Bucket=s3_bucket,
            Key=f'incremental/{filename}',
            Body=csv_buffer.encode('utf-8')  # Encode string to bytes
        )
        print(f"File {filename} uploaded successfully to {s3_bucket}")
    except Exception as e:
        error_message = f"Failed to upload CSV file: {e}"
        print(error_message)
        send_email("Lambda S3 Upload Failure", error_message)  # Ensure send_email is defined
    
    return {
        'statusCode': 200,
        'body': 'Daily electricity operational data uploaded to S3'
    }
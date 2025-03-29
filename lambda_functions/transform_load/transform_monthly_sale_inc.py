import pandas as pd
import boto3
from io import BytesIO
import smtplib
import awswrangler as awr
from decimal import Decimal
from email.mime.text import MIMEText
from datetime import datetime, timezone
import json

# Initialize S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

"""
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
"""

def read_csv_from_s3(bucket, key):
    """Read CSV file from S3 with error handling"""
    try:
        # Get CSV file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Read directly into pandas DataFrame
        return pd.read_csv(response['Body'])
        
    except Exception as e:
        error_msg = f"Error reading CSV file {key} from S3: {str(e)}"
        print("S3 CSV Read Failure", error_msg)
        raise

def process_file(bucket_name, csv_file_key):
    try:
        table_name = 'SalesData' 
        # Step 1: Read CSV file from S3
        sales_df = read_csv_from_s3(bucket_name, csv_file_key)
        
        if sales_df.empty:
            error_msg = f"Empty DataFrame from {csv_file_key}"
            print("Empty Data Warning", error_msg)
            return {'statusCode': 204, 'body': 'No content'}

        # Step 2: Data processing
        sales_df = sales_df.drop(columns=[
                'customers-units', 'price-units', 
                'revenue-units', 'sales-units', 'stateid'
            ])
            
        sales_df.fillna(0, inplace=True)
        
        sales_df.rename(columns={
            'stateDescription': 'state',
            'period': 'timestamp',
            'customers': 'num_customers',
            'price': 'price_per_kwh',
            'revenue': 'total_revenue',
            'sales': 'total_sales'
        }, inplace=True)

        sales_df['state_sectorid'] = sales_df['state'] + "_" + sales_df['sectorid']
        sales_df['revenue_per_customer'] = (
            sales_df['total_revenue'] / sales_df['num_customers']
        )
    
        sales_df['timestamp'] = sales_df['timestamp'].astype(str)
        sales_df['num_customers'] = sales_df['num_customers'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        sales_df['price_per_kwh'] = sales_df['price_per_kwh'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        sales_df['total_revenue'] = sales_df['total_revenue'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        sales_df['total_sales'] = sales_df['total_sales'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)

        # Generate composite keys
        sales_df['revenue_per_customer'] = sales_df['revenue_per_customer'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)

        
        # Step 3: Upload to DynamoDB
        awr.dynamodb.put_df(df=sales_df, table_name=table_name)

        # Step 4: Move Processed File to 'historical/' Folder
        move_file_to_historical(bucket_name, csv_file_key)

        success_msg = (
            f"Successfully processed {len(sales_df)} records\n"
            f"File: {csv_file_key}\n"
            f"Table: {table_name}\n"
            f"Timestamp: {datetime.now(timezone.utc).isoformat()}"
        )
        print("Data Processing Success", success_msg)
            
    except Exception as e:
        error_msg = f"Data processing error: {str(e)}"
        print("Data Processing Failure", error_msg)
        raise

def move_file_to_historical(bucket_name, csv_file_key):
    """Moves a processed file from 'incremental/' to 'historical/'."""
    historical_key = csv_file_key.replace("incremental/", "historical/")
    
    try:
        # Copy file to historical folder
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': csv_file_key},
            Key=historical_key
        )
        print(f"Copied {csv_file_key} to {historical_key}")

        # Delete original file from incremental folder
        s3_client.delete_object(Bucket=bucket_name, Key=csv_file_key)
        print(f"Deleted {csv_file_key} from {bucket_name}")

    except Exception as e:
        print(f"Error moving file {csv_file_key} to historical/: {e}")
        print("S3 File Move Failure", f"Failed to move {csv_file_key} to historical/")


def lambda_handler(event, context):   
    
    print("Received event:", json.dumps(event, indent=2))

    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        csv_file_key = record["s3"]["object"]["key"]

        print(f"Processing file: {csv_file_key} from bucket: {bucket_name}")

        # Only process files from 'incremental/' folder
        
        if csv_file_key.startswith("incremental/monthly_sales_data_"):
            process_file(bucket_name, csv_file_key)
        else:
            print(f"Skipping unrecognized file: {csv_file_key}")
            

    return {'statusCode': 200, 'body': 'Processing successful'}


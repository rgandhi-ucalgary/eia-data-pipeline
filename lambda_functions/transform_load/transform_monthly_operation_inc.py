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
        table_name = 'OperationalMonthlyData' 
        # Step 1: Read CSV file from S3
        monthly_df = read_csv_from_s3(bucket_name, csv_file_key)
        
        if monthly_df.empty:
            error_msg = f"Empty DataFrame from {csv_file_key}"
            print("Empty Data Warning", error_msg)
            return {'statusCode': 204, 'body': 'No content'}

        # Step 2: Data processing
        columns_to_drop = [
            'sectorid', 'location', 'consumption-for-eg-btu-units',
            'ash-content-units', 'consumption-for-eg-units', 'consumption-uto-units',
            'cost-units', 'cost-per-btu-units', 'generation-units', 'heat-content-units',
            'receipts-units', 'stocks-units', 'sulfur-content-units', 'total-consumption-units',
            'total-consumption-btu-units', 'consumption-uto-btu-units', 'receipts-btu-units'
        ]
        monthly_df = monthly_df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        monthly_df.fillna(0, inplace=True)

        # Rename columns
        rename_mapping = {
            'period': 'timestamp',
            'stateDescription': 'state',
            'sectorDescription': 'sector',
            'fuelTypeDescription': 'fuelType',
            'consumption-for-eg': 'consumption_eg',
            'consumption-for-eg-btu': 'consumption_eg_btu',
            'consumption-uto': 'consumption_uto',
            'consumption-uto-btu': 'consumption_uto_btu',
            'heat-content': 'heat_content',
            'cost-per-btu': 'cost_per_btu',
            'sulfur-content': 'sulfur_content',
            'total-consumption': 'total_consumption',
            'total-consumption-btu': 'total_consumption_btu'
        }
        monthly_df = monthly_df.rename(columns=rename_mapping)

        # Convert numeric columns
        numeric_cols = [
            'ash-content', 'consumption_eg', 'consumption_eg_btu', 'consumption_uto',
            'consumption_uto_btu', 'cost', 'cost_per_btu', 'generation', 
            'heat_content', 'receipts', 'receipts-btu', 'stocks', 'sulfur_content',
            'total_consumption', 'total_consumption_btu'
        ]
        
        for col in numeric_cols:
            if col in monthly_df.columns:
                monthly_df[col] = monthly_df[col].apply(
                    lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))

        # Generate composite keys
        monthly_df['state_month'] = monthly_df['state'] + "_" + monthly_df['timestamp'].str[:7]
        monthly_df['sector_fuelType'] = monthly_df['sector'] + "_" + monthly_df['fueltypeid']

        # Step 3: Upload to DynamoDB
        awr.dynamodb.put_df(df=monthly_df, table_name=table_name)

        # Step 4: Move Processed File to 'historical/' Folder
        move_file_to_historical(bucket_name, csv_file_key)

        success_msg = (
            f"Successfully processed {len(monthly_df)} records\n"
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
    # S3 bucket and file details
    
    print("Received event:", json.dumps(event, indent=2))

    for record in event.get("Records", []):
        bucket_name = record["s3"]["bucket"]["name"]
        csv_file_key = record["s3"]["object"]["key"]

        print(f"Processing file: {csv_file_key} from bucket: {bucket_name}")

        # Only process files from 'incremental/' folder
        
        if csv_file_key.startswith("incremental/monthly_electricity_data_"):
            process_file(bucket_name, csv_file_key)
        else:
            print(f"Skipping unrecognized file: {csv_file_key}")
            

    return {'statusCode': 200, 'body': 'Processing successful'}

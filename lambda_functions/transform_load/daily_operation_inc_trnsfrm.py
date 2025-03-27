import pandas as pd
import boto3
from io import BytesIO
import smtplib
import awswrangler as awr
from decimal import Decimal
from email.mime.text import MIMEText
from datetime import datetime, timezone


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


def read_parquet_from_s3(bucket, key):
    """Read Parquet file from S3 with error handling"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_bytes = response['Body'].read()
        return pd.read_parquet(BytesIO(parquet_bytes))
    except Exception as e:
        error_msg = f"Error reading Parquet file {key} from S3: {str(e)}"
        send_email("S3 Read Failure", error_msg, is_error=True)
        raise


# Initialize S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # S3 bucket and file details
    bucket_name = "eia-data-ucalgary"
    parquet_file_key = 'operational_month_sample.parquet'  # Updated to Parquet
    table_name = 'OperationalDailyData'    
    
    try:
        # Step 1: Read Parquet file from S3
        daily_df = read_parquet_from_s3(bucket_name, parquet_file_key)
        
        if daily_df.empty:
            error_msg = f"Empty DataFrame from {parquet_file_key}"
            send_email("Empty Data Warning", error_msg, is_error=True)
            return {'statusCode': 204, 'body': 'No content'}

        # Step 2: Data processing
        try:
            daily_df = daily_df.drop(columns=['fueltype', 'timezone-description'])

            daily_df.fillna(0, inplace=True)

            daily_df.rename(columns={
                'period': 'timestamp',
                'type-name': 'fueltype',
                'respondent-name': 'respondent_name',
                'value': 'energy_generated_MWh'
            }, inplace=True)
            
            daily_df['timestamp'] = daily_df['timestamp'].astype(str)
            
            daily_df['energy_generated_MWh'] = daily_df['energy_generated_MWh'].apply(lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))
            

        except Exception as e:
            error_msg = f"Data processing error: {str(e)}"
            send_email("Data Processing Failure", error_msg, is_error=True)
            raise
        
        # Step 3: Upload to DynamoDB
        try:
            awr.dynamodb.put_df(df=daily_df, table_name=table_name)
            success_msg = (
                f"Successfully processed {len(daily_df)} records\n"
                f"File: {parquet_file_key}\n"
                f"Table: {table_name}\n"
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}"
            )
            send_email("Data Processing Success", success_msg)
            
        except Exception as e:
            error_msg = f"DynamoDB upload error: {str(e)}"
            send_email("DynamoDB Failure", error_msg, is_error=True)
            raise

        return {'statusCode': 200, 'body': 'Processing successful'}

    except Exception as e:
        error_msg = f"Critical pipeline failure: {str(e)}"
        send_email("Pipeline Failure", error_msg, is_error=True)
        return {'statusCode': 500, 'body': error_msg}

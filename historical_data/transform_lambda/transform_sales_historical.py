
import os
import pandas as pd
import boto3
import awswrangler as awr
from decimal import Decimal
from datetime import datetime, timezone
from botocore.exceptions import ClientError


# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Get configuration from environment variables
        bucket_name = 'eia-data-ucalgary'
        table_name = 'SalesData'
        
        #csv_file_key = 'historical/retail_sales_2022.csv'
        #csv_file_key = 'historical/retail_sales_2023.csv'
        csv_file_key = 'historical/retail_sales_2024.csv'
        
        # Process and upload data
        process_retail_data(bucket_name, table_name, csv_file_key)
        
        return {
            'statusCode': 200,
            'body': f'Successfully processed {csv_file_key}'
        }
        
    except Exception as e:
        error_msg = f"Critical pipeline failure: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': error_msg
        }

def process_retail_data(bucket_name, table_name, csv_file_key):
    """Process and upload daily electrical data"""
    try:
        # Read and process data
        sales_df = read_csv_from_s3(bucket_name, csv_file_key)
        processed_df = transform_data(sales_df)
        
        # Upload to DynamoDB
        upload_dynamodb(processed_df, table_name)
        
        print(f"Successfully processed {len(processed_df)} records")
        
    except Exception as e:
        print(f"Processing failed: {str(e)}")
        raise

def read_csv_from_s3(bucket_name, key):
    """Read CSV file from S3"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return pd.read_csv(response['Body'])
    except ClientError as e:
        error_msg = f"S3 read error: {e.response['Error']['Message']}"
        print(error_msg)
        raise

def transform_data(df):
    """Perform data transformation"""
    try:
        print("Transforming data...")
        # Clean data

        df = df.drop(columns=[
                'customers-units', 'price-units', 
                'revenue-units', 'sales-units', 'stateid'
            ])
            
        df.fillna(0, inplace=True)
        
        df.rename(columns={
            'stateDescription': 'state',
            'period': 'timestamp',
            'customers': 'num_customers',
            'price': 'price_per_kwh',
            'revenue': 'total_revenue',
            'sales': 'total_sales'
        }, inplace=True)

        df['state_sectorid'] = df['state'] + "_" + df['sectorid']
        df['revenue_per_customer'] = (
            df['total_revenue'] / df['num_customers']
        )
    
        df['timestamp'] = df['timestamp'].astype(str)
        df['num_customers'] = df['num_customers'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        df['price_per_kwh'] = df['price_per_kwh'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        df['total_revenue'] = df['total_revenue'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        df['total_sales'] = df['total_sales'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)
        df['revenue_per_customer'] = df['revenue_per_customer'].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)

        
        key_columns = ['state_sectorid', 'timestamp']  # From Step 1
    
        # Track duplicates
        duplicates = df.duplicated(subset=key_columns, keep=False)
        duplicate_count = duplicates.sum()
        
        if duplicate_count > 0:
            print(f"Found {duplicate_count} duplicate(s). Keeping last occurrence.")
            
            print("Data transformation completed.")
        return df

    except Exception as e:
        print(f"Data transformation error: {str(e)}")
        raise

def upload_dynamodb(df, table_name):
    """Upload DataFrame to DynamoDB"""
    try:
        awr.config.region = "us-east-1"
        awr.dynamodb.put_df(
            df=df,
            table_name=table_name
        )
        print(f"Successfully uploaded to {table_name}")
        
    except Exception as e:
        print(f"DynamoDB upload failed: {str(e)}")
        raise
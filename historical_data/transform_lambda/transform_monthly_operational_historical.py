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
        table_name = 'OperationalMonthlyData'
        
        #csv_file_key = 'historical/operational_monthly_2022.csv'
        #csv_file_key = 'historical/operational_monthly_2023.csv'
        csv_file_key = 'historical/operational_monthly_2024.csv'
        
        # Process and upload data
        process_monthly_elect_data(bucket_name, table_name, csv_file_key)
        
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

def process_monthly_elect_data(bucket_name, table_name, csv_file_key):
    """Process and upload monthly electrical data"""
    try:
        # Read and process data
        monthly_df = read_csv_from_s3(bucket_name, csv_file_key)
        processed_df = transform_data(monthly_df)
        
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
        columns_to_drop = [
            'sectorid', 'location', 'consumption-for-eg-btu-units',
            'ash-content-units', 'consumption-for-eg-units', 'consumption-uto-units',
            'cost-units', 'cost-per-btu-units', 'generation-units', 'heat-content-units',
            'receipts-units', 'stocks-units', 'sulfur-content-units', 'total-consumption-units',
            'total-consumption-btu-units', 'consumption-uto-btu-units', 'receipts-btu-units'
        ]
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        df.fillna(0, inplace=True)

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
        df = df.rename(columns=rename_mapping)

        # Convert numeric columns
        numeric_cols = [
            'ash-content', 'consumption_eg', 'consumption_eg_btu', 'consumption_uto',
            'consumption_uto_btu', 'cost', 'cost_per_btu', 'generation', 
            'heat_content', 'receipts', 'receipts-btu', 'stocks', 'sulfur_content',
            'total_consumption', 'total_consumption_btu'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: Decimal(str(x)) if pd.notna(x) else Decimal('0'))

        # Generate composite keys
        df['state_month'] = df['state'] + "_" + df['timestamp'].str[:7]
        df['sector_fuelType'] = df['sector'] + "_" + df['fueltypeid']

        key_columns = ['state_month', 'sector_fuelType']  # From Step 1
    
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
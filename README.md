# EIA Data Pipeline & Forecasting System

An end-to-end cloud-native solution for collecting, processing, and analyzing energy data from the U.S. Energy Information Administration (EIA) API. The system enables historical and real-time electricity market analysis with machine learning forecasting capabilities.

## 🌟 Key Features

- **Automated Data Pipeline**
  - **Historical Data**: Batch processing via EC2 instances
  - **Incremental Updates**: Serverless Lambda functions triggered daily/monthly
  - **API Integration**: 3+ EIA endpoints supported

- **Cloud Data Lake**
  - S3 storage with time-partitioned Parquet files
  - DynamoDB for query-optimized operational data
  - Automated data validation/cleaning (null handling, type conversion)

- **Forecasting & Visualization**
  - Streamlit dashboard with interactive visualizations
  - Electricity usage forecasting models
  - Real-time data refresh capabilities


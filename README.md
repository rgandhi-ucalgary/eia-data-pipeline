# GridCast: Electricity Data Pipeline & Forecasting System

GridCast is an end-to-end electricity data pipeline and forecasting system that ingests real-time and historical data from the U.S. Energy Information Administration (EIA), processes and stores it using AWS services, forecasts trends using Prophet, and visualizes the insights with an interactive Streamlit dashboard.

---

## Overview

This project was built to enable smarter decision-making in energy management through:

- Automated ingestion of electricity and sales data via API
- Scalable storage using AWS S3 and DynamoDB
- Real-time and batch data processing with AWS Lambda and EC2
- Accurate forecasting using Facebook Prophet
- Streamlit-based dashboards for user-friendly insights

---

## Data Lifecycle

| Stage         | Technology Used             | Description |
|---------------|-----------------------------|-------------|
| Ingestion     | EIA API, Python              | Fetches daily/monthly electricity and sales data |
| Storage       | AWS S3, DynamoDB             | Raw data stored in S3 (historical + incremental), clean data in DynamoDB |
| Processing    | AWS Lambda, EC2              | Cleans and transforms data in real time and batches |
| Forecasting   | Prophet                      | Forecasts revenue and generation by fuel type and state |
| Visualization | Streamlit                    | Presents insights in an interactive dashboard |

---

## Optimization Techniques

- **Lambda Architecture**: Used for event-driven incremental processing without server overhead
- **EC2 for Historical Loads**: Used a higher-powered EC2 instance for batch ingestion of large datasets
- **S3 Partitioning**: Data stored in year-based folders to improve scalability and access time
- **DynamoDB Schema Design**: Custom partition/sort keys designed to support fast retrieval and filtering in the dashboard

---

## Challenges & Solutions

- **Large CSVs**: >1.6M rows split into partitioned year-wise files
- **Lambda Layer Limits**: Couldn’t use Parquet due to PyArrow size; stuck with CSV
- **IAM Constraints**: Limited permissions caused trigger setup issues; resolved with EventBridge
- **EC2 Limits**: Couldn’t access higher spec instances due to org constraints

---

## Future Improvements

- Migrate storage format to **Parquet** using AWS Glue or higher Lambda layers
- Integrate more ML models (e.g., XGBoost, LSTM) for advanced forecasting
- Add **filtering, theming, and export** features to the dashboard

---

## License

This project is licensed under the MIT License – feel free to fork and build upon it!

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from prophet import Prophet
import boto3

# Function to scan all items from a DynamoDB table (with basic pagination)
def scan_table(table):
    data = []
    response = table.scan()
    data.extend(response.get('Items', []))
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response.get('Items', []))
    return data

@st.cache_data
def load_sales_data():
    # Connect to DynamoDB and select the sales table
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table('SalesData')
    data = scan_table(table)
    return pd.DataFrame(data)

@st.cache_data
def load_op_monthly_data():
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table('OperationalMonthlyData')
    data = scan_table(table)
    return pd.DataFrame(data)

@st.cache_data
def load_op_daily_data():
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table('OperationalDailyData')
    data = scan_table(table)
    return pd.DataFrame(data)

# Load the data from DynamoDB
sales = load_sales_data()
op_monthly = load_op_monthly_data()
op_daily = load_op_daily_data()

print("Data Loading Complete")

# Convert the timestamp columns (if present) to datetime objects

if 'timestamp' in sales.columns:
    sales['timestamp'] = pd.to_datetime(sales['timestamp'])
if 'timestamp' in op_monthly.columns:
    op_monthly['timestamp'] = pd.to_datetime(op_monthly['timestamp'])
if 'timestamp' in op_daily.columns:
    op_daily['timestamp'] = pd.to_datetime(op_daily['timestamp'], format="mixed", dayfirst=True)


print("Date Conversion Complete")

st.sidebar.title("Analysis Type")
analysis_type = st.sidebar.radio("Select an analysis type:", ["Forecast", "EDA"])

if analysis_type == "Forecast":
    st.sidebar.title("Select Dataset for Forecast")
    dataset = st.sidebar.radio("Choose a dataset:", ["Sales", "Generation - Monthly", "Generation - Daily"])

    if dataset == "Sales":
        st.title("State-wise Revenue Forecasting")
        col1, col2 = st.columns(2)
        with col1:
            states = sales["state"].unique()
            selected_state = st.selectbox("Select a State:", states)
        with col2:
            sector = sales["sectorName"].unique()
            selected_sector = st.selectbox("Select a Sector:", sector)

        forecast_horizon = st.number_input("Enter forecast horizon (months)", min_value=1, value=6, step=1)

        if st.button("Run Forecast", key="sales_forecast"):
            f_s = sales[(sales['state'] == selected_state) & (sales['sectorName'] == selected_sector)]
            if not f_s.empty:
                df_prophet = f_s[['timestamp', 'total_revenue']].rename(columns={'timestamp': 'ds', 'total_revenue': 'y'})
                model = Prophet(seasonality_mode="multiplicative")
                model.fit(df_prophet)
                future = model.make_future_dataframe(periods=forecast_horizon, freq='M')
                forecast = model.predict(future)
                st.subheader("Forecasted Revenue Data")
                fig, ax = plt.subplots(figsize=(10, 5))
                model.plot(forecast, ax=ax)
                plt.title(f"Sales Forecast for {selected_state} - {selected_sector} Sector")
                st.pyplot(fig)
            else:
                st.error(f"No data available for {selected_state} in the {selected_sector} sector.")

    elif dataset == "Generation - Monthly":
        st.title("Electricity Generation Forecasting - Monthly Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            states = op_monthly["state"].unique()
            selected_state = st.selectbox("Select a State:", states)
        with col2:
            sector = op_monthly["sector"].unique()
            selected_sector = st.selectbox("Select a Sector:", sector)
        with col3:
            fuel = op_monthly["fuelType"].unique()
            selected_fuel = st.selectbox("Select a Fuel:", fuel)

        forecast_horizon = st.number_input("Enter forecast horizon (months)", min_value=1, value=6, step=1)

        if st.button("Run Forecast", key="monthly_forecast"):
            f_s = op_monthly[
                (op_monthly['state'] == selected_state) &
                (op_monthly['sector'] == selected_sector) &
                (op_monthly['fuelType'] == selected_fuel)
            ]
            if not f_s.empty:
                df_prophet = f_s[['timestamp', 'generation']].rename(columns={'timestamp': 'ds', 'generation': 'y'})
                model = Prophet(seasonality_mode="multiplicative")
                model.fit(df_prophet)
                future = model.make_future_dataframe(periods=forecast_horizon, freq='M')
                forecast = model.predict(future)
                st.subheader("Forecasted Generation Data")
                fig, ax = plt.subplots(figsize=(10, 5))
                model.plot(forecast, ax=ax)
                plt.title(f"Generation Forecast for {selected_state} - {selected_sector} Sector")
                st.pyplot(fig)
            else:
                st.error(f"No data available for {selected_state} in the {selected_sector} sector.")

    elif dataset == "Generation - Daily":
        st.title("Electricity Generation Forecasting - Daily Data")
        col1, col2, col3 = st.columns(3)
        with col1:
            states = op_daily["respondent_name"].unique()
            selected_state = st.selectbox("Select a State:", states)
        with col2:
            sector = op_daily["timezone"].unique()
            selected_sector = st.selectbox("Select a Sector:", sector)
        with col3:
            name = op_daily["fueltype"].unique()
            selected_name = st.selectbox("Select a Name:", name)

        forecast_horizon = st.number_input("Enter forecast horizon (days)", min_value=1, value=30, step=1)

        if st.button("Run Forecast", key="daily_forecast"):
            f_s = op_daily[
                (op_daily['respondent_name'] == selected_state) &
                (op_daily['timezone'] == selected_sector) &
                (op_daily['fueltype'] == selected_name)
            ]
            if not f_s.empty:
                df_prophet = f_s[['timestamp', 'energy_generated_MWh']].rename(columns={'timestamp': 'ds', 'energy_generated_MWh': 'y'})
                model = Prophet(seasonality_mode="multiplicative", daily_seasonality=True)
                model.fit(df_prophet)
                future = model.make_future_dataframe(periods=forecast_horizon, freq='D')
                forecast = model.predict(future)
                st.subheader("Forecasted Revenue Data")
                fig, ax = plt.subplots(figsize=(10, 5))
                model.plot(forecast, ax=ax)
                plt.title(f"Revenue Forecast for {selected_state} - {selected_sector} Sector")
                st.pyplot(fig)
            else:
                st.error(f"No data available for {selected_state} in the {selected_sector} sector.")

elif analysis_type == "EDA":
    st.sidebar.title("EDA Options")

    st.title("Exploratory Data Analysis")
    st.subheader("Revenue by Top 5 States (Sales Data)")
    sal = sales[sales['state'] != 'U.S. Total']
    top_5_states = sal.sort_values(by=['total_revenue'], ascending=False)['state'].unique()[:5]


    filtered_sales = sales[sales['state'].isin(top_5_states)]
    filtered_sales['timestamp'] = pd.to_datetime(filtered_sales['timestamp'])
    fig1, ax1 = plt.subplots(figsize=(16, 8))
    sns.lineplot(
        data=filtered_sales,
        x="timestamp",
        y="total_revenue",
        hue="state",
        marker="o",
        linewidth=2.5,
        palette="tab20",
        ci=None
    )
    plt.title("Revenue by Top 5 States", fontsize=14, pad=20)
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("Revenue (million dollars)", fontsize=12)
    plt.xticks(
        pd.date_range(start='2023-01-01', end='2025-01-01', freq='YS'),
        labels=[str(year) for year in range(2023, 2026)],
        rotation=45
    )
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.legend(title="State", bbox_to_anchor=(1.05, 1), loc="upper left", frameon=False)
    plt.tight_layout()
    st.pyplot(fig1)


    st.subheader("Electricity Generation by Top 5 Companies (Daily Data)")
    op_d = op_daily[op_daily['respondent_name'] != 'United States Lower 48']
    top_10_names = op_d[['respondent_name', 'energy_generated_MWh', 'timezone']]\
                     .sort_values(by=['energy_generated_MWh'], ascending=False)['respondent_name']\
                     .unique()[:5]

    filtered_names = op_daily[op_daily['respondent_name'].isin(top_10_names)]
    filtered_names['timestamp'] = pd.to_datetime(filtered_names['timestamp'])
    fig2, ax2 = plt.subplots(figsize=(16, 8))
    sns.lineplot(
        data=filtered_names,
        x="timestamp",
        y="energy_generated_MWh",
        hue="respondent_name",
        marker="o",
        linewidth=2.5,
        palette="tab20",
        ci=None
    )
    plt.title("Electricity Generation Value by Top 5 Companies", fontsize=14, pad=20)
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("Value (megawatthours)", fontsize=12)
    start_year = filtered_names['timestamp'].min().year
    end_year = filtered_names['timestamp'].max().year
    plt.xticks(
        pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='YS'),
        labels=[str(year) for year in range(start_year, end_year + 1)],
        rotation=45
    )
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.legend(title="Respondent", bbox_to_anchor=(1.05, 1), loc="upper left", frameon=False)
    plt.tight_layout()
    st.pyplot(fig2)


    st.subheader("Electricity Generation by Top 5 States (Monthly Data)")
    op_m = op_monthly[op_monthly['state'] != 'U.S. Total']
    top_10_names = op_m[['state', 'generation', 'fuelType']]\
                     .sort_values(by=['generation'], ascending=False)['state']\
                     .unique()[:5]

    filtered_names = op_monthly[op_monthly['state'].isin(top_10_names)]
    filtered_names['timestamp'] = pd.to_datetime(filtered_names['timestamp'])
    fig3, ax3 = plt.subplots(figsize=(16, 8))
    sns.lineplot(
        data=filtered_names,
        x="timestamp",
        y="generation",
        hue="state",
        marker="o",
        linewidth=2.5,
        palette="tab20",
        ci=None
    )
    plt.title("Electricity Generation by Top 5 States", fontsize=14, pad=20)
    plt.xlabel("Year", fontsize=12)
    plt.ylabel("Generation (thousand megawatthours)", fontsize=12)
    start_year = filtered_names['timestamp'].min().year
    end_year = filtered_names['timestamp'].max().year
    plt.xticks(
        pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='YS'),
        labels=[str(year) for year in range(start_year, end_year + 1)],
        rotation=45
    )
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.legend(title="State", bbox_to_anchor=(1.05, 1), loc="upper left", frameon=False)
    plt.tight_layout()
    st.pyplot(fig3)

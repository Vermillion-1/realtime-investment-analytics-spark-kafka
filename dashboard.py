import streamlit as st
import pandas as pd
import glob
import os
import time

# Configuration
DASHBOARD_DATA_PATH = "/home/ubuntu/dashboard_data"
ML_OUTPUT_DIR = "/home/ubuntu/nashville_investment_map_output"

st.set_page_config(page_title="Nashville Tourism & Investment Dashboard", layout="wide")

st.title("Nashville Tourism & Investment Dashboard")
st.markdown("Real-time insights combining **Yelp Sentiment**, **Airbnb Data**, and **ML Investment Predictions**.")

# Define Tabs
tab1, tab2 = st.tabs(["📡 Live Sentiment Pulse", "💰 Investment Opportunities"])

# --- Tab 2: Investment Map (ML) ---
with tab2:
    st.header("Predicted Investment Zones (ML Model)")
    st.markdown("Insights from `Linear_Regression_Nashville.py`")
    
    # Find the CSV file inside the Spark output directory
    ml_files = glob.glob(f"{ML_OUTPUT_DIR}/*.csv")
    
    if ml_files:
        df_ml = pd.read_csv(ml_files[0])
        st.dataframe(df_ml)
        
        if 'center_lat' in df_ml.columns and 'center_lon' in df_ml.columns:
            map_df = df_ml.rename(columns={'center_lat': 'latitude', 'center_lon': 'longitude'})
            st.map(map_df)
        else:
            st.info("Map requires latitude/longitude columns in ML output.")
    else:
        st.warning(f"ML Output not found at {ML_OUTPUT_DIR}. Run the ML script first!")

# --- Tab 1: Live Sentiment (Streaming) ---
with tab1:
    st.header("Real-Time Tourism Sentiment")
    
    def load_latest_data():
        try:
            files = glob.glob(f"{DASHBOARD_DATA_PATH}/*.csv")
            if not files:
                return None
            return pd.read_csv(files[0])
        except Exception as e:
            st.error(f"Error reading data: {e}")
            return None

    placeholder = st.empty()

    if st.checkbox("Start Live Updates", value=True):
        while True:
            df = load_latest_data()
            
            with placeholder.container():
                if df is not None and not df.empty:
                    latest = df.sort_values("year_month", ascending=False).iloc[0]
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Current Month", latest['year_month'])
                    col2.metric("Yelp Sentiment", f"{float(latest['yelp_avg_sentiment']):.2f}")
                    col3.metric("Airbnb Sentiment", f"{float(latest['airbnb_avg_sentiment']):.2f}")
                    
                    status = latest['tourism_status']
                    if "Booming" in status:
                        st.success(f"### Status: {status}")
                    elif "Poor" in status:
                        st.warning(f"### Status: {status}")
                    else:
                        st.info(f"### Status: {status}")
                    
                    st.subheader("Historical Trend")
                    st.dataframe(df.sort_values("year_month", ascending=False))
                    
                    st.subheader("Sentiment Gap Over Time")
                    st.line_chart(df.set_index("year_month")[["yelp_avg_sentiment", "airbnb_avg_sentiment"]])
                    
                else:
                    st.warning("Waiting for data from Spark... (Is the pipeline running?)")
            
            time.sleep(5)

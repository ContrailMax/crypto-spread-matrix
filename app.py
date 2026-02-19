import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# Set up the web page configuration
st.set_page_config(layout="wide", page_title="Crypto Matrix")
st.title("🚀 Crypto Arbitrage Matrix")

# Fetch data from BigQuery
try:
    # 1. Retrieve the service account key from Streamlit Secrets
    key_dict = json.loads(st.secrets["GCP_KEY"])
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = bigquery.Client(credentials=credentials, project='project-2c68fafb-fc39-4b54-b6f')
    
    st.success("✅ Successfully connected to BigQuery!")

    # 2. SQL Query to fetch data
    query = """
        SELECT * FROM `project-2c68fafb-fc39-4b54-b6f.spread_raw_data.price_logs` 
        LIMIT 5
    """
    
    # 3. Load the data into a Pandas DataFrame and display it
    df = client.query(query).to_dataframe()
    st.write("Sample data (first 5 rows) from BigQuery:")
    st.dataframe(df)

except Exception as e:
    st.error(f"❌ An error occurred: {e}")

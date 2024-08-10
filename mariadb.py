import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
import time

# Database connection settings
host = "10.10.154.44"
port = 3306  # Default MariaDB port
user = "developer"
password = "developer"

# Enhanced function to create a database connection
def create_db_engine():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}', pool_recycle=3600, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return engine
    except OperationalError as e:
        st.error("Could not connect to the database. Please check your connection settings.")
        st.stop()  # Stop further execution if the connection fails
        return None

# Create the engine
engine = create_db_engine()

# Hardcoded facility options
facility_options = ['AIP', 'AIN', 'A03', 'S01']

# Sidebar for facility selection
selected_facilities = st.sidebar.multiselect("Select Facilities", options=facility_options)

# Fetch unique options for dropdowns (SBU, FLG, Buyer) based on selected facilities
@st.cache_data
def fetch_filtered_options(engine, selected_facilities):
    if selected_facilities:
        selected_facilities_str = ', '.join([f"'{facility}'" for facility in selected_facilities])
        query = f"""
        SELECT DISTINCT SBU, FLG, Buyer 
        FROM brandix_production.aplrejection_wip
        WHERE Facility IN ({selected_facilities_str})
        """
    else:
        query = "SELECT DISTINCT SBU, FLG, Buyer FROM brandix_production.aplrejection_wip WHERE 1=0"  # No data when no facility is selected

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    return df['SBU'].dropna().unique().tolist(), df['FLG'].dropna().unique().tolist(), df['Buyer'].dropna().unique().tolist()

# Adjust other dropdowns based on selected facilities
sbu_options, flg_options, buyer_options = fetch_filtered_options(engine, selected_facilities)

# Sidebar for SBU, FLG, Buyer
selected_sbus = st.sidebar.multiselect("Select SBUs", options=sbu_options)
selected_flg = st.sidebar.multiselect("Select FLG (excluding '99')", options=[flg for flg in flg_options if flg != '99'])
selected_buyers = st.sidebar.multiselect("Select Buyers", options=buyer_options)

# Date range input on the body of the page
start_date = st.date_input("Start Date", value=datetime.now())
end_date = st.date_input("End Date", value=datetime.now())

# Helper function to format SQL IN clause
def format_sql_in_clause(field_name, selected_items):
    if selected_items:
        formatted_items = ', '.join([f"'{item}'" for item in selected_items])
        return f"{field_name} IN ({formatted_items})"
    else:
        return None

# Building the SQL query conditions
conditions = [
    f"transactionDate BETWEEN '{start_date}' AND '{end_date}'",
    format_sql_in_clause("Facility", selected_facilities),
    format_sql_in_clause("SBU", selected_sbus),
    format_sql_in_clause("FLG", selected_flg),
    format_sql_in_clause("Buyer", selected_buyers),
    "FLG <> '99'"
]

# Filtering out None values from conditions
conditions = [condition for condition in conditions if condition is not None]

# Final SQL query
query = f"""
SELECT * 
FROM brandix_production.aplrejection_wip aw 
WHERE {' AND '.join(conditions)}
"""

# Run the SQL query and load the data into a DataFrame
@st.cache_data
def load_data(engine, query):
    try:
        with engine.connect() as conn:
            data = pd.read_sql(query, conn)
        return data
    except OperationalError as e:
        st.error("Failed to execute the query. Please try again later.")
        return pd.DataFrame()

# Load data (but do not display it)
data = load_data(engine, query)

# Convert the data to CSV
@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

csv_data = convert_df_to_csv(data)

# Download button below the date selection
st.download_button(
    label="Download data as CSV",
    data=csv_data,
    file_name='filtered_data.csv',
    mime='text/csv',
)

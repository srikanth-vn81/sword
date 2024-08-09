import logging
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Access the secrets from the .streamlit/secrets.toml file
db_config = st.secrets["database"]

# Database connection settings
host = db_config["host"]
port = db_config["port"]  # Default MariaDB port
user = db_config["user"]
password = db_config["password"]

# Attempt to connect to the database
try:
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}')
except Exception as e:
    st.error("Failed to connect to the database.")
    logging.error("Database connection error:", exc_info=e)
    st.stop()

# Hardcoded facility options
facility_options = ['AIP', 'AIN', 'A03', 'S01']

# Sidebar for facility selection
selected_facilities = st.sidebar.multiselect("Select Facilities", options=facility_options)

# Fetch unique options for dropdowns (SBU, FLG, Buyer) based on selected facilities
@st.cache_data
def fetch_filtered_options(selected_facilities):
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
try:
    sbu_options, flg_options, buyer_options = fetch_filtered_options(selected_facilities)
except Exception as e:
    st.error("Failed to fetch filter options.")
    logging.error("Error fetching filter options:", exc_info=e)
    st.stop()

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
def load_data(query):
    try:
        with engine.connect() as conn:
            data = pd.read_sql(query, conn)
        return data
    except Exception as e:
        st.error("Failed to load data.")
        logging.error("Error loading data:", exc_info=e)
        st.stop()

# Load data (but do not display it)
data = load_data(query)

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

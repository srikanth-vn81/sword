import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
import time

# Load database connection settings from secrets
db_host = st.secrets["database"]["host"]
db_port = st.secrets["database"]["port"]
db_user = st.secrets["database"]["user"]
db_password = st.secrets["database"]["password"]

# Enhanced function to create a database connection
def create_db_engine():
    try:
        engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}', pool_recycle=3600, pool_pre_ping=True)
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
    

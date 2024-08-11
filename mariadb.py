import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from datetime import datetime
import time
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)

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
        logging.info("Successfully connected to the database.")
        return engine
    except OperationalError as e:
        logging.error(f"OperationalError: {e}")
        st.error(f"Could not connect to the database. OperationalError: {e}")
        st.stop()  # Stop further execution if the connection fails
        return None

# Create the engine
engine = create_db_engine()

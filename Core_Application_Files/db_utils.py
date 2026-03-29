"""
DATABASE UTILITY MODULE - PHONEPE PULSE ANALYSIS
------------------------------------------------
This module handles all SQL queries to the 'phonepe.db' SQLite database.
It provides data for the Streamlit dashboard and manages data cleaning 
(state name standardization) to ensure compatibility with GeoJSON maps.
"""

import sqlite3
import pandas as pd
import os

# Database Path Configuration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data_Analysis_Files", "phonepe.db")

def get_connection():
    """Establishes and returns a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)

# =============================================================================
# QUERY 1: Aggregated Transaction Data
# Used for: Time-series analysis, category distributions, and state-level Choropleth maps.
# =============================================================================
def get_aggregated_transaction(year=None, quarter=None):
    """
    Fetches filtered aggregated transaction records.
    SQL: SELECT * FROM aggregated_transaction WHERE year=? AND quarter=?
    """
    conn = get_connection()
    query = "SELECT * FROM aggregated_transaction"
    conditions = []
    if year: conditions.append(f"year = {year}")
    if quarter: conditions.append(f"quarter = {quarter}")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    df = pd.read_sql(query, conn)
    conn.close()
    # Clean state names to match GeoJSON
    df['state'] = df['state'].str.replace('-', ' ').str.replace('&', 'and').str.title()
    return df

# =============================================================================
# QUERY 2: Aggregated User & Engagement Data
# Used for: Tracking registered users and app opens engagement metrics.
# =============================================================================
def get_aggregated_user(year=None, quarter=None):
    """
    Fetches filtered aggregated user and device brand records.
    SQL: SELECT * FROM aggregated_user WHERE year=? AND quarter=?
    """
    conn = get_connection()
    query = "SELECT * FROM aggregated_user"
    conditions = []
    if year: conditions.append(f"year = {year}")
    if quarter: conditions.append(f"quarter = {quarter}")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    df = pd.read_sql(query, conn)
    conn.close()
    df['state'] = df['state'].str.replace('-', ' ').str.replace('&', 'and').str.title()
    return df

# =============================================================================
# QUERY 3: Map / District-level Transaction Data
# Used for: Granular district-level drill-downs and performance analysis.
# =============================================================================
def get_map_transaction(year=None, quarter=None):
    """
    Fetches filtered map transaction records for district-level analysis.
    SQL: SELECT * FROM map_transaction WHERE year=? AND quarter=?
    """
    conn = get_connection()
    query = "SELECT * FROM map_transaction"
    conditions = []
    if year: conditions.append(f"year = {year}")
    if quarter: conditions.append(f"quarter = {quarter}")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    df = pd.read_sql(query, conn)
    conn.close()
    df['state'] = df['state'].str.replace('-', ' ').str.replace('&', 'and').str.title()
    return df

# =============================================================================
# QUERY 4: Aggregated Insurance Data
# Used for: Tracking the growth and regional adoption of insurance products.
# =============================================================================
def get_insurance_data(year=None, quarter=None):
    """
    Fetches filtered insurance transaction records.
    SQL: SELECT * FROM aggregated_insurance WHERE year=? AND quarter=?
    """
    conn = get_connection()
    query = "SELECT * FROM aggregated_insurance"
    conditions = []
    if year: conditions.append(f"year = {year}")
    if quarter: conditions.append(f"quarter = {quarter}")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    df = pd.read_sql(query, conn)
    conn.close()
    df['state'] = df['state'].str.replace('-', ' ').str.replace('&', 'and').str.title()
    return df

# =============================================================================
# QUERY 5: Top Performing Entities (States, Districts, Pincodes)
# Used for: Leaderboards and identifying high-growth hotspots.
# =============================================================================
def get_top_data(entity_type='state'):
    """
    Fetches top-performing entity records based on type (state, district, or pincode).
    SQL: SELECT * FROM top_transaction WHERE entity_type = ?
    """
    conn = get_connection()
    query = f"SELECT * FROM top_transaction WHERE entity_type = '{entity_type}'"
    df = pd.read_sql(query, conn)
    conn.close()
    if entity_type == 'state':
        df['entity_name'] = df['entity_name'].str.replace('-', ' ').str.replace('&', 'and').str.title()
    return df

# =============================================================================
# HELPER: Get Unique Years
# Used for: Populating sidebar filters in the dashboard.
# =============================================================================
def get_years():
    """
    Fetches unique years available in the dataset for filtering.
    SQL: SELECT DISTINCT year FROM aggregated_transaction ORDER BY year
    """
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT year FROM aggregated_transaction ORDER BY year", conn)
    conn.close()
    return df['year'].tolist()

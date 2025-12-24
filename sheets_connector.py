import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st


# Google Sheets authentication
SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


@st.cache_resource
def authenticate_google_sheets():
    """
    Authenticate with Google Sheets API
    Uses st.secrets for credentials (Streamlit Cloud)
    """
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=SCOPE
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Authentication failed: {str(e)}")
        st.info("Please add gcp_service_account to Streamlit Secrets")
        return None


@st.cache_data(ttl=300)
def fetch_candidates_data(sheet_url, sheet_name="Candidates"):
    """
    Fetch all candidates data from Google Sheet
    TTL = 5 minutes (cache refresh)
    """
    try:
        client = authenticate_google_sheets()
        if client is None:
            return None
        
        # Open spreadsheet by URL
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all data
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            return None
        
        # Convert to DataFrame
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None


@st.cache_data(ttl=300)
def fetch_companies_data(sheet_url, sheet_name="Sheet4"):
    """
    Fetch all companies data from Google Sheet (Sheet4)
    TTL = 5 minutes (cache refresh)
    """
    try:
        client = authenticate_google_sheets()
        if client is None:
            return None
        
        # Open spreadsheet by URL
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get all data
        data = worksheet.get_all_values()
        
        if len(data) <= 1:
            return None
        
        # Convert to DataFrame
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        return df
    except Exception as e:
        st.error(f"Error fetching companies data: {str(e)}")
        return None


def get_column_headers(df):
    """Get all column headers from DataFrame"""
    if df is None:
        return []
    return list(df.columns)


def get_unique_values(df, column):
    """
    Get unique values from specific column
    Remove empty values
    """
    if df is None or column not in df.columns:
        return []
    
    values = df[column].unique().tolist()
    values = [v for v in values if v and str(v).strip()]  # Remove empty
    return sorted(values)


def apply_filter(df, column, value):
    """
    Apply single filter to DataFrame
    Returns filtered DataFrame
    """
    if df is None or column not in df.columns:
        return df
    
    filtered = df[df[column] == value]
    return filtered

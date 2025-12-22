from logger_config import logger
import logging
logging.disable(logging.INFO)  # सभी INFO messages hide करो

# ====================================================
# PAGE CONFIG
# ====================================================
import streamlit as st
st.set_page_config(
    page_title="Placement Agency",
    page_icon="💼",
    layout="wide"
)
st.markdown("""
<style>
.main .block-container {
    max-width: 1100px;      /* yahan size 900–1200 tak adjust kar sakte ho */
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    margin-left: auto;
    margin-right: auto;
}
</style>
""", unsafe_allow_html=True)

import pandas as pd
from datetime import datetime
import gspread
from rapidfuzz import fuzz
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
from login import render_login, logout, render_change_password, render_user_management
from status_updater import sync_all_statuses
# Import modular filters
from filter_candidates import render_filter_section as render_candidate_filter
from filter_companies import render_filter_section as render_company_filter
# Import export utilities
from export_utils import export_single_match, export_to_interview_sheet
# Import candidate wizard for internal use
from candidate_wizard_module import render_wizard
from job_matcher_module import run_matching, export_to_interview_sheet
import warnings
warnings.filterwarnings('ignore')



# ====================================================
# GOOGLE SHEETS CONNECTION
# ====================================================
SHEET_ID = "1rpuXdpfwjy0BQcaZcn0Acbh-Se6L3PvyNGiNu4NLcPA"
#logger
# All required columns for Candidates sheet
REQUIRED_COLUMNS = [
    "Candidate ID", "Date Applied", "Full Name", "Father Name", "DOB",
    "Gender", "Marital Status", "Category", "Aadhaar", "PAN",
    "Mobile", "Alt Mobile", "Email", "WhatsApp",
    "Current Address", "Current City", "Current District", "Current State", "Current PIN",
    "Permanent Address", "Permanent City", "Permanent District", "Permanent State", "Permanent PIN",
    "Job Pref 1", "Job Pref 2", "Job Pref 3", "Preferred Location",
    "Expected Salary", "Notice Period", "Willing to Relocate",
    "10th Board", "10th Year", "10th Percentage",
    "12th Board", "12th Stream", "12th Year", "12th Percentage",
    "Graduation Degree", "Graduation University", "Graduation Specialization", "Graduation Year", "Graduation Percentage",
    "Computer Skills", "Technical Skills", "Other Skills",
    "Hindi Level", "English Level",
    "Is Fresher", "Experience Years", "Experience Months", "Current CTC",
    "Disability", "Disability Details", "Own Vehicle", "Driving License",
    "Reference 1 Name", "Reference 1 Designation", "Reference 1 Organization", "Reference 1 Contact",
    "Reference 2 Name", "Reference 2 Contact",
    "Status",
]
logger.info("Required columns defined.")
# ✅ Deployment ke liye (local vs Streamlit Cloud)
if os.path.exists("credentials.json"):
    logger.info("Using local credentials file for Google Sheets authentication.")
    CRED_FILE = "credentials.json"  # Local development
else:
    logger.info("Using Streamlit secrets for Google Sheets authentication.")
    CRED_FILE = None  # Streamlit Cloud - use secrets


@st.cache_resource
def get_google_sheets_client():
    logger.info("Connecting to Google Sheets...")
    #"""Connect to Google Sheets"""
    logger.info("=" * 60)
    logger.info("Initializing Google Sheets client...")
    try:
        logger.info(f"Using credentials file: {CRED_FILE}")
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        logger.info(f"OAuth scopes: {scope}")
        if CRED_FILE:
            # Local development
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                CRED_FILE, scope
            )
            logger.info("Using local credentials file for Google Sheets authentication.")
        else:
            # Streamlit Cloud deployment
            logger.info("Loading credentials from Streamlit secrets...")
            creds_dict = st.secrets["gcp_service_account"]
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                creds_dict, scope
            )
            logger.info("Using Streamlit secrets for Google Sheets authentication.")
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Google Sheets connection error: {e}")
        st.error(f"❌ Google Sheets connection error: {e}")
        return None


# ====================================================
# VERIFY AND ADD MISSING COLUMNS (SAFE - NO DATA LOSS)
# ====================================================
@st.cache_resource
def verify_sheet_columns():
    """
    Check and add missing columns to Candidates sheet.
    SAFE: Only adds headers, never modifies or deletes existing data.
    Also removes duplicate column headers.
    """
    logger.info("Verifying Candidates sheet columns...")
    try:
        logger.info("Starting verification of sheet columns.")
        client = get_google_sheets_client()
        if client is None:
            logger.error("Cannot verify columns: No Google Sheets client.")
            return
        
        spreadsheet = client.open_by_key(SHEET_ID)
        logger.info(f"Opened spreadsheet with ID: {SHEET_ID}")
        worksheet = spreadsheet.worksheet("Candidates")
        logger.info("Accessed 'Candidates' worksheet.")
        
        # Get existing headers (Row 1 only)
        existing_headers = worksheet.row_values(1)
        logger
        # Check for duplicate headers and remove them
        if len(existing_headers) != len(set(existing_headers)):
            logger.warning("⚠️ Duplicate column headers detected, cleaning up...")
            
            # Keep only first occurrence of each column
            seen = set()
            clean_headers = []
            duplicate_cols = []
            logger.info(f"Existing headers: {existing_headers}")
            for i, header in enumerate(existing_headers):
                if header in seen:
                    duplicate_cols.append((i+1, header))  # 1-based index for gspread
                else:
                    clean_headers.append(header)
                    seen.add(header)
            
            if duplicate_cols:
                logger.info("Removing duplicate columns...")
                # Remove duplicates (delete from right to left to avoid index shift)
                for col_index, col_name in sorted(duplicate_cols, reverse=True):
                    worksheet.delete_columns(col_index)
                    logger.info(f"  ✅ Removed duplicate: {col_name}")
                
                existing_headers = clean_headers
                logger.info("✅ Sheet cleaned up")
        
        # Find missing columns
        missing = [col for col in REQUIRED_COLUMNS if col not in existing_headers]
        
        if missing:
            logger.info(f"Missing columns detected: {missing}")
            # SAFETY CHECK: Ensure we have at least 1 row of data before adding columns
            total_rows = len(worksheet.get_all_values())
            
            if total_rows > 0:
                logger.info("Adding missing columns to Candidates sheet...")
                # Add missing columns to the right of existing headers
                last_col = len(existing_headers)
                for i, col in enumerate(missing):
                    logger.info(f"  ➕ Adding column: {col}")
                    # update_cell only updates the cell, doesn't modify data
                    worksheet.update_cell(1, last_col + i + 1, col)
                
                logger.info(f"✅ Added {len(missing)} missing columns (No data affected)")
        
        return True
    except Exception as e:
        logger
        print(f"⚠️ Could not verify columns: {e}")
        return False


# ====================================================
# SMALL HELPER: MAKE ALL COLUMNS STRING
# ====================================================
def _to_str_df(data):
    logger.info("Converting dict/records to DataFrame and forcing all columns to string.")
    #"""Convert dict/records → DataFrame and force all columns to string."""
    df = pd.DataFrame(data)
    if not df.empty:
        logger.info("Converting all DataFrame columns to string type.")
        for col in df.columns:
            df[col] = df[col].astype(str)
    return df


# ====================================================
# DATA FETCHERS
# ====================================================
@st.cache_data(ttl=300)
def get_companies():
    logger.info("Fetching companies from CID sheet.")
    #"""Fetch companies from CID sheet"""
    try:
        logger.info("Attempting to get Google Sheets client for companies.")
        client = get_google_sheets_client()
        if client:
            logger.info("Google Sheets client obtained for companies.")
            sheet = client.open_by_key(SHEET_ID).worksheet("CID")
            data = sheet.get_all_records()
            return _to_str_df(data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching companies: {e}")
        st.warning(f"⚠️ Error fetching companies: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_vacancies():
    logger.info("Fetching vacancies from Sheet4.")
    #"""Fetch vacancies from Sheet4"""
    try:
        logger.info("Attempting to get Google Sheets client for vacancies.")
        client = get_google_sheets_client()
        if client:
            logger.info("Google Sheets client obtained for vacancies.")
            sheet = client.open_by_key(SHEET_ID).worksheet("Sheet4")
            data = sheet.get_all_records()
            return _to_str_df(data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching vacancies: {e}")
        st.warning(f"⚠️ Error fetching vacancies: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_candidates():
    logger.info("Fetching candidates from Candidates sheet.")
    #"""Fetch candidates from Candidates sheet"""
    try:
        logger.info("Attempting to get Google Sheets client for candidates.")
        client = get_google_sheets_client()
        if client:
            logger.info("Google Sheets client obtained for candidates.")
            sheet = client.open_by_key(SHEET_ID).worksheet("Candidates")
            data = sheet.get_all_records()
            return _to_str_df(data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching candidates: {e}")
        st.warning(f"⚠️ Error fetching candidates: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_interviews():
    logger.info("Fetching interviews from Interview_Records sheet.")
    #"""Fetch interviews from Interview_Records sheet"""
    try:
        logger.info("Attempting to get Google Sheets client for interviews.")
        client = get_google_sheets_client()
        if client: 
            logger.info("Google Sheets client obtained for interviews.")
            sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
            data = sheet.get_all_records()
            return _to_str_df(data)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching interviews: {e}")
        st.warning(f"⚠️ Error fetching interviews: {e}")
        return pd.DataFrame()


# ====================================================
# GENERIC APPEND TO SHEET
# ====================================================
def add_to_sheet(sheet_name, data_dict):
    logger.info(f"Adding data to sheet: {sheet_name} with data: {data_dict}")
    #"""Add new row to Google Sheet with dynamic header matching"""
    try:
        logger.info(f"Attempting to get Google Sheets client for adding data to {sheet_name}.")
        client = get_google_sheets_client()
        if client:
            logger.info(f"Google Sheets client obtained for adding data to {sheet_name}.")
            sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
            headers = sheet.row_values(1)

            # Create row with values in correct column order
            row = []
            for header in headers:
                clean_header = header.strip()
                value = data_dict.get(clean_header, "")
                row.append(value)

            sheet.append_row(row)
            st.success("✅ Data added to Google Sheets!")
            st.cache_data.clear()
            return True
        else:
            logger.error("Cannot add data: No Google Sheets client.")
            st.error("❌ Cannot connect to Google Sheets")
            return False
    except Exception as e:
        logger.error(f"Error adding data to sheet {sheet_name}: {e}")
        st.error(f"❌ Error adding data: {e}")
        return False


# ====================================================
# COMPANY ID GENERATOR
# ====================================================
def generate_next_cid():
    #"""Generate next CID in format CID0001, CID0002, etc."""
    try:
        companies_df = get_companies()
        if len(companies_df) == 0 or "CID" not in companies_df.columns:
            return "CID0001"
        existing_cids = companies_df["CID"].tolist()
        numbers = []
        for cid in existing_cids:
            if isinstance(cid, str) and cid.startswith("CID"):
                try:
                    num = int(cid.replace("CID", ""))
                    numbers.append(num)
                except Exception:
                    pass
        next_num = max(numbers) + 1 if numbers else 1
        return f"CID{next_num:04d}"
    except Exception as e:
        st.warning(f"⚠️ CID generation error: {e}")
        return f"CID{pd.Timestamp.now().strftime('%Y%m%d%H%M')}"


# ====================================================
# SESSION STATE INITIALIZATION
# ====================================================
if "logged_in" not in st.session_state:
        logger.info("Initializing session state for 'logged_in'.")
        st.session_state.logged_in = False
if "username" not in st.session_state:
        logger.info("Initializing session state for 'username'.")
        st.session_state.username = None
if "role" not in st.session_state:
        logger.info("Initializing session state for 'username'.")
        st.session_state.role = None
if "full_name" not in st.session_state:
        logger.info("Initializing session state for 'full_name'.")
        st.session_state.full_name = ""
if "email" not in st.session_state:
        logger.info("Initializing session state for 'email'.")
        st.session_state.email = ""
if "active_candidate_tab" not in st.session_state:
        logger.info("Initializing session state for 'active_candidate_tab'.")
        st.session_state["active_candidate_tab"] = "View All Candidates"





# ====================================================
# ADMIN TAB ROUTER (FIXED - NO DUPLICATE HEADER)
# ====================================================
def admin_tab():
    #st.title("👨‍💼 Admin Dashboard")
    admin_menu = st.sidebar.radio(
        "Admin Menu",
        [
            "📊 Dashboard",
            "🏢 Company Management",
            "💼 Vacancy Management",
            "👥 Candidate Management",
            "🔍 Advanced Filtering",
            "🎯 Job Matching",
            "📋 Interview Management",
            "📈 Reports & Analytics",
        ],
    )
    if admin_menu == "📊 Dashboard":
        logger.info("Admin selected Dashboard tab.")
        admin_dashboard()
    elif admin_menu == "🏢 Company Management":
        logger.info("Admin selected Company Management tab.")
        admin_company_mgmt()
    elif admin_menu == "💼 Vacancy Management":
        logger.info("Admin selected Vacancy Management tab.")
        admin_vacancy_mgmt()
    elif admin_menu == "👥 Candidate Management":
        logger.info("Admin selected Candidate Management tab.")
        admin_candidate_mgmt()
    elif admin_menu == "🔍 Advanced Filtering":
        logger.info("Admin selected Advanced Filtering tab.")
        admin_advanced_filtering()
    elif admin_menu == "🎯 Job Matching":
        logger.info("Admin selected Job Matching tab.")
        admin_job_matching()
    elif admin_menu == "📋 Interview Management":
        logger.info("Admin selected Interview Management tab.")
        admin_interview_mgmt()
    elif admin_menu == "📈 Reports & Analytics":
        logger.info("Admin selected Reports & Analytics tab.")
        admin_reports()


# ====================================================
# ADMIN: DASHBOARD
# ====================================================
def admin_dashboard():
    st.subheader("📊 Dashboard Overview")
    companies_df = get_companies()
    vacancies_df = get_vacancies()
    candidates_df = get_candidates()
    interviews_df = get_interviews()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Companies", len(companies_df))
    col2.metric("Total Vacancies", len(vacancies_df))
    col3.metric("Total Candidates", len(candidates_df))
    col4.metric("Total Interviews", len(interviews_df))

    st.write("---")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 Recent Interviews")
        if len(interviews_df) > 0:
            st.dataframe(interviews_df.head(5), use_container_width=True)
        else:
            st.info("No interviews yet")
    with col2:
        st.subheader("📊 Interview Status")
        if len(interviews_df) > 0 and "Status" in interviews_df.columns:
            status_count = interviews_df["Status"].value_counts()
            st.bar_chart(status_count)
        else:
            st.info("No data available")
# ====================================================
# ADMIN: COMPANY MANAGEMENT
# ====================================================
def admin_company_mgmt():
    st.subheader("🏢 Company Management")
    tab1, tab2, tab3 = st.tabs(
        ["View All Companies", "Add Company", "Edit/Delete Company"]
    )

    # View tab
    with tab1:
        st.write("### All Companies")
        companies_df = get_companies()
        if len(companies_df) > 0:
            st.dataframe(companies_df, use_container_width=True)
        else:
            st.info("No companies found")

    # Add tab
    with tab2:
        st.write("### Add New Company")
        with st.form("add_company_form"):
            col1, col2 = st.columns(2)
            with col1:
                name = st.text_input("Company Name *")
                industry = st.selectbox(
                    "Industry *",
                    [
                        "IT/Software",
                        "Finance/Banking",
                        "Healthcare/Medical",
                        "Education/Training",
                        "Manufacturing",
                        "Retail/E-commerce",
                        "Construction/Real Estate",
                        "Hospitality/Tourism",
                        "Transportation/Logistics",
                        "Telecommunications",
                        "Media/Entertainment",
                        "Agriculture/Farming",
                        "Automotive",
                        "Pharmaceuticals",
                        "Energy/Utilities",
                        "Food & Beverage",
                        "Fashion/Textiles",
                        "Consulting",
                        "Legal Services",
                        "Marketing/Advertising",
                        "Insurance",
                        "NGO/Non-Profit",
                        "Government",
                        "Other",
                    ],
                )
                contact = st.text_input("Contact Email")
                phone = st.text_input("Contact Number")
                alt_phone = st.text_input("Alternate Number")
            with col2:
                description = st.text_area("Company Description")
                address = st.text_area("Address of Company")
                city = st.text_input("City")
                state = st.text_input("State")
                pin_code = st.text_input("PIN Code")
                website = st.text_input("Website")
            submitted = st.form_submit_button("➕ Add Company")

        if submitted:
            if not name:
                st.error("⚠️ Company Name is required!")
            else:
                final_cid = generate_next_cid()
                data = {
                    "Company Name": name,
                    "CID": final_cid,
                    "Industry": industry,
                    "Company Description": description,
                    "Contact Number": phone,
                    "Address of Company": address,
                    "City": city,
                    "State": state,
                    "PIN Code": pin_code,
                    "Email": contact,
                    "Website": website,
                    "Date Added": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "alternateNumber": alt_phone,
                }
                if add_to_sheet("CID", data):
                    st.success(
                        f"✅ Company '{name}' added successfully with CID: {final_cid}"
                    )
                    st.balloons()
                    import time

                    time.sleep(3)
                    st.rerun()

    # Edit tab
    with tab3:
        st.write("### Edit/Delete Company")
        st.info("⚠️ Edit/Delete functionality - Update directly in Google Sheets")
        companies_df = get_companies()
        if len(companies_df) > 0:
            st.dataframe(companies_df, use_container_width=True)


# ====================================================
# COMMON HELPERS FOR VACANCIES
# ====================================================
def _norm(s: str) -> str:
    return str(s).strip().lower().replace("_", " ").replace("-", " ")


def normalize_series(vals):
    return sorted({str(x).strip() for x in vals if str(x).strip()})


@st.cache_data(ttl=300)
def get_company_name_options():
    df = get_companies()
    pick = (
        "Company Name"
        if "Company Name" in df.columns
        else ("Company_Name" if "Company_Name" in df.columns else None)
    )
    return normalize_series(df[pick].dropna().tolist()) if pick else []


@st.cache_data(ttl=300)
def get_designation_options():
    client = get_google_sheets_client()
    if not client:
        return []
    ws = client.open_by_key(SHEET_ID).worksheet("Sheet2")
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    return (
        normalize_series(df["Designation"].dropna().tolist())
        if "Designation" in df.columns
        else []
    )


@st.cache_data(ttl=300)
def get_sheet2_df():
    try:
        client = get_google_sheets_client()
        if not client:
            return pd.DataFrame()
        ws = client.open_by_key(SHEET_ID).worksheet("Sheet2")
        rows = ws.get_all_records()
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def lookup_cid(company_name: str) -> str:
    df = get_companies()
    if df.empty:
        return ""
    name_col = (
        "Company Name"
        if "Company Name" in df.columns
        else ("Company_Name" if "Company_Name" in df.columns else None)
    )
    if not name_col or "CID" not in df.columns:
        return ""
    key = str(company_name).strip().lower()
    hit = df[df[name_col].astype(str).str.strip().str.lower() == key]
    return str(hit.iloc[0]["CID"]) if not hit.empty and "CID" in hit.columns else ""


def lookup_dgn_id(job_title: str) -> str:
    df2 = get_sheet2_df()
    if df2.empty:
        return ""
    des_col = "Designation" if "Designation" in df2.columns else None
    dgn_col = (
        "DGN ID"
        if "DGN ID" in df2.columns
        else ("DGN_ID" if "DGN_ID" in df2.columns else None)
    )
    if not des_col or not dgn_col:
        return ""
    key = str(job_title).strip().lower()
    hit = df2[df2[des_col].astype(str).str.strip().str.lower() == key]
    return str(hit.iloc[0][dgn_col]) if not hit.empty else ""


@st.cache_data(ttl=300)
def get_education_options():
    """
    Priority:
    1) Sheet title case-insensitive: 'education' → column 'Academic Education'
    2) Fallback: 'Sheet4' → column 'Education Required'
    3) Final defaults list
    """
    try:
        client = get_google_sheets_client()
        if not client:
            return ["12th", "Diploma", "B.Sc", "B.Tech", "M.Sc", "MBA"]

        ss = client.open_by_key(SHEET_ID)
        titles = [ws.title for ws in ss.worksheets()]
        edu_title = next(
            (t for t in titles if t.strip().lower() == "education"), None
        )

        if edu_title:
            try:
                ws = ss.worksheet(edu_title)
                rows = ws.get_all_records()
                if rows:
                    df = pd.DataFrame(rows)
                    if "Academic Education" in df.columns:
                        return normalize_series(
                            df["Academic Education"].dropna().tolist()
                        )
            except gspread.exceptions.WorksheetNotFound:
                pass

        if "Sheet4" in titles:
            try:
                ws4 = ss.worksheet("Sheet4")
                rows4 = ws4.get_all_records()
                if rows4:
                    df4 = pd.DataFrame(rows4)
                    if "Education Required" in df4.columns:
                        return normalize_series(
                            df4["Education Required"].dropna().tolist()
                        )
            except gspread.exceptions.WorksheetNotFound:
                pass

        return ["12th", "Diploma", "B.Sc", "B.Tech", "M.Sc", "MBA"]
    except Exception:
        return ["12th", "Diploma", "B.Sc", "B.Tech", "M.Sc", "MBA"]


def add_to_sheet_safe(sheet_name, data_dict):
    #"""Header-insensitive append; maps keys to Sheet first-row headers after normalizing."""
    try:
        client = get_google_sheets_client()
        if not client:
            st.error("❌ Cannot connect to Google Sheets")
            return False
        ws = client.open_by_key(SHEET_ID).worksheet(sheet_name)
        headers = ws.row_values(1)
        norm_map = {
            _norm(k): (v.strip() if isinstance(v, str) else v)
            for k, v in data_dict.items()
        }
        row = [norm_map.get(_norm(h), "") for h in headers]
        ws.append_row(row)
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"❌ Error adding data: {e}")
        return False

def require_permission(flag_column):
    """Users sheet me diya gaya permission column check kare."""
    role = (st.session_state.get("role") or "").upper()
    if role == "ADMIN":
        return  # Admin ko hamesha allow

    try:
        client = get_google_sheets_client()
        if not client:
            st.error("❌ Permission check failed (no Sheets connection)")
            st.stop()

        sheet = client.open_by_key(SHEET_ID).worksheet("Users")
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        if df.empty or flag_column not in df.columns:
            st.error("❌ Permission column missing – contact admin.")
            st.stop()

        uname = st.session_state.get("username", "")
        row = df[df["Username"].str.lower() == str(uname).lower()]

        allowed_values = ["yes", "1", "true", "y"]
        if row.empty or str(row.iloc[0][flag_column]).strip().lower() not in allowed_values:
            st.error("❌ Aapko is module ka access nahi diya gaya. Please contact admin.")
            st.stop()
    except Exception as e:
        st.error(f"⚠️ Permission check error: {e}")
        st.stop()

# ====================================================
# ADMIN: VACANCY MANAGEMENT
# ====================================================
def admin_vacancy_mgmt():
    st.subheader("💼 Vacancy Management")
    tab1, tab2 = st.tabs(["View All Vacancies", "Add Vacancy"])

    # View tab
    with tab1:
        st.write("### All Vacancies")
        vacancies_df = get_vacancies()
        if len(vacancies_df) > 0:
            st.dataframe(vacancies_df, use_container_width=True)
        else:
            st.info("No vacancies found")

    # Add tab
    with tab2:
        st.write("### Add New Vacancy")

        company_opts = get_company_name_options()
        dgn_opts = get_designation_options()
        edu_opts = get_education_options()

        if "vac_step" not in st.session_state:
            st.session_state.vac_step = 0  # reserved if multi-step later

        with st.form("add_vacancy_form", clear_on_submit=True):
            t_basic, t_req, t_log = st.tabs(["Basic", "Requirements", "Logistics"])

            # Basic
            with t_basic:
                col1, col2 = st.columns(2)
                with col1:
                    company_name = st.selectbox(
                        "Company Name",
                        company_opts,
                        index=0 if company_opts else None,
                        placeholder="Select company",
                    )
                    job_title = st.selectbox(
                        "Job Title (Designation)",
                        dgn_opts,
                        index=0 if dgn_opts else None,
                        placeholder="Select designation",
                    )
                    vacancy_count = st.number_input(
                        "Vacancy Count", min_value=0, step=1, value=1
                    )
                with col2:
                    salary = st.text_input("Salary")
                    job_desc = st.text_area("Job Description", height=120)

            # Requirements
            with t_req:
                col1, col2 = st.columns(2)
                with col1:
                    edu_req = st.selectbox(
                        "Education Required",
                        edu_opts
                        if edu_opts
                        else ["12th", "Diploma", "B.Sc", "B.Tech", "M.Sc", "MBA"],
                        index=0,
                        placeholder="Select education",
                    )
                    skills_req = st.text_input("Skills Required")
                    exp_req = st.text_input("Experience Required")
                    gender_pref = st.selectbox(
                        "Gender Preference", ["Any", "Male", "Female", "Other"]
                    )
                with col2:
                    urgency = st.selectbox(
                        "Urgency Level", ["Low", "Medium", "High", "Critical"], index=0
                    )
                    age_min = st.number_input(
                        "Age Range Min", min_value=0, max_value=100, value=18, step=1
                    )
                    age_max = st.number_input(
                        "Age Range Max", min_value=0, max_value=100, value=60, step=1
                    )
                    pref_loc = st.text_input("Preferred Candidate Location")

            # Logistics
            with t_log:
                col1, col2 = st.columns(2)
                with col1:
                    job_city = st.text_input("Job Location/City")
                    job_type = st.selectbox(
                        "Job Type",
                        ["Full-time", "Part-time", "Contract", "Internship"],
                    )
                    work_mode = st.selectbox(
                        "Work Mode", ["Onsite", "Remote", "Hybrid"]
                    )
                    job_timing = st.text_input("Job Timing")
                with col2:
                    shift_timings = st.text_input("Shift Timings")
                    notice_ok = st.selectbox(
                        "Notice Period Acceptable",
                        ["Any", "Immediate", "15 days", "30 days", "60 days"],
                    )
                    contact_person = st.text_input("Contact Person")
                    contact_number = st.text_input("Contact Number")
                notes = st.text_area("Additional Notes", height=100)

            submit_vac = st.form_submit_button("➕ Add Vacancy", type="primary")

        if submit_vac:
            if not str(company_name).strip() or not str(job_title).strip():
                st.error("⚠️ Company Name और Job Title आवश्यक हैं!")
            else:
                cid_val = lookup_cid(company_name)
                dgn_id_val = lookup_dgn_id(job_title)

                data = {
                    "Company Name": str(company_name).strip(),
                    "CID": cid_val,
                    "Job Title": str(job_title).strip(),
                    "DGN ID": dgn_id_val,
                    "Salary": str(salary).strip(),
                    "Job Description": str(job_desc).strip(),
                    "Education Required": str(edu_req).strip(),
                    "Skills Required": str(skills_req).strip(),
                    "Experience Required": str(exp_req).strip(),
                    "Vacancy Count": vacancy_count,
                    "Contact Person": str(contact_person).strip(),
                    "Contact Number": str(contact_number).strip(),
                    "Additional Notes": str(notes).strip(),
                    "Date Added": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Job Location/City": str(job_city).strip(),
                    "Gender Preference": str(gender_pref).strip(),
                    "Job Type": str(job_type).strip(),
                    "Job Timing": str(job_timing).strip(),
                    "Shift Timings": str(shift_timings).strip(),
                    "Notice Period Acceptable": str(notice_ok).strip(),
                    "Work Mode": str(work_mode).strip(),
                    "Age Range Min": age_min,
                    "Age Range Max": age_max,
                    "Preferred Candidate Location": str(pref_loc).strip(),
                    "status": "Open",
                    "Urgency Level": str(urgency).strip(),
                }

                if add_to_sheet_safe("Sheet4", data):
                    st.success(f"✅ Vacancy for '{job_title}' added!")
                    st.balloons()
                    import time

                    time.sleep(3)
                    st.rerun()


# ====================================================
# ADMIN: CANDIDATE MANAGEMENT
# ====================================================
def admin_candidate_mgmt():
    logger.info("Entering admin_candidate_mgmt function.")
    st.subheader("👥 Candidate Management")
    
    # Define tab names
    tab_names = [
        "View All Candidates", 
        "Add Candidate (Quick)", 
        "Add Candidate (Full Form)"
    ]
    
    # Get current active tab from session state
    if "active_candidate_tab" not in st.session_state:
        logger.info("Setting default active_candidate_tab to 'View All Candidates'")
        st.session_state["active_candidate_tab"] = tab_names[0]
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(tab_names)
    logger.info(f"Created tabs: {tab_names}")
    
    # TAB 1: View All Candidates
    with tab1:
        logger.info("Displaying all candidates in View All Candidates tab.")
        st.write("### All Candidates")
        logger.info("Showing info tip for advanced filtering.")
        st.info(
            "💡 Tip: Use 'Advanced Filtering' menu for detailed filtering with 70+ columns"
        )
        candidates_df = get_candidates()
        logger.info(f"Number of candidates fetched: {len(candidates_df)}")
        if len(candidates_df) > 0:
            logger.info("Displaying candidates dataframe.")
            st.dataframe(candidates_df, use_container_width=True, height=400)
            csv = candidates_df.to_csv(index=False)
            logger.info("Prepared CSV data for download.")
            st.download_button(
                label="📤 Download All Candidates (CSV)",
                data=csv,
                file_name="all_candidates.csv",
                mime="text/csv",
            )
            logger.info("Displayed download button for all candidates CSV.")
        else:
            st.info("No candidates found")
            logger.info("No candidates found to display.")

    # TAB 2: Quick Form
    with tab2:
        st.write("### Add Candidate (Quick Form)")
        with st.form("add_candidate_form"):
            col1, col2 = st.columns(2)
            with col1:
                cand_id = st.text_input("Candidate ID")
                name = st.text_input("Full Name")
                email = st.text_input("Email")
                phone = st.text_input("Phone")
                gender = st.radio("Gender", ["Male", "Female", "Other"])
            with col2:
                address = st.text_input("Address")
                education = st.selectbox(
                    "Education", ["12th", "B.Tech", "B.Sc", "M.Tech", "M.Sc", "MBA"]
                )
                skills = st.text_input("Skills (comma separated)")
                salary_exp = st.number_input("Expected Salary", min_value=0)
                experience = st.text_input("Experience (years)")
            location_pref = st.text_input("Location Preference")
            cand_submitted = st.form_submit_button("➕ Add Candidate")
        
        if cand_submitted:
            if not name or not email or not phone:
                st.error("⚠️ Name, Email, and Phone are required!")
            else:
                data = {
                    "Candidate_ID": cand_id,
                    "Name": name,
                    "Email": email,
                    "Phone": phone,
                    "Address": address,
                    "Education": education,
                    "Gender": gender,
                    "Skills": skills,
                    "Salary_Expected": salary_exp,
                    "Experience": experience,
                    "Location_Preference": location_pref,
                }
                if add_to_sheet_safe("Candidates", data):
                    st.success(f"✅ Candidate '{name}' added!")
                    # Stay on this tab
                    st.session_state["active_candidate_tab"] = tab_names[1]
    
    # TAB 3: Full Form (Wizard)
    with tab3:
        logger.info("Rendering Full Form (Wizard) tab for adding candidate.")
        #st.write("### Add Candidate (Full 70+ Field Form)")
        #st.info("📝 Complete candidate registration with all details")
        
        # Set active tab when this tab is accessed
        #st.session_state["active_candidate_tab"] = tab_names[2]
        
        # Call the wizard module
        render_wizard()
# ====================================================
# ADMIN: ADVANCED FILTERING
# ====================================================
def admin_advanced_filtering():
    st.subheader("🔍 Advanced Filtering System")
    st.markdown(
        "Filter candidates and companies with dynamic column selection - All 70+ columns available!"
    )
    st.info("💡 Pro Tip: Apply multiple cascading filters to narrow down results precisely")

    tab1, tab2 = st.tabs(["🔎 Filter Candidates", "🏢 Filter Companies"])
    with tab1:
        #st.markdown("### Filter Candidates (All 70+ Columns)")
        st.markdown(
            "Select any column, apply filters, and download filtered results"
        )
        render_candidate_filter()
    with tab2:
        st.markdown("### Filter Companies/Vacancies (All Columns)")
        st.markdown(
            "Select any column, apply filters, and download filtered results"
        )
        render_company_filter()


# ====================================================
# JOB MATCHING (Hybrid engine using job_matcher_module)
# ====================================================
def admin_job_matching():
    st.subheader("Job Matching Engine – Hybrid AI")

    # DEBUG (temporary – later hata sakte ho)
    #st.write("DEBUG filtered_df in session_state:", "filtered_df" in st.session_state)
    #st.write("DEBUG companies_filtered_df in session_state:", "companies_filtered_df" in st.session_state)

    # 1) Data load – Advanced Filtering ka respect

    col1, col2 = st.columns(2)

    # Candidates
    with col1:
        st.markdown("### 👥 Candidates Data")
        if "filtered_df" in st.session_state and st.session_state.get("filtered_df") is not None:
            candidates_df = st.session_state["filtered_df"]
            st.success(f"Using {len(candidates_df)} filtered candidates from Advanced Filtering.")
        else:
            candidates_df = get_candidates()
            st.warning(f"No candidate filters applied. Using all {len(candidates_df)} candidates.")

    # Companies / Vacancies
    with col2:
        st.markdown("### 🏢 Companies Data")
        if "companies_filtered_df" in st.session_state and st.session_state.get("companies_filtered_df") is not None:
            vacancies_df = st.session_state["companies_filtered_df"]
            st.success(f"Using {len(vacancies_df)} filtered companies from Advanced Filtering.")
        else:
            vacancies_df = get_vacancies()
            st.warning(f"No company filters applied. Using all {len(vacancies_df)} vacancies.")

    # Safety checks
    if len(candidates_df) == 0:
        st.error("No candidates available for matching!")
        return
    if len(vacancies_df) == 0:
        st.error("No companies/vacancies available for matching!")
        return

    st.markdown(f"**Using:** {len(candidates_df)} candidates × {len(vacancies_df)} vacancies")

    

    st.markdown("---")
    with st.expander("How Smart Matching Works", expanded=False):
        st.markdown(
            """
- **Critical fields (100%)**
  - Job Title – 40% weight (checks all 3 job preferences)
  - Location – 30% weight (preferred + current city)
  - Salary – 30% weight (numeric with 30% tolerance)

- **Optional bonus (20%)**
  - Skills
  - Education
  - Experience

- **Thresholds**
  - Minimum field match: 50%
  - Minimum total score: 40%
  - Uses fuzzy matching for text fields
            """
        )

    st.markdown("---")

    # 2) Controls row
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        run_matching_btn = st.button(
            "Run Smart Matching", type="primary", use_container_width=True
        )
    with col2:
        refresh_btn = st.button("Refresh Data", use_container_width=True)
    with col3:
        clear_btn = st.button("Clear Matches", use_container_width=True)

    if refresh_btn:
        st.cache_data.clear()
        st.success("Data refreshed from Google Sheets.")
        st.experimental_rerun()

    if clear_btn:
        if "matches_admin" in st.session_state:
            del st.session_state["matches_admin"]
        st.success("Cleared in-memory matches.")
        return

    # 3) Run matching
    if run_matching_btn:
        progress_placeholder = st.empty()
        status_placeholder = st.empty()

        def _progress(p):
            progress_placeholder.progress(p)

        def _status(txt):
            status_placeholder.text(txt)

        with st.spinner("Running matching algorithm..."):
            matches_df = run_matching(
                candidates_df,
                vacancies_df,
                progress_callback=_progress,
                status_callback=_status,
            )
            st.session_state["matches_admin"] = matches_df

        progress_placeholder.empty()
        status_placeholder.empty()

    # 4) Show results
    # 🆕 FIX: Check for matches FIRST
    if "matches_admin" in st.session_state and len(st.session_state["matches_admin"]) > 0:
        matches_df = st.session_state["matches_admin"]

        st.success(f"✅ Found {len(matches_df)} matches.")
        st.markdown("---")
        st.subheader("Match Results")

        # Reset index and prepare selection list
        matches_df = matches_df.reset_index(drop=True)
        selected_rows = []

        # -------- Export controls (top) --------
        st.markdown("### 📤 Export Controls")
        col_e1, col_e2, col_e3 = st.columns([2, 1, 1])

        with col_e1:
            st.info(f"Selected: {len(selected_rows)} / {len(matches_df)} matches")

        with col_e2:
            if st.button("Export ALL Matches to Interview Records", key="adm_export_all_top"):
                gc = get_google_sheets_client()
                if gc:
                    all_matches = [row.to_dict() for _, row in matches_df.iterrows()]
                    success, msg = export_to_interview_sheet(gc, SHEET_ID, all_matches)
                    if success:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)
                else:
                    st.error("Google Sheets connection failed.")

        with col_e3:
            # Button text will be updated after loop once we know selected_rows length
            pass

        st.markdown("---")

        # -------- Row-wise results with selection + Quick Add --------
        for idx, row in matches_df.iterrows():
            c1, c2, c3 = st.columns([0.08, 0.72, 0.20])

            with c1:
                selected = st.checkbox("", key=f"adm_match_sel_{idx}")
                if selected:
                    selected_rows.append(idx)

            with c2:
                st.markdown(
                    f"**{row['Full Name']}** → **{row['Company Name']}** "
                    f"(CID: {row['CID']})"
                )
                st.caption(
                    f"Job: {row['Job Title']} | "
                    f"Match Score: **{row['Match Score']}%** | "
                    f"Salary: {row.get('Salary', 'N/A')} | "
                    f"Industry: {row.get('Industry', 'N/A')}"
                )
                st.caption(
                    f"Contact: {row.get('Contact', 'N/A')} "
                    f"({row.get('Phone', 'N/A')})"
                )

            with c3:
                if st.button("Quick Add", key=f"adm_quick_add_{idx}"):
                    gc = get_google_sheets_client()
                    if gc:
                        success, msg = export_to_interview_sheet(
                            gc,
                            SHEET_ID,
                            [row.to_dict()],
                        )
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                    else:
                        st.error("Google Sheets connection failed.")
            st.markdown("---")

        # -------- Batch export (selected) --------
        st.markdown(f"**Selected: {len(selected_rows)} matches**")
        if st.button(
            f"Export Selected ({len(selected_rows)})",
            type="primary",
            key="adm_export_selected",
            disabled=len(selected_rows) == 0,
        ):
            if not selected_rows:
                st.warning("Please select at least one match.")
            else:
                gc = get_google_sheets_client()
                if gc:
                    selected_matches = [
                        matches_df.iloc[i].to_dict() for i in selected_rows
                    ]
                    success, msg = export_to_interview_sheet(
                        gc,
                        SHEET_ID,
                        selected_matches,
                    )
                    if success:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)
                else:
                    st.error("Google Sheets connection failed.")

    # 🆕 FIX: No matches found message
    elif "matches_admin" in st.session_state and len(st.session_state["matches_admin"]) == 0:
        st.warning("⚠️ No Matches Found!")
        st.info("""
        💡 **Why no matches?**
        - Job preferences don't match available positions
        - Salary expectations are too high or too low
        - Location preferences don't match company locations
        - Experience or skills don't align with requirements
        
        **💡 What to try:**
        1. Adjust candidate filters (lower salary expectations, expand locations)
        2. Adjust company/vacancy filters (different industries, job types)
        3. Check candidate Job Preferences 1, 2, 3 in the sheet
        4. Run matching again with different filters
        """)

    else:
        st.info("Run Smart Matching to see results.") 



# ====================================================
# INTERVIEW MANAGEMENT (keep your existing implementation here)
# ====================================================

# ========== HELPER FUNCTIONS (Outside admin_interview_mgmt) ==========

# ========== HELPER FUNCTIONS (Outside admin_interview_mgmt) ==========

# ========== HELPER FUNCTIONS (Outside admin_interview_mgmt) ==========
# ========== HELPER FUNCTIONS (Outside admin_interview_mgmt) ==========

def check_existing_selections(candidate_id):
    """Check if candidate already has any 'Selected' result status"""
    try:
        client = get_google_sheets_client()
        if not client:
            return []
        
        sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:
            return []
        
        headers = all_data[0]
        candidate_id_col = headers.index('Candidate ID') if 'Candidate ID' in headers else -1
        result_status_col = headers.index('Result Status') if 'Result Status' in headers else -1
        record_id_col = headers.index('Record ID') if 'Record ID' in headers else -1
        company_col = headers.index('Company Name') if 'Company Name' in headers else -1
        job_title_col = headers.index('Job Title') if 'Job Title' in headers else -1
        
        if candidate_id_col == -1 or result_status_col == -1:
            return []
        
        existing_selections = []
        for row_idx, row in enumerate(all_data[1:], start=2):
            if (candidate_id_col < len(row) and 
                str(row[candidate_id_col]).strip() == str(candidate_id).strip() and
                result_status_col < len(row) and
                str(row[result_status_col]).strip() == "Selected"):
                
                record_id = row[record_id_col] if record_id_col < len(row) else "Unknown"
                company = row[company_col] if company_col < len(row) else "Unknown"
                job_title = row[job_title_col] if job_title_col < len(row) else "Unknown"
                
                existing_selections.append({
                    'row_num': row_idx,
                    'record_id': record_id,
                    'company': company,
                    'job_title': job_title
                })
        
        logger.info(f"Found {len(existing_selections)} existing selections for candidate {candidate_id}")
        return existing_selections
        
    except Exception as e:
        logger.error(f"Error checking existing selections: {e}")
        return []


def update_selection_status(current_record_id, keep_selection, existing_selections):
    """Update selection status based on user choice"""
    try:
        client = get_google_sheets_client()
        if not client:
            logger.error("Failed to get sheets client")
            return False
        
        sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
        all_data = sheet.get_all_values()
        headers = all_data[0]
        
        result_status_col = headers.index('Result Status') + 1 if 'Result Status' in headers else -1
        record_id_col = headers.index('Record ID') if 'Record ID' in headers else -1
        
        if result_status_col == -1 or record_id_col == -1:
            return False
        
        # Determine which rows to reject
        if keep_selection == 'current':
            # Keep current, reject existing
            reject_rows = [sel['row_num'] for sel in existing_selections]
        else:
            # Keep existing, reject current
            reject_rows = []
            for row_idx, row in enumerate(all_data[1:], start=2):
                if (record_id_col < len(row) and 
                    str(row[record_id_col]).strip() == str(current_record_id).strip()):
                    reject_rows.append(row_idx)
        
        # Update rejected records to "Rejected"
        updates = []
        for row_num in reject_rows:
            updates.append({
                'range': f"{chr(64 + result_status_col)}{row_num}",
                'values': [['Rejected']]
            })
        
        if updates:
            sheet.batch_update(updates)
            logger.info(f"Updated {len(reject_rows)} records to 'Rejected'")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating selection status: {e}")
        return False


def cancel_pending_entries(candidate_id, current_record_id):
    """Cancel all PENDING entries for a candidate when one is SELECTED"""
    try:
        client = get_google_sheets_client()
        if not client:
            return False
        
        sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
        all_data = sheet.get_all_values()
        
        if len(all_data) <= 1:
            return False
        
        headers = all_data[0]
        candidate_id_col = headers.index('Candidate ID') if 'Candidate ID' in headers else -1
        result_status_col = headers.index('Result Status') if 'Result Status' in headers else -1
        record_id_col = headers.index('Record ID') if 'Record ID' in headers else -1
        
        if candidate_id_col == -1 or result_status_col == -1:
            return False
        
        # Find all PENDING entries to cancel (exclude current and existing selections)
        pending_rows = []
        for row_idx, row in enumerate(all_data[1:], start=2):
            record_id = row[record_id_col] if record_id_col < len(row) else ""
            result_status = row[result_status_col] if result_status_col < len(row) else ""
            cand_id = row[candidate_id_col] if candidate_id_col < len(row) else ""
            
            if (str(cand_id).strip() == str(candidate_id).strip() and
                str(result_status).strip() == "Pending" and
                str(record_id).strip() != str(current_record_id).strip()):
                
                pending_rows.append(row_idx)
        
        # Update pending entries to "Cancelled due to Selection"
        if pending_rows:
            updates = []
            result_col = headers.index('Result Status') + 1 if 'Result Status' in headers else -1
            interview_status_col = headers.index('Interview Status') + 1 if 'Interview Status' in headers else -1
            
            for row_num in pending_rows:
                # Update Result Status
                updates.append({
                    'range': f"{chr(64 + result_col)}{row_num}",
                    'values': [['Cancelled due to Selection']]  # ✅ नया message
                })
                # Update Interview Status
                updates.append({
                    'range': f"{chr(64 + interview_status_col)}{row_num}",
                    'values': [['Cancelled due to Selection']]  # ✅ दोनों जगह
                })
            
            if updates:
                sheet.batch_update(updates)
                logger.info(f"Cancelled {len(pending_rows)} pending entries for candidate {candidate_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error cancelling pending entries: {e}")
        return False

# ========== ADD THESE 3 NEW FUNCTIONS ==========

def get_closed_vacancy_keys(vacancies_df):
    """
    Extract closed vacancies as (CID, Job Title) tuples
    Returns: Set of ('CID', 'Job Title') tuples
    """
    if len(vacancies_df) == 0:
        return set()
    
    closed = vacancies_df[
        vacancies_df['status'].str.strip().str.upper() == 'CLOSED'
    ].copy()
    
    if len(closed) == 0:
        return set()
    
    closed_keys = set()
    
    for _, row in closed.iterrows():
        cid = str(row.get('CID', '')).strip()
        job_title = str(row.get('Job Title', '')).strip()
        
        if cid and job_title:
            closed_keys.add((cid, job_title))
    
    return closed_keys


def is_vacancy_closed(interview_row, vacancies_df):
    """Check if this interview's vacancy is closed"""
    closed_keys = get_closed_vacancy_keys(vacancies_df)
    
    interview_cid = str(interview_row.get('CID', '')).strip()
    interview_job = str(interview_row.get('Job Title', '')).strip()
    
    return (interview_cid, interview_job) in closed_keys


def get_schedulable_interviews(interviews_df, vacancies_df):
    """
    Filter interviews that can be scheduled.
    - Interview Status = 'Matched'
    - No duplicates (Candidate+Company+Job)
    - Vacancy not closed
    """
    if len(interviews_df) == 0:
        return pd.DataFrame()
    
    # Keep only 'Matched' status
    matched = interviews_df[
        interviews_df['Interview Status'].str.strip() == 'Matched'
    ].copy()
    
    if len(matched) == 0:
        return pd.DataFrame()
        matched = matched[
        matched['Result Status'] != 'Rejected'
    ]
    
    # Filter out closed vacancies
    matched = matched[
        ~matched.apply(lambda row: is_vacancy_closed(row, vacancies_df), axis=1)
    ]
    
    # Check for duplicates
    grouped = interviews_df.groupby(['Candidate ID', 'Company Name', 'Job Title'])
    duplicates_to_hide = set()
    
    for (cand_id, company, job_title), group in grouped:
        if len(group) > 1:
            statuses = group['Interview Status'].unique()
            
            if 'Interview Scheduled' in statuses or 'Interview Completed' in statuses:
                matched_in_group = group[group['Interview Status'] == 'Matched']
                duplicates_to_hide.update(matched_in_group['Record ID'].tolist())
    
    schedulable = matched[~matched['Record ID'].isin(duplicates_to_hide)]
    
    return schedulable.reset_index(drop=True)


def get_updatable_interviews(interviews_df, vacancies_df):
    """
    Filter interviews that can have results updated.
    - Interview Status = 'Scheduled' or 'Completed'
    - Result Status != 'Selected'
    - Candidate doesn't have existing selection
    - Vacancy not closed
    """
    if len(interviews_df) == 0:
        return pd.DataFrame()
    
    # Only scheduled/completed
    active = interviews_df[
        interviews_df['Interview Status'].isin(['Interview Scheduled', 'Interview Completed'])
    ].copy()
    
    # Exclude selected and cancelled
    active = active[
        ~active['Result Status'].isin(['Selected', 'Cancelled due to Selection'])
    ]
    
    # Exclude candidates with existing selections
    selected_candidates = interviews_df[
        interviews_df['Result Status'] == 'Selected'
    ]['Candidate ID'].unique()
    
    active = active[~active['Candidate ID'].isin(selected_candidates)]
    
    # Exclude closed vacancies
    active = active[
        ~active.apply(lambda row: is_vacancy_closed(row, vacancies_df), axis=1)
    ]
    
    return active.reset_index(drop=True)

# ========== END OF NEW FUNCTIONS ==========
# ========== MAIN FUNCTION ==========

def admin_interview_mgmt():
    logger.info("Entering admin_interview_mgmt function.")
    #st.markdown(
    #"""
    #Complete Interview Management System
    #- Dashboard with statistics
    #- Schedule interviews with auto-fetch candidate/company details
    #- Update interview results
    #- View all interviews with filters
    #"""
    #)
    st.subheader("📋 Interview Management System")
    st.markdown("---")
    interviews_df = get_interviews()
    vacancies_df = get_vacancies()
    candidates_df = get_candidates()
    companies_df = get_companies()

    
    # Create 4 tabs
    logger.info("Creating tabs for Interview Management.")      
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard", 
        "🗓️ Schedule Interview", 
        "✅ Update Result", 
        "📋 All Interviews"
    ])
    
    # ========== TAB 1: DASHBOARD ==========
    with tab1:
        st.markdown("### 📊 Interview Dashboard")
        
        interviews_df = get_interviews()
        logger.info(f"Fetched {len(interviews_df)} interview records.")    
        
        if len(interviews_df) > 0:
            logger.info("Rendering statistics cards.")
            col1, col2, col3, col4 = st.columns(4)
            logger.info("Calculating metrics for dashboard.")   
            
            with col1:
                logger.info("Calculating Matched count.")
                matched_count = len(interviews_df[interviews_df['Interview Status'] == 'Matched'])
                st.metric("🎯 Matched", matched_count)
            
            with col2:
                logger.info("Calculating Scheduled count.")
                scheduled_count = len(interviews_df[interviews_df['Interview Status'] == 'Interview Scheduled'])
                st.metric("🗓️ Scheduled", scheduled_count)
            
            with col3:
                logger.info("Calculating Completed count.")
                completed_count = len(interviews_df[interviews_df['Interview Status'] == 'Interview Completed'])
                st.metric("✅ Completed", completed_count)
            
            with col4:
                logger.info("Calculating Selected count.")
                selected_count = len(interviews_df[interviews_df['Result Status'] == 'Selected'])
                st.metric("🎉 Selected", selected_count)
            
            st.markdown("---")
            logger.info("Rendering today's interviews section.")
            
            # Today's Interviews
            today = pd.Timestamp.now().strftime('%Y-%m-%d')
            logger.info(f"Filtering interviews for today: {today}")
            
            if 'Interview Date' in interviews_df.columns:
                logger.info("Interview Date column found, proceeding with filtering.")
                interviews_df['Interview Date'] = interviews_df['Interview Date'].astype(str)
                logger.info("Filtering interviews scheduled for today.")
                today_interviews = interviews_df[
                    (interviews_df['Interview Status'] == 'Interview Scheduled') & 
                    (interviews_df['Interview Date'].str.contains(today, na=False))
                ]
                logger.info(f"Found {len(today_interviews)} interviews scheduled for today.")
                if len(today_interviews) > 0:
                    logger.info("Displaying today's interviews.")
                    st.markdown("### 🔥 Today's Interviews")
                    logger.info("Preparing columns for display.")
                    display_cols = ['Record ID', 'Full Name', 'Company Name', 'Job Title', 'Interview Time']
                    logger.info("Checking available columns in today's interviews.")
                    available_cols = [col for col in display_cols if col in today_interviews.columns]
                    logger.info(f"Available columns for display: {available_cols}") 
                    st.dataframe(today_interviews[available_cols], use_container_width=True, hide_index=True)
                    logger.info("Displayed today's interviews dataframe.")
                else:
                    logger.info("No interviews scheduled for today.")
                    st.info("✅ No interviews scheduled for today")
            else:
                st.info("✅ No interviews scheduled for today")
            
            st.markdown("---")
            
            # Pending Actions
            pending = interviews_df[interviews_df['Interview Status'] == 'Matched']
            if len(pending) > 0:
                st.markdown("### ⚠️ Pending to Schedule")
                st.warning(f"{len(pending)} interviews need to be scheduled!")
                display_cols = ['Record ID', 'Full Name', 'Company Name', 'Job Title', 'Match Score']
                available_cols = [col for col in display_cols if col in pending.columns]
                st.dataframe(pending[available_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No interview records found. Export some matches from Job Matching to get started!")
    
    # ========== TAB 2: SCHEDULE INTERVIEW ==========
    # TAB 2: SCHEDULE INTERVIEW - COMPLETE CODE

    # ========== TAB 2: SCHEDULE INTERVIEW ==========
    with tab2:
        st.markdown("### 🗓️ Schedule Interview")
        
        matched_interviews = get_schedulable_interviews(interviews_df, vacancies_df)
        
        if len(matched_interviews) > 0:
            st.success(f"📊 {len(matched_interviews)} interviews ready to schedule")
            
            record_options = matched_interviews.apply(
                lambda x: f"{x['Record ID']} | {x['Full Name']} → {x['Company Name']} ({x['Job Title']})", 
                axis=1
            ).tolist()
            
            selected_record = st.selectbox(
                "Select Interview Record",
                record_options,
                key="schedule_select"
            )
            
            if selected_record:
                record_id = selected_record.split('|')[0].strip()
                interview_data = matched_interviews[matched_interviews['Record ID'] == record_id].iloc[0]
                
                st.markdown("---")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 👤 Candidate Details")
                    st.write(f"**Name:** {interview_data['Full Name']}")
                    st.write(f"**ID:** {interview_data['Candidate ID']}")
                    
                    candidates_df_local = get_candidates()
                    cand_id_col = 'Candidate_ID' if 'Candidate_ID' in candidates_df_local.columns else 'Candidate ID'
                    candidate_info = candidates_df_local[
                        candidates_df_local[cand_id_col] == interview_data['Candidate ID']
                    ]
                    
                    if len(candidate_info) > 0:
                        logger.info("Candidate details found in Candidates sheet.")
                        candidate = candidate_info.iloc[0]
                        phone = candidate.get('Phone', candidate.get('Mobile', candidate.get('Contact Number', 'N/A')))
                        email = candidate.get('Email', 'N/A')
                        st.write(f"**📞 Phone:** {phone}")
                        st.write(f"**📧 Email:** {email}")
                    else:
                        st.warning("⚠️ Candidate details not found in Candidates sheet")
                
                with col2:
                    logger.info("Displaying company details.")
                    st.markdown("#### 🏢 Company Details")
                    st.write(f"**Company:** {interview_data['Company Name']}")
                    st.write(f"**Position:** {interview_data['Job Title']}")
                    st.write(f"**Match Score:** {interview_data['Match Score']}")
                    
                    companies_df_local = get_companies()
                    cid_col = 'CID' if 'CID' in companies_df_local.columns else 'CID'
                    company_info = companies_df_local[
                        companies_df_local[cid_col] == interview_data['CID']
                    ]
                    
                    if len(company_info) > 0:
                        company = company_info.iloc[0]
                        contact_person = company.get('Contact Person', 'N/A')
                        company_phone = company.get('Contact Number', 'N/A')
                        company_address = company.get('Address of Company', company.get('Address', 'N/A'))
                        st.write(f"**👤 Contact:** {contact_person}")
                        st.write(f"**📞 Phone:** {company_phone}")
                        st.write(f"**📍 Address:** {company_address}")
                    else:
                        st.warning("⚠️ Company details not found in CID sheet")
                
                st.markdown("---")
                
                with st.form("schedule_interview_form"):
                    st.markdown("#### 📅 Schedule Details")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        interview_date = st.date_input(
                            "Interview Date *",
                            min_value=pd.Timestamp.now().date(),
                            value=pd.Timestamp.now().date() + pd.Timedelta(days=1)
                        )
                        
                        interview_time = st.time_input(
                            "Interview Time *",
                            value=pd.Timestamp('10:00').time()
                        )
                        
                        round_number = st.selectbox(
                            "Interview Round *",
                            ["Round 1", "Round 2", "Round 3", "Final Round"],
                            index=0
                        )
                    
                    with col2:
                        interview_mode = st.selectbox(
                            "Interview Mode *",
                            ["Offline", "Online", "Hybrid"],
                            index=0
                        )
                        
                        if interview_mode == "Online":
                            meeting_link = st.text_input("Meeting Link (Google Meet/Zoom)")
                        else:
                            interview_location = st.text_input("Interview Location")
                        
                        interviewer_name = st.text_input("Interviewer Name")
                    
                    remarks = st.text_area("Additional Remarks", height=100)
                    
                    submit_schedule = st.form_submit_button("📅 Schedule Interview", type="primary")
                
                if submit_schedule:
                    try:
                        client = get_google_sheets_client()
                        if client:
                            sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
                            
                            all_data = sheet.get_all_values()
                            if not all_data:
                                st.error("Interview_Records sheet is empty. Please add header row.")
                                return
                            headers = all_data[0]
                            
                            row_to_update = None
                            for idx, row in enumerate(all_data[1:], start=2):
                                if row[0] == record_id:
                                    row_to_update = idx
                                    break
                            
                            if row_to_update:
                                updates = []
                                
                                status_col = headers.index('Interview Status') + 1 if 'Interview Status' in headers else 9
                                updates.append({
                                    'range': f"{chr(64 + status_col)}{row_to_update}",
                                    'values': [['Interview Scheduled']]
                                })
                                
                                date_col = headers.index('Interview Date') + 1 if 'Interview Date' in headers else 10
                                updates.append({
                                    'range': f"{chr(64 + date_col)}{row_to_update}",
                                    'values': [[interview_date.strftime('%Y-%m-%d')]]
                                })
                                
                                time_col = headers.index('Interview Time') + 1 if 'Interview Time' in headers else 11
                                updates.append({
                                    'range': f"{chr(64 + time_col)}{row_to_update}",
                                    'values': [[interview_time.strftime('%H:%M')]]
                                })
                                
                                round_col = headers.index('Interview Round') + 1 if 'Interview Round' in headers else 12
                                updates.append({
                                    'range': f"{chr(64 + round_col)}{row_to_update}",
                                    'values': [[round_number]]
                                })
                                
                                remarks_col = headers.index('Remarks') + 1 if 'Remarks' in headers else 16
                                location_info = meeting_link if interview_mode == "Online" else interview_location if interview_mode != "Hybrid" else ""
                                full_remarks = f"Mode: {interview_mode} | Location/Link: {location_info} | Interviewer: {interviewer_name} | {remarks}"
                                updates.append({
                                    'range': f"{chr(64 + remarks_col)}{row_to_update}",
                                    'values': [[full_remarks]]
                                })
                                
                                updated_col = headers.index('Last Updated') + 1 if 'Last Updated' in headers else 17
                                updates.append({
                                    'range': f"{chr(64 + updated_col)}{row_to_update}",
                                    'values': [[pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]]
                                })
                                
                                updated_by_col = headers.index('Updated By') + 1 if 'Updated By' in headers else 18
                                updates.append({
                                    'range': f"{chr(64 + updated_by_col)}{row_to_update}",
                                    'values': [[st.session_state.get('user', 'Admin')]]
                                })
                                
                                sheet.batch_update(updates)
                                
                                st.success("✅ Interview scheduled successfully!")
                                st.info("📧 Email notification will be sent automatically via App Script")
                                st.balloons()
                                st.cache_data.clear()
                                
                                import time
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error("❌ Record not found in sheet")
                        else:
                            st.error("❌ Could not connect to Google Sheets")
                    except Exception as e:
                        st.error(f"❌ Error scheduling interview: {str(e)}")
        else:
            st.info("✅ No interviews to schedule!")
    # ========== TAB 3: UPDATE RESULT ==========
    with tab3:
        st.markdown("### ✅ Update Interview Result")
        logger.info("Fetching interview and vacancy data for result update.")   
        
        logger.info("Merging vacancy status into interview records.")
    # NEW: merge vacancy Status into interviews_df using CID + Job Title
        if len(vacancies_df) > 0 and 'CID' in vacancies_df.columns and 'Job Title' in vacancies_df.columns and 'status' in vacancies_df.columns:
            logger.info("Vacancy DataFrame has required columns. Proceeding with merge.")
            interviews_df = interviews_df.merge(
                vacancies_df[['CID', 'Job Title', 'status']],
                on=['CID', 'Job Title'],
                how='left'
            )

        
        if len(interviews_df) > 0:
            updatable_interviews = get_updatable_interviews(interviews_df, vacancies_df)
            if len(updatable_interviews) > 0:
                st.info(f"📊 {len(updatable_interviews)} interviews to update")
                
                record_options = updatable_interviews.apply(
                    lambda x: f"{x['Record ID']} | {x['Full Name']} → {x['Company Name']} | {x.get('Interview Date', 'No Date')} {x.get('Interview Time', '')}", 
                    axis=1
                ).tolist()
                
                selected_record = st.selectbox(
                    "Select Interview Record",
                    record_options,
                    key="update_select"
                )
                
                if selected_record:
                    record_id = selected_record.split('|')[0].strip()
                    interview_data = updatable_interviews[updatable_interviews['Record ID'] == record_id].iloc[0]
                    
                    st.markdown("---")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Candidate:** {interview_data['Full Name']}")
                        st.write(f"**Company:** {interview_data['Company Name']}")
                        st.write(f"**Position:** {interview_data['Job Title']}")
                    
                    with col2:
                        st.write(f"**Date:** {interview_data.get('Interview Date', 'N/A')}")
                        st.write(f"**Time:** {interview_data.get('Interview Time', 'N/A')}")
                        st.write(f"**Round:** {interview_data.get('Interview Round', 'N/A')}")
                    
                    st.markdown("---")
                    
                    with st.form("update_result_form"):
                        st.markdown("#### 📝 Update Result")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            interview_status = st.selectbox(
                                "Interview Status *",
                                ["Interview Scheduled", "Interview Completed", "Cancelled", "Rescheduled"],
                                index=1
                            )
                            
                            result_status = st.selectbox(
                                "Result Status *",
                                ["Pending", "Selected", "Rejected", "On Hold", "Next Round", "Candidate Declined", "Company Declined"],
                                index=0
                            )
                        
                        with col2:
                            if result_status == "Selected":
                                salary_offered = st.number_input(
                                    "Salary Offered (₹)",
                                    min_value=0,
                                    step=1000,
                                    value=int(interview_data.get('Salary Offered', 0)) if pd.notna(interview_data.get('Salary Offered')) and interview_data.get('Salary Offered') != '' else 0
                                )
                                
                                joining_date = st.date_input(
                                    "Joining Date",
                                    value=pd.Timestamp.now().date() + pd.Timedelta(days=15)
                                )
                            else:
                                salary_offered = None
                                joining_date = None
                        
                        feedback = st.text_area("Feedback/Remarks", height=150)
                        
                        submit_result = st.form_submit_button("💾 Update Result", type="primary")
                    
                    if submit_result:
                        existing_selections = []
                        choice = 'proceed'
                        
                        if result_status == "Selected":
                            # 🆕 LOGIC 1: Cancel all PENDING entries
                            cancel_pending_entries(interview_data['Candidate ID'], record_id)
                            
                            # 🆕 LOGIC 2: Handle multiple SELECTED entries
                            existing_selections = check_existing_selections(interview_data['Candidate ID'])
                            
                            if existing_selections:
                                st.warning("⚠️ This candidate already has selection(s)!")
                                
                                st.write("### Existing Selection(s):")
                                for sel in existing_selections:
                                    st.write(f"- **{sel['company']}** | {sel['job_title']}")
                                
                                st.write("---")
                                st.write("### What do you want to do?")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    if st.button("✅ Keep NEW Selection (Reject old)", key="keep_new_sel"):
                                        choice = 'current'
                                
                                with col2:
                                    if st.button("✅ Keep OLD Selection (Reject new)", key="keep_old_sel"):
                                        choice = 'existing'
                                
                                if choice == 'proceed':
                                    st.info("👆 Select an option above to proceed")
                                    st.stop()
                        
                        if choice != 'proceed' or result_status != "Selected" or not existing_selections:
                            try:
                                client = get_google_sheets_client()
                                if client:
                                    sheet = client.open_by_key(SHEET_ID).worksheet("Interview_Records")
                                    all_data = sheet.get_all_values()
                                    if not all_data:
                                        st.error("Interview_Records sheet is empty.")
                                        return
                                    headers = all_data[0]
                                    
                                    row_to_update = None
                                    for idx, row in enumerate(all_data[1:], start=2):
                                        if row[0] == record_id:
                                            row_to_update = idx
                                            break
                                    
                                    if row_to_update:
                                        updates = []
                                        
                                        status_col = headers.index('Interview Status') + 1 if 'Interview Status' in headers else 9
                                        updates.append({
                                            'range': f"{chr(64 + status_col)}{row_to_update}",
                                            'values': [[interview_status]]
                                        })
                                        
                                        result_col = headers.index('Result Status') + 1 if 'Result Status' in headers else 13
                                        updates.append({
                                            'range': f"{chr(64 + result_col)}{row_to_update}",
                                            'values': [[result_status]]
                                        })
                                        
                                        if salary_offered is not None:
                                            salary_col = headers.index('Salary Offered') + 1 if 'Salary Offered' in headers else 14
                                            updates.append({
                                                'range': f"{chr(64 + salary_col)}{row_to_update}",
                                                'values': [[salary_offered]]
                                            })
                                        
                                        if joining_date is not None:
                                            joining_col = headers.index('Joining Date') + 1 if 'Joining Date' in headers else 15
                                            updates.append({
                                                'range': f"{chr(64 + joining_col)}{row_to_update}",
                                                'values': [[joining_date.strftime('%Y-%m-%d')]]
                                            })
                                        
                                        remarks_col = headers.index('Remarks') + 1 if 'Remarks' in headers else 16
                                        existing_remarks = interview_data.get('Remarks', '')
                                        new_remarks = f"{existing_remarks}\n\n[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}] {feedback}"
                                        updates.append({
                                            'range': f"{chr(64 + remarks_col)}{row_to_update}",
                                            'values': [[new_remarks]]
                                        })
                                        
                                        updated_col = headers.index('Last Updated') + 1 if 'Last Updated' in headers else 17
                                        updates.append({
                                            'range': f"{chr(64 + updated_col)}{row_to_update}",
                                            'values': [[pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]]
                                        })
                                        
                                        updated_by_col = headers.index('Updated By') + 1 if 'Updated By' in headers else 18
                                        updates.append({
                                            'range': f"{chr(64 + updated_by_col)}{row_to_update}",
                                            'values': [[st.session_state.get('user', 'Admin')]]
                                        })
                                        
                                        sheet.batch_update(updates)
                                        
                                        if result_status == "Selected" and existing_selections:
                                            update_selection_status(record_id, choice, existing_selections)
                                        
                                        st.success("✅ Result updated in Interview_Records!")
                                        
                                        logger.info("Starting status sync...")
                                        sync_result = sync_all_statuses(
                                            candidate_id=interview_data['Candidate ID'],
                                            company_id=interview_data['CID'],
                                            job_title=interview_data['Job Title'],
                                            interview_status=interview_status,
                                            result_status=result_status
                                        )
                                        
                                        if sync_result:
                                            st.success("✅ Candidate status and vacancy status synced!")
                                        else:
                                            st.warning("⚠️ Result updated but some sync issues occurred")
                                        
                                        if result_status == "Selected":
                                            st.balloons()
                                        
                                        st.cache_data.clear()
                                        
                                        import time
                                        time.sleep(2)
                                        st.rerun()
                                    else:
                                        st.error("❌ Record not found")
                                else:
                                    st.error("❌ Could not connect to Google Sheets")
                            except Exception as e:
                                logger.error(f"Error updating result: {str(e)}")
                                st.error(f"❌ Error updating result: {str(e)}")
            else:
                st.info("✅ No interviews to update")
        else:
            st.info("No interview records found")    
    
    # ========== TAB 4: ALL INTERVIEWS ==========
    with tab4:
        st.markdown("### 📋 All Interview Records")
        
        interviews_df = get_interviews()
        
        if len(interviews_df) > 0:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'Interview Status' in interviews_df.columns:
                    status_filter = st.multiselect(
                        "Filter by Interview Status",
                        options=interviews_df['Interview Status'].unique().tolist(),
                        default=interviews_df['Interview Status'].unique().tolist()
                    )
                else:
                    status_filter = []
            
            with col2:
                if 'Result Status' in interviews_df.columns:
                    result_filter = st.multiselect(
                        "Filter by Result Status",
                        options=interviews_df['Result Status'].unique().tolist(),
                        default=interviews_df['Result Status'].unique().tolist()
                    )
                else:
                    result_filter = []
            
            with col3:
                search_text = st.text_input("Search (Name/Company)")
            
            filtered_df = interviews_df.copy()
            
            if status_filter and 'Interview Status' in interviews_df.columns:
                filtered_df = filtered_df[filtered_df['Interview Status'].isin(status_filter)]
            
            if result_filter and 'Result Status' in interviews_df.columns:
                filtered_df = filtered_df[filtered_df['Result Status'].isin(result_filter)]
            
            if search_text:
                if 'Full Name' in filtered_df.columns and 'Company Name' in filtered_df.columns:
                    filtered_df = filtered_df[
                        filtered_df['Full Name'].str.contains(search_text, case=False, na=False) |
                        filtered_df['Company Name'].str.contains(search_text, case=False, na=False)
                    ]
            
            st.write(f"**Showing {len(filtered_df)} / {len(interviews_df)} records**")
            st.dataframe(filtered_df, use_container_width=True, height=400)
            
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data (CSV)",
                data=csv,
                file_name=f"interviews_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No interview records found. Export some matches from Job Matching to get started!")


            
# ====================================================
# REPORTS & ANALYTICS (keep your existing implementation here)
# ====================================================
def admin_reports():
    st.subheader("📈 Reports & Analytics")
    st.markdown("---")
    
    # Create tabs for different report types
    tab1, tab2, tab3 = st.tabs([
        "📅 Today's Activity",
        "📊 Week/Month Summary", 
        "📈 Overall Statistics"
    ])
    
    # ========================================
    # TAB 1: TODAY'S ACTIVITY REPORT
    # ========================================
    with tab1:
        st.markdown("### 📅 Today's Activity Report")
        st.markdown(f"**Date:** {pd.Timestamp.now().strftime('%d %B %Y')}")
        st.markdown("---")
        
        # Get today's date in various formats for matching
        today = pd.Timestamp.now().date()
        today_str = today.strftime('%Y-%m-%d')
        
        # Fetch all data
        candidates_df = get_candidates()
        interviews_df = get_interviews()
        vacancies_df = get_vacancies()
        
        # Calculate today's stats
        col1, col2, col3, col4 = st.columns(4)
        
        # 1. New Candidates Registered Today
        with col1:
            if len(candidates_df) > 0 and 'Date Applied' in candidates_df.columns:
                candidates_df['Date Applied'] = pd.to_datetime(candidates_df['Date Applied'], errors='coerce')
                today_candidates = len(candidates_df[candidates_df['Date Applied'].dt.date == today])
            else:
                today_candidates = 0
            
            st.metric(
                label="👥 New Candidates",
                value=today_candidates,
                delta="Today"
            )
        
        # 2. Interviews Scheduled Today
        with col2:
            if len(interviews_df) > 0 and 'Interview Date' in interviews_df.columns:
                # Convert to datetime
                interviews_df['Interview Date'] = pd.to_datetime(interviews_df['Interview Date'], errors='coerce')
                today_interviews = len(interviews_df[
                    (interviews_df['Interview Date'].dt.date == today) &
                    (interviews_df['Interview Status'] == 'Interview Scheduled')
                ])
            else:
                today_interviews = 0
            
            st.metric(
                label="🗓️ Interviews Today",
                value=today_interviews,
                delta="Scheduled"
            )
        
        # 3. Candidates Selected Today
        with col3:
            if len(interviews_df) > 0 and 'Last Updated' in interviews_df.columns:
                interviews_df['Last Updated'] = pd.to_datetime(interviews_df['Last Updated'], errors='coerce')
                today_selected = len(interviews_df[
                    (interviews_df['Last Updated'].dt.date == today) &
                    (interviews_df['Result Status'] == 'Selected')
                ])
            else:
                today_selected = 0
            
            st.metric(
                label="🎉 Selected Today",
                value=today_selected,
                delta="Placements"
            )
        
        # 4. Vacancies Posted Today
        with col4:
            if len(vacancies_df) > 0 and 'Date Added' in vacancies_df.columns:
                vacancies_df['Date Added'] = pd.to_datetime(vacancies_df['Date Added'], errors='coerce')
                today_vacancies = len(vacancies_df[vacancies_df['Date Added'].dt.date == today])
            else:
                today_vacancies = 0
            
            st.metric(
                label="💼 New Vacancies",
                value=today_vacancies,
                delta="Posted"
            )
        
        st.markdown("---")
        
        # Detailed breakdown
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📋 Today's Interview Details")
            if len(interviews_df) > 0 and 'Interview Date' in interviews_df.columns:
                today_interview_details = interviews_df[
                    (interviews_df['Interview Date'].dt.date == today)
                ].copy()
                
                if len(today_interview_details) > 0:
                    display_cols = ['Full Name', 'Company Name', 'Job Title', 'Interview Time', 'Interview Status']
                    available_cols = [col for col in display_cols if col in today_interview_details.columns]
                    st.dataframe(today_interview_details[available_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("✅ No interviews scheduled for today")
            else:
                st.info("✅ No interviews scheduled for today")
        
        with col2:
            st.markdown("#### 🎯 Today's Selections")
            if len(interviews_df) > 0 and 'Last Updated' in interviews_df.columns:
                today_selections = interviews_df[
                    (interviews_df['Last Updated'].dt.date == today) &
                    (interviews_df['Result Status'] == 'Selected')
                ].copy()
                
                if len(today_selections) > 0:
                    display_cols = ['Full Name', 'Company Name', 'Job Title', 'Salary Offered']
                    available_cols = [col for col in display_cols if col in today_selections.columns]
                    st.dataframe(today_selections[available_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("No selections today yet")
            else:
                st.info("No selections today yet")
    
    # ========================================
    # TAB 2: WEEK/MONTH SUMMARY
    # ========================================
    with tab2:
        st.markdown("### 📊 Week/Month Summary")
        
        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            period = st.selectbox(
                "Select Period",
                ["This Week", "This Month", "Last 7 Days", "Last 30 Days", "Custom Range"]
            )
        
        # Calculate date range based on selection
        today = pd.Timestamp.now()
        
        if period == "This Week":
            start_date = today - pd.Timedelta(days=today.weekday())
            end_date = today
            period_label = "This Week"
        elif period == "This Month":
            start_date = today.replace(day=1)
            end_date = today
            period_label = "This Month"
        elif period == "Last 7 Days":
            start_date = today - pd.Timedelta(days=7)
            end_date = today
            period_label = "Last 7 Days"
        elif period == "Last 30 Days":
            start_date = today - pd.Timedelta(days=30)
            end_date = today
            period_label = "Last 30 Days"
        else:  # Custom Range
            with col2:
                date_range = st.date_input(
                    "Select Date Range",
                    value=(today - pd.Timedelta(days=30), today),
                    max_value=today.date()
                )
                if len(date_range) == 2:
                    start_date = pd.Timestamp(date_range[0])
                    end_date = pd.Timestamp(date_range[1])
                    period_label = f"{start_date.strftime('%d %b')} - {end_date.strftime('%d %b %Y')}"
                else:
                    start_date = today - pd.Timedelta(days=30)
                    end_date = today
                    period_label = "Last 30 Days"
        
        st.markdown(f"**Period:** {period_label}")
        st.markdown("---")
        
        # Fetch data
        candidates_df = get_candidates()
        interviews_df = get_interviews()
        vacancies_df = get_vacancies()
        
        # Metrics for the period
        col1, col2, col3, col4 = st.columns(4)
        
        # 1. Candidate Registrations
        with col1:
            if len(candidates_df) > 0 and 'Date Applied' in candidates_df.columns:
                candidates_df['Date Applied'] = pd.to_datetime(candidates_df['Date Applied'], errors='coerce')
                period_candidates = len(candidates_df[
                    (candidates_df['Date Applied'] >= start_date) &
                    (candidates_df['Date Applied'] <= end_date)
                ])
            else:
                period_candidates = 0
            
            st.metric(
                label="👥 Registrations",
                value=period_candidates,
                delta=period_label
            )
        
        # 2. Interviews Conducted
        with col2:
            if len(interviews_df) > 0 and 'Interview Date' in interviews_df.columns:
                interviews_df['Interview Date'] = pd.to_datetime(interviews_df['Interview Date'], errors='coerce')
                period_interviews = len(interviews_df[
                    (interviews_df['Interview Date'] >= start_date) &
                    (interviews_df['Interview Date'] <= end_date) &
                    (interviews_df['Interview Status'].isin(['Interview Completed', 'Interview Scheduled']))
                ])
            else:
                period_interviews = 0
            
            st.metric(
                label="🗓️ Interviews",
                value=period_interviews,
                delta=period_label
            )
        
        # 3. Placements/Selections
        with col3:
            if len(interviews_df) > 0 and 'Last Updated' in interviews_df.columns:
                interviews_df['Last Updated'] = pd.to_datetime(interviews_df['Last Updated'], errors='coerce')
                period_placements = len(interviews_df[
                    (interviews_df['Last Updated'] >= start_date) &
                    (interviews_df['Last Updated'] <= end_date) &
                    (interviews_df['Result Status'] == 'Selected')
                ])
            else:
                period_placements = 0
            
            st.metric(
                label="🎉 Placements",
                value=period_placements,
                delta=period_label
            )
        
        # 4. Vacancies Posted
        with col4:
            if len(vacancies_df) > 0 and 'Date Added' in vacancies_df.columns:
                vacancies_df['Date Added'] = pd.to_datetime(vacancies_df['Date Added'], errors='coerce')
                period_vacancies = len(vacancies_df[
                    (vacancies_df['Date Added'] >= start_date) &
                    (vacancies_df['Date Added'] <= end_date)
                ])
            else:
                period_vacancies = 0
            
            st.metric(
                label="💼 Vacancies",
                value=period_vacancies,
                delta=period_label
            )
        
        st.markdown("---")
        
        # Conversion metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if period_candidates > 0:
                interview_rate = (period_interviews / period_candidates * 100)
                st.metric(
                    label="📊 Interview Conversion",
                    value=f"{interview_rate:.1f}%",
                    help="Percentage of candidates who got interviews"
                )
            else:
                st.metric(label="📊 Interview Conversion", value="N/A")
        
        with col2:
            if period_interviews > 0:
                selection_rate = (period_placements / period_interviews * 100)
                st.metric(
                    label="🎯 Selection Rate",
                    value=f"{selection_rate:.1f}%",
                    help="Percentage of interviews resulting in selection"
                )
            else:
                st.metric(label="🎯 Selection Rate", value="N/A")
        
        with col3:
            if period_candidates > 0:
                placement_rate = (period_placements / period_candidates * 100)
                st.metric(
                    label="✅ Overall Placement",
                    value=f"{placement_rate:.1f}%",
                    help="Percentage of candidates placed"
                )
            else:
                st.metric(label="✅ Overall Placement", value="N/A")
        
        st.markdown("---")
        
        # Daily trend chart
        st.markdown("#### 📈 Daily Activity Trend")
        
        if len(candidates_df) > 0 or len(interviews_df) > 0:
            # Create date range
            date_range_list = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Count registrations per day
            daily_data = []
            for date in date_range_list:
                date_only = date.date()
                
                cand_count = 0
                if len(candidates_df) > 0 and 'Date Applied' in candidates_df.columns:
                    cand_count = len(candidates_df[candidates_df['Date Applied'].dt.date == date_only])
                
                int_count = 0
                if len(interviews_df) > 0 and 'Interview Date' in interviews_df.columns:
                    int_count = len(interviews_df[interviews_df['Interview Date'].dt.date == date_only])
                
                sel_count = 0
                if len(interviews_df) > 0 and 'Last Updated' in interviews_df.columns:
                    sel_count = len(interviews_df[
                        (interviews_df['Last Updated'].dt.date == date_only) &
                        (interviews_df['Result Status'] == 'Selected')
                    ])
                
                daily_data.append({
                    'Date': date_only,
                    'Candidates': cand_count,
                    'Interviews': int_count,
                    'Selections': sel_count
                })
            
            trend_df = pd.DataFrame(daily_data)
            trend_df = trend_df.set_index('Date')
            
            st.line_chart(trend_df)
        else:
            st.info("No data available for trend chart")
        
        st.markdown("---")
        
        # Download report
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.markdown("#### 📥 Download Report")
        
        with col2:
            # Prepare summary data
            summary_data = {
                'Metric': [
                    'Period', 'Candidate Registrations', 'Interviews Conducted',
                    'Placements', 'Vacancies Posted', 'Interview Conversion Rate',
                    'Selection Rate', 'Overall Placement Rate'
                ],
                'Value': [
                    period_label,
                    period_candidates,
                    period_interviews,
                    period_placements,
                    period_vacancies,
                    f"{interview_rate:.1f}%" if period_candidates > 0 else "N/A",
                    f"{selection_rate:.1f}%" if period_interviews > 0 else "N/A",
                    f"{placement_rate:.1f}%" if period_candidates > 0 else "N/A"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            
            csv = summary_df.to_csv(index=False)
            st.download_button(
                label="📊 Summary CSV",
                data=csv,
                file_name=f"summary_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            if len(trend_df) > 0:
                trend_csv = trend_df.to_csv()
                st.download_button(
                    label="📈 Trend CSV",
                    data=trend_csv,
                    file_name=f"trend_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
    
    # ========================================
    # TAB 3: OVERALL STATISTICS (EXISTING CODE)
    # ========================================
    with tab3:
        st.markdown("### 📈 Overall Statistics")
        
        # Existing code from original admin_reports() function
        interviews_df = get_interviews()
        companies_df = get_companies()
        vacancies_df = get_vacancies()
        candidates_df = get_candidates()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Companies", len(companies_df))
        col2.metric("Vacancies", len(vacancies_df))
        col3.metric("Candidates", len(candidates_df))
        col4.metric("Interviews", len(interviews_df))

        st.write("---")
        if len(interviews_df) > 0 and 'Interview Status' in interviews_df.columns:
            col1, col2 = st.columns(2)
            with col1:
                st.write("### Interview Status Distribution")
                status_dist = interviews_df['Interview Status'].value_counts()
                st.bar_chart(status_dist)
            with col2:
                st.write("### Summary")
                selected = len(interviews_df[interviews_df['Result Status'] == 'Selected']) if 'Result Status' in interviews_df.columns else 0
                total = len(interviews_df)
                rate = (selected / total * 100) if total > 0 else 0
                st.metric("Selection Rate", f"{rate:.1f}%")
                
                st.write("### Download Reports")
                csv = interviews_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Full Report (CSV)",
                    data=csv,
                    file_name="placement_report.csv",
                    mime="text/csv"
                )

# ====================================================
# COMPANY PORTAL
# ====================================================
def company_tab():
    st.title("🏢 Company Portal")
    company_menu = st.sidebar.radio(
        "Company Menu",
        ["➕ New Company Registration", "💼 Post Vacancy", "View Vacancies", "View Applications"],
    )

    if company_menu == "➕ New Company Registration":
        st.subheader("Register New Company")
        with st.form("company_reg_form"):
            cid = st.text_input("Company ID")
            name = st.text_input("Company Name")
            industry = st.selectbox(
                "Industry", ["IT", "Finance", "Healthcare", "Education", "Manufacturing", "Other"]
            )
            contact = st.text_input("Contact Email")
            location = st.text_input("Location")
            submitted = st.form_submit_button("✅ Register Company")
        if submitted:
            data = {
                "CID": cid,
                "Company_Name": name,
                "Industry": industry,
                "Contact": contact,
                "Location": location,
            }
            if add_to_sheet("CID", data):
                st.success("✅ Company registered successfully!")

    elif company_menu == "💼 Post Vacancy":
        st.subheader("Post New Vacancy")
        with st.form("post_vacancy_form"):
            job_title = st.text_input("Job Title")
            salary = st.number_input("Salary", min_value=0)
            location = st.text_input("Location")
            skills = st.text_input("Skills (comma separated)")
            experience = st.text_input("Experience Required")
            education = st.selectbox(
                "Education Required", ["12th", "B.Tech", "B.Sc", "M.Tech", "M.Sc", "MBA"]
            )
            v_submitted = st.form_submit_button("📤 Post Vacancy")
        if v_submitted:
            data = {
                "Job_Title": job_title,
                "Salary": salary,
                "Location": location,
                "Skills": skills,
                "Experience": experience,
                "Education": education,
            }
            if add_to_sheet("Sheet4", data):
                st.success("✅ Vacancy posted successfully!")

    elif company_menu == "View Vacancies":
        st.subheader("Your Posted Vacancies")
        vacancies_df = get_vacancies()
        if len(vacancies_df) > 0:
            st.dataframe(vacancies_df, use_container_width=True)
        else:
            st.info("No vacancies posted yet")

    elif company_menu == "View Applications":
        st.subheader("Applications to Your Vacancies")
        candidates_df = get_candidates()
        if len(candidates_df) > 0:
            st.dataframe(candidates_df, use_container_width=True, height=400)
        else:
            st.info("No applications yet")


# ====================================================
# CANDIDATE PORTAL (INCLUDES FULL WIZARD)
# ====================================================
def candidate_tab():
    st.title("👥 Candidate Portal")
    candidate_menu = st.sidebar.radio(
        "Candidate Menu",
        [
            "➕ Quick Registration",
            "📝 Full Application Form",
            "💼 Apply for Job",
            "📋 My Applications",
            "🏢 View Company Info",
        ],
    )

    # QUICK REGISTRATION
    if candidate_menu == "➕ Quick Registration":
        st.subheader("Quick Candidate Registration")
        st.info("⚡ Fast registration with basic information")

        with st.form("candidate_reg_form"):
            col1, col2 = st.columns(2)
            with col1:
                cand_id = st.text_input("Candidate ID")
                name = st.text_input("Full Name")
                email = st.text_input("Email")
                phone = st.text_input("Phone")
                gender = st.radio("Gender", ["Male", "Female", "Other"])
            with col2:
                address = st.text_input("Address")
                education = st.selectbox(
                    "Education", ["12th", "B.Tech", "B.Sc", "M.Tech", "M.Sc", "MBA"]
                )
                skills = st.text_input("Skills (comma separated)")
                salary_exp = st.number_input("Expected Salary", min_value=0)
                experience = st.text_input("Experience (years)")
            location_pref = st.text_input("Location Preference")
            reg_submitted = st.form_submit_button("✅ Register")

        if reg_submitted:
            data = {
                "Candidate ID": cand_id,
                "Name": name,
                "Email": email,
                "Mobile": phone,
                "Address": address,
                "Education": education,
                "Gender": gender,
                "Skills": skills,
                "Expected Salary": salary_exp,
                "Experience": experience,
                "Preferred Location": location_pref,
            }
            if add_to_sheet("Candidates", data):
                st.success("✅ Registration successful!")
                st.balloons()

    # FULL WIZARD (INTERNAL)
    elif candidate_menu == "📝 Full Application Form":
        st.subheader("Full Candidate Application (7-Step Wizard)")
        st.info("Use this detailed form for complete candidate profile.")
        
        # Ensure wizard stays persistent
        if "candidate_portal_wizard" not in st.session_state:
            st.session_state["candidate_portal_wizard"] = True
        
        # Wizard UI is fully handled inside candidate_wizard_module
        render_wizard()

    # APPLY FOR JOB
    elif candidate_menu == "💼 Apply for Job":
        st.subheader("Browse & Apply for Jobs")
        vacancies_df = get_vacancies()
        if len(vacancies_df) > 0:
            # Handle both Job_Title / Job Title
            job_col = (
                "Job_Title"
                if "Job_Title" in vacancies_df.columns
                else ("Job Title" if "Job Title" in vacancies_df.columns else None)
            )
            if job_col:
                selected_job = st.selectbox(
                    "Select Job to Apply", vacancies_df[job_col].tolist()
                )
                job_details = vacancies_df[vacancies_df[job_col] == selected_job].iloc[0]
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Job Title:** {job_details.get(job_col, 'N/A')}")
                    st.write(f"**Salary:** {job_details.get('Salary', 'N/A')}")
                    st.write(
                        f"**Location:** {job_details.get('Job Location/City', job_details.get('Location', 'N/A'))}"
                    )
                with col2:
                    st.write(
                        f"**Skills:** {job_details.get('Skills Required', job_details.get('Skills', 'N/A'))}"
                    )
                    st.write(
                        f"**Experience:** {job_details.get('Experience Required', job_details.get('Experience', 'N/A'))}"
                    )
                    st.write(
                        f"**Education:** {job_details.get('Education Required', job_details.get('Education', 'N/A'))}"
                    )
                if st.button("✅ Apply Now"):
                    st.success(
                        "✅ Application submitted successfully! (Application workflow to be integrated)"
                    )
            else:
                st.info("Vacancies sheet is missing Job Title column.")
        else:
            st.info("No vacancies available")

    # MY APPLICATIONS
    elif candidate_menu == "📋 My Applications":
        st.subheader("My Applications")
        interviews_df = get_interviews()
        if len(interviews_df) > 0:
            st.dataframe(interviews_df, use_container_width=True)
        else:
            st.info("No applications yet")

    # COMPANY INFO
    elif candidate_menu == "🏢 View Company Info":
        st.subheader("Company Information")
        companies_df = get_companies()
        if len(companies_df) > 0:
            if "Company_Name" in companies_df.columns and "Location" in companies_df.columns:
                limited_companies = companies_df[["Company_Name", "Industry", "Location"]]
                st.dataframe(limited_companies, use_container_width=True)
            else:
                st.dataframe(companies_df, use_container_width=True)
        else:
            st.info("No companies found")


# ====================================================
# MAIN APP - FIXED (Dashboard Header Only Once)
# ====================================================
def main():
    # Candidates sheet ke columns verify/add
    try:
        verify_sheet_columns()
    except Exception:
        pass

    # Agar login nahi hai → login screen show karo
    if not st.session_state.get("logged_in", False):
        render_login()
        return

    # Sidebar: user info
    st.sidebar.title("Placement Agency System")
    st.sidebar.markdown("---")
    if st.session_state.get("full_name"):
        st.sidebar.success(f"✅ {st.session_state.full_name}")
    if st.session_state.get("role"):
        st.sidebar.info(f"👤 Role: {st.session_state.role}")
    if st.session_state.get("email"):
        st.sidebar.info(f"📧 {st.session_state.email}")
    st.sidebar.markdown("---")

    # Role ke hisab se top-level menu
    role = (st.session_state.get("role") or "").lower()

    if role == "admin":
        main_choice = st.sidebar.radio(
            "Main Menu",
            ["🧭 Admin Panel", "👥 User Management", "🔒 Change Password"],
        )
    else:
        main_choice = st.sidebar.radio(
            "Main Menu",
            ["Portal", "🔒 Change Password"],
        )

    # Logout button
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        logout()
        return

    # Content routing
    if role == "admin":
        if main_choice == "🧭 Admin Panel":
            # ===== ADMIN DASHBOARD HEADER (Only when Admin Panel selected) =====
            #st.markdown("""
            #<style>
            #.header-container { padding: 20px; background: linear-gradient(90deg, #2ca02c, #1a7a1a); border-radius: 10px; color: white; margin-bottom: 30px; }
            #.header-title { font-size: 2.5em; font-weight: bold; margin: 0; }
            #</style>
            #""", unsafe_allow_html=True)
            
            #col1, col2, col3 = st.columns([1, 2, 1])
            #with col2:
            #  st.markdown('<div class="header-container"><div class="header-title">👨‍💼 Admin Dashboard</div></div>', unsafe_allow_html=True)
            
            #st.markdown("---")
            
            # NOW call admin_tab
            admin_tab()
        elif main_choice == "👥 User Management":
            render_user_management()
        elif main_choice == "🔒 Change Password":
            render_change_password()
    else:
        if main_choice == "Portal":
            if role == "company":
                company_tab()
            elif role == "candidate":
                candidate_tab()
            else:
                st.error("❌ Unknown role configured for this user.")
        elif main_choice == "🔒 Change Password":
            render_change_password()


# ====================================================
# ENTRY POINT
# ====================================================
if __name__ == "__main__":
    main()
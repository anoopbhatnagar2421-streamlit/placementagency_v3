from asyncio.log import logger
import streamlit as st
import pandas as pd
from datetime import datetime, date
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =======================================================
# GOOGLE SHEETS CONFIG
# =======================================================
SHEET_ID = "1rpuXdpfwjy0BQcaZcn0Acbh-Se6L3PvyNGiNu4NLcPA"
CRED_FILE = "credentials.json"

@st.cache_resource
def get_google_sheets_client():
    logger.debug("Initializing Google Sheets client in candidate_wizard_module.")
    try:
        logger.debug(f"Using credentials file: {CRED_FILE}")
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_FILE, scope)
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Error initializing Google Sheets client in candidate_wizard_module: {e}")
        return None

# =======================================================
# HELPER FUNCTIONS FOR G-SHEETS
# =======================================================
def get_job_titles():
    try:
        logger.debug("Fetching job titles from Sheet2.")
        client = get_google_sheets_client()
        if client:
            logger.debug("Google Sheets client obtained for job titles.")
            sheet = client.open_by_key(SHEET_ID).worksheet("Sheet2")
            df = pd.DataFrame(sheet.get_all_records())
            if "Designation" in df.columns:
                logger.debug("Job titles fetched successfully.")
                return sorted(df["Designation"].dropna().unique().tolist())
    except Exception as e:
        logger.error(f"Error fetching job titles: {e}")
    return []

def generate_candidate_id():
    #logger
    today_prefix = f"CND{datetime.now().strftime('%Y%m%d')}"
    try:
        logger.debug("Generating new candidate ID.")
        client = get_google_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet("Candidates")
        df = pd.DataFrame(sheet.get_all_records())

        if len(df) == 0 or "Candidate ID" not in df.columns:
            logger.debug("No existing candidates found or 'Candidate ID' column missing.")
            return f"{today_prefix}0001"

        ids_today = [int(str(CND)[-4:]) for CND in df["Candidate ID"]if str(CND).startswith(today_prefix)]
        next_num = max(ids_today) + 1 if ids_today else 1
        return f"{today_prefix}{next_num:04d}"
    except Exception as e:
        logger.error(f"Error generating candidate ID: {e}")
        return f"{today_prefix}0001"

def save_candidate_data(data):
    logger.debug("Saving candidate data to Google Sheets.")
    """Save candidate data to Google Sheets"""
    try:
        logger.debug(f"Saving candidate data: {data}")
        client = get_google_sheets_client()
        if not client:
            logger.error("Google Sheets client not available.")
            st.error("Google Sheets client not available.")
            return False
            
        sheet = client.open_by_key(SHEET_ID).worksheet("Candidates")
        logger.debug("Opened 'Candidates' worksheet.")
        
        # Get existing headers
        headers = sheet.row_values(1)
        logger.debug(f"Existing headers: {headers}")
        
        # Create row by matching data keys with headers
        row = [str(data.get(h, "")) if data.get(h) is not None else "" for h in headers]
        logger.debug(f"Prepared row for insertion: {row}")
        
        # Append the row
        sheet.append_row(row)
        logger.debug("Candidate data appended successfully.")
        
        # ❌ REMOVE THIS LINE if present:
        # st.cache_data.clear()
        
        return True
    except Exception as e:
        logger.error(f"Error saving candidate data: {e}")
        st.error(f"Error saving data: {str(e)}")
        return False
        logger
# =======================================================
# SESSION INITIALIZER
# =======================================================
def init_wizard_state():
    logger
    if "current_step" not in st.session_state:
        logger
        st.session_state.current_step = 1
    if "form_data" not in st.session_state:
        logger.debug("Initializing form_data in session state.")
        st.session_state.form_data = {}
    if "candidate_id" not in st.session_state:
        logger.debug("Generating new candidate_id in session state.")
        st.session_state.candidate_id = generate_candidate_id()
        logger
# =======================================================
# HELPER: Save field value to session state
# =======================================================
def save_field(field_name, value):
    logger.debug(f"Saving field: {field_name} with value: {value}")
    """Save field value to form_data safely"""
    if value is not None:
        logger.debug(f"Saving field: {field_name} with value: {value}")
        st.session_state.form_data[field_name] = value
    return value
    logger.debug(f"Field saved: {field_name} with value: {value}")

# =======================================================
# HELPER: Get field value from session state
# =======================================================
def get_field(field_name, default=""):
    logger.debug(f"Getting field: {field_name} with default: {default}")
    """Get field value from form_data safely (stable for reruns)"""
    d = st.session_state.form_data
    if field_name not in d:
        logger.debug(f"Field {field_name} not found in form_data. Returning default: {default}")
        return default
    val = d.get(field_name)
    if val is None:
        logger.debug(f"Field {field_name} is None. Returning default: {default}")
        return default
    # Har text field ke liye string hi return kro
    if isinstance(val, (int, float)):
        logger.debug(f"Field {field_name} is a number. Converting to string.")
        return str(val)
        logger.debug(f"Field {field_name} found with value: {val}")
    return val

# =======================================================
# STEP VALIDATION
# =======================================================
def validate_current_step():
    """Validate required fields for current step"""
    step = st.session_state.current_step
    d = st.session_state.form_data

    required = {
        1: ["full_name", "father_name", "dob", "gender"],
        2: ["mobile", "email", "current_address1", "current_city",
            "current_district", "current_state", "current_pin"],  
        3: ["job_pref1", "job_pref2", "job_pref3",
            "expected_salary", "notice_period", "relocate"],
        4: ["board_10th", "year_10th", "percentage_10th",
            "board_12th", "stream_12th", "year_12th", "percentage_12th"],  
        5: ["computer_skills", "hindi_level", "english_level"],
        6: ["disability", "own_vehicle", "driving_license"],
        7: ["declaration"],
    }

    if step not in required:
        logger.debug(f"Step {step} not in required fields. Skipping validation.")
        return True
    
    missing_fields = []
    logger.debug(f"Validating step {step} with required fields: {required.get(step, [])}")
    for field in required[step]:
        logger.debug(f"Validating field: {field} with value: {d.get(field)}")
        value = d.get(field)
        
        # Special handling for different types
        if field == "declaration":
            logger.debug(f"Validating declaration field with value: {value}")
            # Declaration must be True
            if not value:  # ✅ Simple check
                logger.debug("Declaration field is not True.")
                missing_fields.append(field)
        elif isinstance(value, str):
            logger.debug(f"Validating string field {field} with value: {value}")
            if not value or value.strip() == "":
                logger.debug(f"String field {field} is empty.")
                missing_fields.append(field)
        elif value is None or value == "":

            missing_fields.append(field)
        elif isinstance(value, (int, float)):
            logger.debug(f"Validating numeric field {field} with value: {value}")
            pass  # Numbers are valid
        elif isinstance(value, (date, datetime, pd.Timestamp)):
            
            pass  # Dates are valid
    
    if missing_fields:
        logger.debug(f"Missing required fields: {missing_fields}")
        field_names = {
            "full_name": "Full Name",
            "father_name": "Father's Name",
            "dob": "Date of Birth",
            "gender": "Gender",
            "mobile": "Mobile Number",
            "email": "Email",
            "current_address1": "Address Line 1",
            "current_city": "City",
            "current_district": "District",
            "current_state": "State",
            "current_pin": "PIN Code",
            "job_pref1": "Job Preference 1",
            "job_pref2": "Job Preference 2",
            "job_pref3": "Job Preference 3",
            "expected_salary": "Expected Salary",
            "notice_period": "Notice Period",
            "relocate": "Willing to Relocate",
            "board_10th": "10th Board",
            "year_10th": "10th Year",
            "percentage_10th": "10th Percentage",
            "board_12th": "12th Board",
            "stream_12th": "12th Stream",
            "year_12th": "12th Year",
            "percentage_12th": "12th Percentage",
            "grad_degree": "Degree Name",
            "grad_university": "University",
            "grad_specialization": "Specialization",
            "grad_year": "Graduation Year",
            "grad_percentage": "Graduation Percentage ",
            "computer_skills": "Computer Skills",
            "hindi_level": "Hindi Level",
            "english_level": "English Level",
            "disability": "Disability",
            "own_vehicle": "Vehicle",
            "driving_license": "Driving License",
            "declaration": "Declaration",
        }
        display_names = [field_names.get(f, f) for f in missing_fields]
        st.error(f"❌ Please fill these required fields:\n\n" + "\n".join([f"  • {n}" for n in display_names]))
        return False
        logger

    return True
# =======================================================
# NAVIGATION HANDLERS
# =======================================================
def next_step():
    logger.debug("Attempting to go to the next step.")
    if validate_current_step():
        logger.debug("Current step validated successfully. Moving to next step.")
        st.session_state.current_step += 1
    

def prev_step():
    logger.debug("Attempting to go to the previous step.")
    st.session_state.current_step -= 1
    

def go_to_step(step):
    logger.debug(f"Going to step {step}.")
    st.session_state.current_step = step
    

# =======================================================
# STEP UI RENDERERS (FIXED)
# =======================================================

def render_step1():
    logger
    st.subheader("👤 Step 1: Personal Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        logger.debug("Rendering Full Name input field.")
        st.session_state.form_data["full_name"] = st.text_input(
            "Full Name *", 
            value=get_field("full_name"),
            key="full_name",
            placeholder="e.g., John Doe"
        )
        logger
        dob = st.date_input(
            "Date of Birth *",
            value=get_field("dob", datetime(2000, 1, 1)),
            key="dob"
        )
        st.session_state.form_data["dob"] = dob
        logger.debug("Rendering Date of Birth input field.")
        
        marital_options = ["Single", "Married", "Divorced", "Widowed"]
        current_marital = get_field("marital_status", "Single")
        marital_index = marital_options.index(current_marital) if current_marital in marital_options else 0
        marital = st.selectbox(
            "Marital Status",
            marital_options,
            index=marital_index,
            key="marital"
        )
        st.session_state.form_data["marital_status"] = marital
    
    with col2:
        logger.debug("Rendering Father's/Guardian's Name input field.")
        st.session_state.form_data["father_name"] = st.text_input(
            "Father's/Guardian's Name *",
            value=get_field("father_name"),
            key="father_name",
            placeholder="Full name"
        )
        
        gender_options = ["Male", "Female", "Other"]
        current_gender = get_field("gender", "Male")
        gender_index = gender_options.index(current_gender) if current_gender in gender_options else 0
        gender = st.selectbox(
            "Gender *",
            gender_options,
            index=gender_index,
            key="gender"
        )
        st.session_state.form_data["gender"] = gender
        
        category_options = ["General", "OBC", "SC", "ST", "Other"]
        current_category = get_field("category", "General")
        category_index = category_options.index(current_category) if current_category in category_options else 0
        st.session_state.form_data["category"] = st.selectbox(
            "Category",
            category_options,
            index=category_index,
            key="category"
        )


def render_step2():
    st.subheader("📍 Step 2: Contact & Address")
    
    st.markdown("#### Contact Information")
    col1, col2 = st.columns(2)
    
    with col1:
        mobile = st.text_input(
            "Mobile Number *",
            value=get_field("mobile"),
            max_chars=10,
            key="mobile",
            placeholder="10 digits"
        )
        st.session_state.form_data["mobile"] = mobile
        
        email = st.text_input(
            "Email Address *",
            value=get_field("email"),
            key="email",
            placeholder="your@email.com"
        )
        st.session_state.form_data["email"] = email
    
    with col2:
        alt_mobile = st.text_input(
            "Alternate Mobile",
            value=get_field("alternate_mobile"),
            max_chars=10,
            key="alt_mobile",
            placeholder="Optional"
        )
        st.session_state.form_data["alternate_mobile"] = alt_mobile
        
        whatsapp = st.text_input(
            "WhatsApp Number",
            value=get_field("whatsapp"),
            max_chars=10,
            key="whatsapp",
            placeholder="Optional"
        )
        st.session_state.form_data["whatsapp"] = whatsapp
    
    st.markdown("#### Identity Documents")
    col1, col2 = st.columns(2)
    
    with col1:
        aadhaar = st.text_input(
            "Aadhaar Number",
            value=get_field("aadhaar"),
            max_chars=12,
            key="aadhaar",
            placeholder="12 digits"
        )
        st.session_state.form_data["aadhaar"] = aadhaar
    
    with col2:
        pan = st.text_input(
            "PAN Number",
            value=get_field("pan"),
            max_chars=10,
            key="pan",
            placeholder="e.g., ABCDE1234F"
        )
        st.session_state.form_data["pan"] = pan
    
    st.markdown("#### Current Address")
    
    address1 = st.text_input(
        "Address Line 1 *",
        value=get_field("current_address1"),
        key="addr1",
        placeholder="Street address"
    )
    st.session_state.form_data["current_address1"] = address1
    
    col1, col2 = st.columns(2)
    
    with col1:
        city = st.text_input(
            "City *",
            value=get_field("current_city"),
            key="city",
            placeholder="e.g., Bangalore"
        )
        st.session_state.form_data["current_city"] = city
        
        state = st.text_input(
            "State *",
            value=get_field("current_state"),
            key="state",
            placeholder="e.g., Karnataka"
        )
        st.session_state.form_data["current_state"] = state
    
    with col2:
        district = st.text_input(
            "District *",
            value=get_field("current_district"),
            key="district",
            placeholder="e.g., Bangalore"
        )
        st.session_state.form_data["current_district"] = district
        
        pin_code = st.text_input(
            "PIN Code *",
            value=get_field("current_pin"),
            max_chars=6,
            key="pin",
            placeholder="6 digits"
        )
        st.session_state.form_data["current_pin"] = pin_code
    
    st.markdown("#### Permanent Address (If different from Current)")
    
    permanent_same = st.checkbox(
        "Same as Current Address",
        value=False,
        key="permanent_same"
    )
    
    if not permanent_same:
        perm_address1 = st.text_input(
            "Permanent Address Line 1",
            value=get_field("permanent_address1"),
            key="perm_addr1",
            placeholder="Street address"
        )
        st.session_state.form_data["permanent_address1"] = perm_address1
        
        col1, col2 = st.columns(2)
        
        with col1:
            perm_city = st.text_input(
                "City",
                value=get_field("permanent_city"),
                key="perm_city",
                placeholder="e.g., Bangalore"
            )
            st.session_state.form_data["permanent_city"] = perm_city
            
            perm_state = st.text_input(
                "State",
                value=get_field("permanent_state"),
                key="perm_state",
                placeholder="e.g., Karnataka"
            )
            st.session_state.form_data["permanent_state"] = perm_state
        
        with col2:
            perm_district = st.text_input(
                "District",
                value=get_field("permanent_district"),
                key="perm_district",
                placeholder="e.g., Bangalore"
            )
            st.session_state.form_data["permanent_district"] = perm_district
            
            perm_pin = st.text_input(
                "PIN Code",
                value=get_field("permanent_pin"),
                max_chars=6,
                key="perm_pin",
                placeholder="6 digits"
            )
            st.session_state.form_data["permanent_pin"] = perm_pin
    else:
        # Copy current to permanent if same
        st.session_state.form_data["permanent_address1"] = st.session_state.form_data.get("current_address1", "")
        st.session_state.form_data["permanent_city"] = st.session_state.form_data.get("current_city", "")
        st.session_state.form_data["permanent_district"] = st.session_state.form_data.get("current_district", "")
        st.session_state.form_data["permanent_state"] = st.session_state.form_data.get("current_state", "")
        st.session_state.form_data["permanent_pin"] = st.session_state.form_data.get("current_pin", "")
    
    # Debug info
    with st.expander("🔍 Debug Info (Step 2)", expanded=False):
        st.write("**Current PIN Value:**", st.session_state.form_data.get("current_pin", "NOT SET"))
        st.write("**All Step 2 Data:**", st.session_state.form_data)


def render_step3():
    st.subheader("💼 Step 3: Job Preferences")
    
    jobs = get_job_titles() or ["Inside Sales", "Back Office", "Field Sales", "Marketing"]
    
    current_job1 = get_field("job_pref1", jobs[0] if jobs else "")
    job1_index = jobs.index(current_job1) if current_job1 in jobs else 0
    job1 = st.selectbox(
        "Job Preference 1 (First Choice) *",
        jobs,
        index=job1_index if jobs else None,
        key="job1"
    )
    st.session_state.form_data["job_pref1"] = job1
    
    current_job2 = get_field("job_pref2", jobs[0] if jobs else "")
    job2_index = jobs.index(current_job2) if current_job2 in jobs else 0
    job2 = st.selectbox(
        "Job Preference 2 (Second Choice) *",
        jobs,
        index=job2_index if jobs else None,
        key="job2"
    )
    st.session_state.form_data["job_pref2"] = job2
    
    current_job3 = get_field("job_pref3", jobs[0] if jobs else "")
    job3_index = jobs.index(current_job3) if current_job3 in jobs else 0
    job3 = st.selectbox(
        "Job Preference 3 (Third Choice) *",
        jobs,
        index=job3_index if jobs else None,
        key="job3"
    )
    st.session_state.form_data["job_pref3"] = job3
    
    col1, col2 = st.columns(2)
    
    with col1:
        salary_val = int(get_field("expected_salary", 0))
        salary = st.number_input(
            "Expected Salary (per month) *",
            min_value=0,
            value=salary_val,
            step=1000,
            key="salary"
        )
        st.session_state.form_data["expected_salary"] = salary
    
    with col2:
        notice_options = ["Immediate", "15 Days", "1 Month", "2 Months"]
        current_notice = get_field("notice_period", "Immediate")
        notice_index = notice_options.index(current_notice) if current_notice in notice_options else 0
        notice = st.selectbox(
            "Notice Period *",
            notice_options,
            index=notice_index,
            key="notice"
        )
        st.session_state.form_data["notice_period"] = notice
    
    relocate_options = ["Yes", "No"]
    current_relocate = get_field("relocate", "Yes")
    relocate_index = relocate_options.index(current_relocate) if current_relocate in relocate_options else 0
    relocate = st.selectbox(
        "Willing to Relocate? *",
        relocate_options,
        index=relocate_index,
        key="relocate"
    )
    st.session_state.form_data["relocate"] = relocate
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.session_state.form_data["preferred_location"] = st.text_input(
            "Preferred Location",
            value=get_field("preferred_location"),
            key="pref_location",
            placeholder="e.g., Bangalore, Mumbai"
        )
    
    with col2:
        fresher_options = ["Yes", "No"]
        current_fresher = get_field("is_fresher", "Yes")
        fresher_index = fresher_options.index(current_fresher) if current_fresher in fresher_options else 0
        st.session_state.form_data["is_fresher"] = st.selectbox(
            "Are you a Fresher?",
            fresher_options,
            index=fresher_index,
            key="is_fresher"
        )


def render_step4():
    st.subheader("🎓 Step 4: Education")
    
    st.markdown("#### 10th Standard")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        board10 = st.text_input(
            "Board Name *", 
            value=get_field("board_10th"),
            key="board10"
        )
        st.session_state.form_data["board_10th"] = board10
    
    with col2:
        year10_val = int(get_field("year_10th", 2020))
        year10 = st.number_input(
            "Year of Passing *",
            min_value=1980,
            max_value=2030,
            value=year10_val,
            key="year10"
        )
        st.session_state.form_data["year_10th"] = year10
    
    with col3:
        perc10 = st.text_input(
            "Percentage *",
            value=get_field("percentage_10th"),
            key="perc10",
            placeholder="e.g., 85.5"
        )
        st.session_state.form_data["percentage_10th"] = perc10
    
    st.markdown("#### 12th Standard")
    col1, col2 = st.columns(2)
    
    with col1:
        board12 = st.text_input(
            "Board Name *",
            value=get_field("board_12th"),
            key="board12"
        )
        st.session_state.form_data["board_12th"] = board12
        
        year12_val = int(get_field("year_12th", 2022))
        year12 = st.number_input(
            "Year of Passing *",
            min_value=1980,
            max_value=2030,
            value=year12_val,
            key="year12"
        )
        st.session_state.form_data["year_12th"] = year12
    
    with col2:
        stream_options = ["Science", "Commerce", "Arts"]
        current_stream = get_field("stream_12th", "Science")
        stream_index = stream_options.index(current_stream) if current_stream in stream_options else 0
        stream = st.selectbox(
            "Stream *",
            stream_options,
            index=stream_index,
            key="stream12"
        )
        st.session_state.form_data["stream_12th"] = stream
        
        perc12 = st.text_input(
            "Percentage *",
            value=get_field("percentage_12th"),
            key="perc12",
            placeholder="e.g., 78.5"
        )
        st.session_state.form_data["percentage_12th"] = perc12
    
    st.markdown("#### Graduation")
    col1, col2 = st.columns(2)
    
    with col1:
        grad_degree = st.text_input(
            "Degree Name *",
            value=get_field("grad_degree"),
            key="grad_deg",
            placeholder="e.g., B.Tech, B.Com"
        )
        st.session_state.form_data["grad_degree"] = grad_degree
        
        grad_spec = st.text_input(
            "Specialization *",
            value=get_field("grad_specialization"),
            key="grad_spec",
            placeholder="e.g., Computer Science"
        )
        st.session_state.form_data["grad_specialization"] = grad_spec
    
    with col2:
        grad_uni = st.text_input(
            "University Name *",
            value=get_field("grad_university"),
            key="grad_uni",
            placeholder="e.g., Delhi University"
        )
        st.session_state.form_data["grad_university"] = grad_uni
        
        grad_year_val = int(get_field("grad_year", 2025))
        grad_year = st.number_input(
            "Year of Passing *",
            min_value=1980,
            max_value=2030,
            value=grad_year_val,
            key="grad_year"
        )
        st.session_state.form_data["grad_year"] = grad_year
    
    # ✅ CRITICAL FIX: Graduation Percentage proper mapping
    col1, col2 = st.columns(2)
    with col1:
        grad_perc = st.text_input(
            "Graduation Percentage/CGPA *",
            value=get_field("grad_percentage", ""),
            key="grad_perc_input",
            placeholder="e.g., 8.5 CGPA or 75%"
        )
        # ✅ Direct assignment - NO conditions
        st.session_state.form_data["grad_percentage"] = grad_perc
    
    # Debug section - temporary (remove in production)
    with st.expander("🔍 Debug - Graduation Data", expanded=False):
        st.write("**Current Graduation Percentage:**", st.session_state.form_data.get("grad_percentage", "NOT SET"))
        st.write("**Input Value:**", grad_perc)
        st.write("**All Graduation Fields:**", {
            "grad_degree": st.session_state.form_data.get("grad_degree"),
            "grad_university": st.session_state.form_data.get("grad_university"),
            "grad_specialization": st.session_state.form_data.get("grad_specialization"),
            "grad_year": st.session_state.form_data.get("grad_year"),
            "grad_percentage": st.session_state.form_data.get("grad_percentage"),
        })

def render_step5():
    st.subheader("🏆 Step 5: Skills & Experience")
    
    st.markdown("#### Skills")
    
    st.session_state.form_data["computer_skills"] = st.text_input(
        "Computer Skills *",
        value=st.session_state.form_data.get("computer_skills", ""),
        key="comp_skills",
        placeholder="e.g., MS Office, Python, Java"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.session_state.form_data["technical_skills"] = st.text_input(
            "Technical Skills",
            value=st.session_state.form_data.get("technical_skills", ""),
            key="tech_skills",
            placeholder="e.g., Programming languages, tools"
        )
        
        hindi_options = ["Basic", "Intermediate", "Fluent", "Native"]
        current_hindi = st.session_state.form_data.get("hindi_level", "Basic")
        hindi_index = hindi_options.index(current_hindi) if current_hindi in hindi_options else 0
        st.session_state.form_data["hindi_level"] = st.selectbox(
            "Hindi Proficiency *",
            hindi_options,
            index=hindi_index,
            key="hindi"
        )
    
    with col2:
        st.session_state.form_data["other_skills"] = st.text_input(
            "Other Skills",
            value=st.session_state.form_data.get("other_skills", ""),
            key="other_skills",
            placeholder="e.g., Soft skills, certifications"
        )
        
        english_options = ["Basic", "Intermediate", "Fluent", "Native"]
        current_english = st.session_state.form_data.get("english_level", "Basic")
        english_index = english_options.index(current_english) if current_english in english_options else 0
        st.session_state.form_data["english_level"] = st.selectbox(
            "English Proficiency *",
            english_options,
            index=english_index,
            key="english"
        )
    
    st.markdown("#### Experience")
    
    # Check if fresher
    is_fresher = st.session_state.form_data.get("is_fresher", "Yes")
    
    if is_fresher == "No":
        st.markdown("#### Experience Details")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            exp_years = st.number_input(
                "Years of Experience",
                min_value=0,
                max_value=60,
                value=int(get_field("exp_years", 0)),
                step=1,
                key="exp_years"
            )
            st.session_state.form_data["exp_years"] = exp_years
        
        with col2:
            exp_months = st.number_input(
                "Additional Months",
                min_value=0,
                max_value=11,
                value=int(get_field("exp_months", 0)),
                step=1,
                key="exp_months"
            )
            st.session_state.form_data["exp_months"] = exp_months
        
        with col3:
            current_ctc = st.text_input(
                "Current CTC (LPA)",
                value=get_field("current_ctc"),
                key="current_ctc",
                placeholder="e.g., 5.5"
            )
            st.session_state.form_data["current_ctc"] = current_ctc
    else:
        # Fresher - set experience to 0
        st.session_state.form_data["exp_years"] = 0
        st.session_state.form_data["exp_months"] = 0
        st.session_state.form_data["current_ctc"] = ""
        st.info("✅ You are a fresher - Experience fields will be set to 0")



def render_step6():
    st.subheader("📋 Step 6: Additional Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        disability_options = ["No", "Yes"]
        current_disability = get_field("disability", "No")
        disability_index = disability_options.index(current_disability) if current_disability in disability_options else 0
        st.session_state.form_data["disability"] = st.selectbox(
            "Do you have any physical disability? *",
            disability_options,
            index=disability_index,
            key="disability"
        )
        
        if st.session_state.form_data.get("disability") == "Yes":
            st.session_state.form_data["disability_details"] = st.text_area(
                "Please specify disability details",
                value=get_field("disability_details"),
                key="disability_details"
            )
        else:
            st.session_state.form_data["disability_details"] = ""
        
        vehicle_options = ["No", "Two-wheeler", "Four-wheeler", "Both"]
        current_vehicle = get_field("own_vehicle", "No")
        vehicle_index = vehicle_options.index(current_vehicle) if current_vehicle in vehicle_options else 0
        st.session_state.form_data["own_vehicle"] = st.selectbox(
            "Do you own a vehicle? *",
            vehicle_options,
            index=vehicle_index,
            key="vehicle"
        )
    
    with col2:
        license_options = ["No", "Yes"]
        current_license = get_field("driving_license", "No")
        license_index = license_options.index(current_license) if current_license in license_options else 0
        st.session_state.form_data["driving_license"] = st.selectbox(
            "Valid Driving License? *",
            license_options,
            index=license_index,
            key="license"
        )
    
    st.markdown("---")
    st.markdown("#### References (Optional)")
    st.info("ℹ️ Provide at least one professional or academic reference")
    
    st.markdown("**Reference 1**")
    col1, col2 = st.columns(2)
    
    with col1:
        ref1_name = st.text_input(
            "Name",
            value=get_field("ref1_name"),
            key="ref1_name_input",
            placeholder="Reference person's name"
        )
        st.session_state.form_data["ref1_name"] = ref1_name
        
        ref1_designation = st.text_input(
            "Designation",
            value=get_field("ref1_designation"),
            key="ref1_designation_input",
            placeholder="e.g., Manager, Professor"
        )
        st.session_state.form_data["ref1_designation"] = ref1_designation
    
    with col2:
        ref1_organization = st.text_input(
            "Organization/Institute",
            value=get_field("ref1_organization"),
            key="ref1_organization_input",
            placeholder="Company or University name"
        )
        st.session_state.form_data["ref1_organization"] = ref1_organization  # ✅ KEY FIX
        
        ref1_contact = st.text_input(
            "Contact Number",
            value=get_field("ref1_contact"),
            key="ref1_contact_input",
            placeholder="Phone or Email"
        )
        st.session_state.form_data["ref1_contact"] = ref1_contact
    
    st.markdown("**Reference 2 (Optional)**")
    col1, col2 = st.columns(2)
    
    with col1:
        ref2_name = st.text_input(
            "Name",
            value=get_field("ref2_name"),
            key="ref2_name_input",
            placeholder="Reference person's name"
        )
        st.session_state.form_data["ref2_name"] = ref2_name
    
    with col2:
        ref2_contact = st.text_input(
            "Contact Number",
            value=get_field("ref2_contact"),
            key="ref2_contact_input",
            placeholder="Phone or Email"
        )
        st.session_state.form_data["ref2_contact"] = ref2_contact  # ✅ KEY FIX
    
    # Debug section
    with st.expander("🔍 Debug - Reference Data", expanded=False):
        st.write("**Reference 1:**")
        st.json({
            "ref1_name": st.session_state.form_data.get("ref1_name", "NOT SET"),
            "ref1_designation": st.session_state.form_data.get("ref1_designation", "NOT SET"),
            "ref1_organization": st.session_state.form_data.get("ref1_organization", "NOT SET"),  # ✅ Check this
            "ref1_contact": st.session_state.form_data.get("ref1_contact", "NOT SET"),
        })
        st.write("**Reference 2:**")
        st.json({
            "ref2_name": st.session_state.form_data.get("ref2_name", "NOT SET"),
            "ref2_contact": st.session_state.form_data.get("ref2_contact", "NOT SET"),  # ✅ Check this
        })
def render_step7():
    st.subheader("✅ Step 7: Review & Submit")
    st.markdown("Please review your information before submitting")
    
    with st.expander("📋 View All Details", expanded=False):
        # Convert date objects to string for JSON serialization
        display_data = {}
        for key, value in st.session_state.form_data.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                display_data[key] = str(value)
            else:
                display_data[key] = value
        st.json(display_data)
    
    st.markdown("---")
    st.warning("⚠️ **Declaration**")
    
    # Get current declaration value
    current_declaration = st.session_state.form_data.get("declaration", False)
    
    # Create checkbox and directly store result
    st.session_state.form_data["declaration"] = st.checkbox(
        "I hereby declare that all the information provided above is true and correct to the best of my knowledge.",
        value=current_declaration,
        key="declaration_checkbox"
    )
    
    # Debug info
    with st.expander("🔍 Debug Info (Step 7)", expanded=False):
        st.write("**Declaration Value:**", st.session_state.form_data.get("declaration"))
        st.write("**Declaration Type:**", type(st.session_state.form_data.get("declaration")))
# =======================================================
# SUBMIT APPLICATION
# =======================================================
def submit_application():
    if not validate_current_step():
        return

    d = st.session_state.form_data
    
    # Helper function to convert dates to strings
    def to_string(value):
        if isinstance(value, (datetime, pd.Timestamp)):
            return str(value)
        elif isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        else:
            return str(value) if value is not None else ""

    data = {
        "Candidate ID": st.session_state.candidate_id,
        "Date Applied": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        "Full Name": to_string(d.get("full_name", "")),
        "Father Name": to_string(d.get("father_name", "")),
        "DOB": to_string(d.get("dob", "")),
        "Gender": to_string(d.get("gender", "")),
        "Marital Status": to_string(d.get("marital_status", "")),
        "Category": to_string(d.get("category", "")),

        "Aadhaar": to_string(d.get("aadhaar", "")),
        "PAN": to_string(d.get("pan", "")),

        "Mobile": to_string(d.get("mobile", "")),
        "Alt Mobile": to_string(d.get("alternate_mobile", "")),
        "Email": to_string(d.get("email", "")),
        "WhatsApp": to_string(d.get("whatsapp", "")),

        "Current Address": to_string(d.get("current_address1", "")),
        "Current City": to_string(d.get("current_city", "")),
        "Current District": to_string(d.get("current_district", "")),
        "Current State": to_string(d.get("current_state", "")),
        "Current PIN": to_string(d.get("current_pin", "")),

        "Permanent Address": to_string(d.get("permanent_address1", "")),
        "Permanent City": to_string(d.get("permanent_city", "")),
        "Permanent District": to_string(d.get("permanent_district", "")),
        "Permanent State": to_string(d.get("permanent_state", "")),
        "Permanent PIN": to_string(d.get("permanent_pin", "")),

        "Job Pref 1": to_string(d.get("job_pref1", "")),
        "Job Pref 2": to_string(d.get("job_pref2", "")),
        "Job Pref 3": to_string(d.get("job_pref3", "")),

        "Preferred Location": to_string(d.get("preferred_location", "")),

        "Expected Salary": to_string(d.get("expected_salary", "")),
        "Notice Period": to_string(d.get("notice_period", "")),
        "Willing to Relocate": to_string(d.get("relocate", "")),

        "10th Board": to_string(d.get("board_10th", "")),
        "10th Year": to_string(d.get("year_10th", "")),
        "10th Percentage": to_string(d.get("percentage_10th", "")),

        "12th Board": to_string(d.get("board_12th", "")),
        "12th Stream": to_string(d.get("stream_12th", "")),
        "12th Year": to_string(d.get("year_12th", "")),
        "12th Percentage": to_string(d.get("percentage_12th", "")),

        "Graduation Degree": to_string(d.get("grad_degree", "")),
        "Graduation University": to_string(d.get("grad_university", "")),
        "Graduation Specialization": to_string(d.get("grad_specialization", "")),
        "Graduation Year": to_string(d.get("grad_year", "")),
        "Graduation Percentage": to_string(d.get("grad_percentage", "")),

        "Computer Skills": to_string(d.get("computer_skills", "")),
        "Technical Skills": to_string(d.get("technical_skills", "")),
        "Other Skills": to_string(d.get("other_skills", "")),

        "Hindi Level": to_string(d.get("hindi_level", "")),
        "English Level": to_string(d.get("english_level", "")),

        "Is Fresher": to_string(d.get("is_fresher", "")),
        "Experience Years": to_string(d.get("exp_years", "")),
        "Experience Months": to_string(d.get("exp_months", "")),
        "Current CTC": to_string(d.get("current_ctc", "")),

        "Disability": to_string(d.get("disability", "")),
        "Disability Details": to_string(d.get("disability_details", "")),
        "Own Vehicle": to_string(d.get("own_vehicle", "")),
        "Driving License": to_string(d.get("driving_license", "")),

        "Reference 1 Name": to_string(d.get("ref1_name", "")),
        "Reference 1 Designation": to_string(d.get("ref1_designation", "")),
        "Reference 1 Organization": to_string(d.get("ref1_organization", "")),
        "Reference 1 Contact": to_string(d.get("ref1_contact", "")),

        "Reference 2 Name": to_string(d.get("ref2_name", "")),
        "Reference 2 Contact": to_string(d.get("ref2_contact", "")),

        "Status": "Applied"
    }

    success = save_candidate_data(data)

    if success:
        st.success("✅ Candidate Registered Successfully!")
        st.info(f"🎫 Candidate ID: **{st.session_state.candidate_id}**")
        st.balloons()
        
        # Reset form
        st.session_state.current_step = 1
        st.session_state.form_data = {}
        st.session_state.candidate_id = generate_candidate_id()
        st.rerun()
    else:
        st.error("❌ Error submitting data! Please check the debug information.")

# =======================================================
# MAIN RENDERER FOR MODULE (USED IN app.py)
# =======================================================
def render_wizard():
    # Wizard state sirf pehli baar initialize karo
    if "wizard_initialized" not in st.session_state:
        init_wizard_state()
        st.session_state["wizard_initialized"] = True

    # Sticky UI CSS
    st.markdown("""
    <style>
    .sticky-header {
        position: sticky;
        top: 0;
        background: white;
        z-index: 999;
        padding: 8px;
        border-bottom: 1px solid #ddd;
    }
    .sticky-footer {
        position: sticky;
        bottom: 0;
        background: white;
        z-index: 999;
        padding: 8px;
        border-top: 1px solid #ddd;
    }
    </style>
    """, unsafe_allow_html=True)

    # Sticky Header
    st.markdown('<div class="sticky-header">', unsafe_allow_html=True)
    step = st.session_state.current_step
    progress = step / 7
    st.progress(progress)
    st.markdown(f"**Step {step} of 7**")
    st.markdown('</div>', unsafe_allow_html=True)

    # Step rendering
    if step == 1:
        render_step1()
    elif step == 2:
        render_step2()
    elif step == 3:
        render_step3()
    elif step == 4:
        render_step4()
    elif step == 5:
        render_step5()
    elif step == 6:
        render_step6()
    elif step == 7:
        render_step7()

    # Sticky Footer Navigation
    st.markdown('<div class="sticky-footer">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if step > 1:
            st.button("← Previous", on_click=prev_step, use_container_width=True)

    with col2:
        st.markdown(f"<center>Step {step} of 7</center>", unsafe_allow_html=True)

    with col3:
        if step < 7:
            st.button("Next →", on_click=next_step, use_container_width=True)
        else:
            st.button(
                "Submit Application",
                on_click=submit_application,
                use_container_width=True,
            )

    st.markdown("</div>", unsafe_allow_html=True)

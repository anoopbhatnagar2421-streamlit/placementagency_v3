import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import hashlib
from datetime import datetime
import pandas as pd

# -------------------------------------------------------
# PAGE CONFIG (top-level)
# -------------------------------------------------------
#st.set_page_config(
   # page_title="Placement Agency - Login",
   # page_icon="🔐",
    #layout="centered",
    #initial_sidebar_state="collapsed"
#)

# =======================================================
# GOOGLE SHEETS CONFIG FOR LOGIN
# =======================================================
SHEET_ID = "1rpuXdpfwjy0BQcaZcn0Acbh-Se6L3PvyNGiNu4NLcPA"


@st.cache_resource
def get_google_sheets_client():
    """Cached Google Sheets client for login - using st.secrets"""
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Use st.secrets instead of credentials.json
        credentials = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=scope
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"Google Sheets connection error: {str(e)}")
        return None


# =======================================================
# USER AUTHENTICATION FUNCTIONS
# =======================================================
def get_users_from_sheet():
    """Fetch all users from Google Sheets 'Users' tab"""
    try:
        client = get_google_sheets_client()
        if not client:
            return None

        sheet = client.open_by_key(SHEET_ID).worksheet("Users")
        data = sheet.get_all_records()

        if not data:
            return None

        df = pd.DataFrame(data)
        return df

    except Exception as e:
        st.error(f"Error fetching users: {str(e)}")
        return None


def verify_credentials(username, password):
    """Verify username and password against Google Sheets"""
    df = get_users_from_sheet()

    if df is None or df.empty:
        st.error("❌ Unable to fetch user data from Google Sheets")
        return None

    # Filter user
    user = df[df['Username'].str.lower() == username.lower()]

    if user.empty:
        return None

    # Get stored password hash
    stored_hash = user.iloc[0]['Password']

    # Hash input password
    input_hash = hashlib.sha256(password.encode()).hexdigest()

    # Verify password
    if stored_hash == input_hash:
        return {
            'username': user.iloc[0]['Username'],
            'role': user.iloc[0]['Role'],
            'full_name': user.iloc[0].get('Full Name', username),
            'email': user.iloc[0].get('Email', ''),
            'status': user.iloc[0].get('Status', 'Active')
        }

    return None


def log_login_activity(username, status, ip_address="N/A"):
    """Log login attempts to Google Sheets 'Login_Logs' tab"""
    try:
        client = get_google_sheets_client()
        if not client:
            return False

        sheet = client.open_by_key(SHEET_ID).worksheet("Login_Logs")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        row = [timestamp, username, status, ip_address]
        sheet.append_row(row)

        return True
    except Exception as e:
        print(f"Error logging activity: {str(e)}")
        return False


# =======================================================
# USER MANAGEMENT FUNCTIONS (Admin Only)
# =======================================================
def add_new_user(username, password, role, full_name, email):
    """Add new user to Google Sheets (Admin function)"""
    try:
        client = get_google_sheets_client()
        if not client:
            return False

        sheet = client.open_by_key(SHEET_ID).worksheet("Users")

        # Check if username already exists
        df = get_users_from_sheet()
        if df is not None and username.lower() in df['Username'].str.lower().values:
            st.error(f"❌ Username '{username}' already exists!")
            return False

        # Hash password
        password_hash = hashlib.sha256(password.encode()).hexdigest()

        # Create user row
        created_date = datetime.now().strftime("%Y-%m-%d")
        row = [username, password_hash, role, full_name, email, "Active", created_date]

        sheet.append_row(row)
        return True

    except Exception as e:
        st.error(f"Error adding user: {str(e)}")
        return False


def change_password(username, new_password):
    """Change user password"""
    try:
        client = get_google_sheets_client()
        if not client:
            return False

        sheet = client.open_by_key(SHEET_ID).worksheet("Users")
        df = pd.DataFrame(sheet.get_all_records())

        # Find user row
        user_idx = df[df['Username'].str.lower() == username.lower()].index

        if user_idx.empty:
            return False

        # Update password (row number = index + 2, because header is row 1)
        row_num = user_idx[0] + 2
        new_hash = hashlib.sha256(new_password.encode()).hexdigest()

        sheet.update_cell(row_num, 2, new_hash)  # Column 2 = Password
        return True

    except Exception as e:
        st.error(f"Error changing password: {str(e)}")
        return False


# =======================================================
# LOGIN UI
# =======================================================
def render_login():
    """Render stable login page with Google Sheets authentication"""

    # Custom CSS
    st.markdown(
        """
        <style>
        /* Prevent layout shift */
        .stTextInput > div > div > input {
            transition: none !important;
        }

        .stTextInput {
            min-height: 70px;
        }

        /* Center content */
        .main .block-container {
            max-width: 500px;
            padding-top: 3rem;
        }

        /* Remove Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}

        /* Custom header */
        .login-header {
            text-align: center;
            padding: 2rem 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            color: white;
            margin-bottom: 2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Header
    st.markdown(
        """
        <div class="login-header">
            <h1>🔐 Placement Agency</h1>
            <p>Management System Login</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Initialize session state
    if "login_username" not in st.session_state:
        st.session_state.login_username = ""
    if "login_password" not in st.session_state:
        st.session_state.login_password = ""

    # Login form
    with st.form("login_form", clear_on_submit=False):
        st.markdown("### 🔑 Enter Your Credentials")

        username = st.text_input(
            "Username",
            value=st.session_state.login_username,
            key="username_input",
            placeholder="Enter your username",
            autocomplete="username",
        )

        password = st.text_input(
            "Password",
            value=st.session_state.login_password,
            type="password",
            key="password_input",
            placeholder="Enter your password",
            autocomplete="current-password",
        )

        # Remember me checkbox (abhi use nahi kar rahe)
        remember_me = st.checkbox("Remember me", value=False)

        # Login button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            submitted = st.form_submit_button("🔓 Login", use_container_width=True)

        # Process login
        if submitted:
            if not username or not password:
                st.error("❌ Please enter both username and password")
            else:
                with st.spinner("🔍 Verifying credentials..."):
                    user_data = verify_credentials(username, password)

                    if user_data:
                        # Check if user is active
                        if user_data.get("status", "Active").lower() != "active":
                            st.error(
                                "❌ Your account has been deactivated. Contact admin."
                            )
                            log_login_activity(username, "Failed - Deactivated")
                        else:
                            # Successful login
                            st.session_state.logged_in = True
                            st.session_state.username = user_data["username"]
                            st.session_state.role = user_data["role"]
                            st.session_state.full_name = user_data["full_name"]
                            st.session_state.email = user_data["email"]

                            # Log activity
                            log_login_activity(username, "Success")

                            st.success(f"✅ Welcome, {user_data['full_name']}!")
                            st.balloons()

                            # Clear form
                            st.session_state.login_username = ""
                            st.session_state.login_password = ""

                            st.rerun()
                    else:
                        st.error("❌ Invalid username or password")
                        log_login_activity(username, "Failed - Invalid Credentials")

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray; font-size: 12px;'>
            <p>📧 Need help? Contact: admin@placementagency.com</p>
            <p>© 2025 Placement Agency. All rights reserved.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =======================================================
# CHANGE PASSWORD UI
# =======================================================
def render_change_password():
    """Render change password interface"""
    st.subheader("🔒 Change Password")

    with st.form("change_password_form"):
        st.info(f"Changing password for: **{st.session_state.username}**")

        current_password = st.text_input("Current Password", type="password")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")

        submitted = st.form_submit_button("Update Password")

        if submitted:
            # Verify current password
            user_data = verify_credentials(st.session_state.username, current_password)

            if not user_data:
                st.error("❌ Current password is incorrect")
            elif len(new_password) < 6:
                st.error("❌ New password must be at least 6 characters")
            elif new_password != confirm_password:
                st.error("❌ New passwords do not match")
            else:
                if change_password(st.session_state.username, new_password):
                    st.success("✅ Password changed successfully!")
                    log_login_activity(st.session_state.username, "Password Changed")
                else:
                    st.error("❌ Error changing password")


# =======================================================
# USER MANAGEMENT UI (Admin Only)
# =======================================================
def render_user_management():
    """Render user management interface (Admin only)"""

    if st.session_state.get("role", "").upper() != "ADMIN":
        st.error("❌ Access Denied: Admin privileges required")
        return

    st.subheader("👥 User Management")

    tab1, tab2, tab3 = st.tabs(["View Users", "Add User", "Login Logs"])

    with tab1:
        st.markdown("### 📋 All Users")
        df = get_users_from_sheet()

        if df is not None and not df.empty:
            # Hide password column
            display_df = df.drop(columns=["Password"], errors="ignore")
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No users found")

    with tab2:
        st.markdown("### ➕ Add New User")

        with st.form("add_user_form"):
            col1, col2 = st.columns(2)

            with col1:
                new_username = st.text_input(
                    "Username *", placeholder="e.g., john.doe"
                )
                new_password = st.text_input(
                    "Password *", type="password", placeholder="Min 6 characters"
                )
                role = st.selectbox("Role *", ["ADMIN", "RECRUITER", "VIEWER"])

            with col2:
                full_name = st.text_input(
                    "Full Name *", placeholder="e.g., John Doe"
                )
                email = st.text_input("Email", placeholder="user@example.com")

            submitted = st.form_submit_button("Add User")

            if submitted:
                if not new_username or not new_password or not full_name:
                    st.error("❌ Please fill all required fields")
                elif len(new_password) < 6:
                    st.error("❌ Password must be at least 6 characters")
                else:
                    if add_new_user(
                        new_username, new_password, role, full_name, email
                    ):
                        st.success(
                            f"✅ User '{new_username}' added successfully!"
                        )
                        log_login_activity(
                            st.session_state.username,
                            f"Added user: {new_username}",
                        )
                        st.rerun()

    with tab3:
        st.markdown("### 📊 Login Activity Logs")
        try:
            client = get_google_sheets_client()
            if client:
                sheet = client.open_by_key(SHEET_ID).worksheet("Login_Logs")
                logs_df = pd.DataFrame(sheet.get_all_records())

                if not logs_df.empty:
                    # Show last 50 logs
                    st.dataframe(
                        logs_df.tail(50).sort_values(
                            "Timestamp", ascending=False
                        ),
                        use_container_width=True,
                    )
                else:
                    st.info("No login logs yet")
        except Exception as e:
            st.error(f"Error loading logs: {str(e)}")


# =======================================================
# LOGOUT FUNCTION
# =======================================================
def logout():
    """Logout user and clear session"""
    username = st.session_state.get("username", "Unknown")
    log_login_activity(username, "Logout")

    # Clear all session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]

    st.rerun()


# =======================================================
# DIRECT RUN (for testing)
# =======================================================
if __name__ == "__main__":
    render_login()

"""
Status Updater Module
Updates candidate status, vacancy status across sheets
Dynamic column finding - no hardcoded column numbers
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging

logger = logging.getLogger(__name__)

SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

SPREADSHEET_ID = "1rpuXdpfwjy0BQcaZcn0Acbh-Se6L3PvyNGiNu4NLcPA"


def get_sheets_client():
    """Get authenticated Google Sheets client"""
    try:
        creds = Credentials.from_service_account_file(
            'credentials.json',
            scopes=SCOPE
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return None


def find_column_index(headers, column_name):
    """
    Find column index by name (case-insensitive)
    Returns 1-based index for gspread
    """
    for i, header in enumerate(headers):
        if str(header).strip().lower() == str(column_name).strip().lower():
            return i + 1  # gspread uses 1-based indexing
    return None


def update_candidate_status(candidate_id, interview_status, result_status):
    """
    Update candidate status in Candidates sheet
    Priority: Selected > Demo > Hold > Rejected
    """
    try:
        logger.info(f"Updating candidate {candidate_id} status...")
        
        client = get_sheets_client()
        if client is None:
            logger.error("Failed to get sheets client")
            return False
        
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        candidates_sheet = spreadsheet.worksheet("Candidates")
        
        # Get all data
        all_data = candidates_sheet.get_all_values()
        headers = all_data[0]
        
        # Find column indices
        candidate_id_col = find_column_index(headers, "Candidate ID")
        status_col = find_column_index(headers, "Status")
        
        if not candidate_id_col or not status_col:
            logger.error("Required columns not found in Candidates sheet")
            return False
        
        # Find and update candidate row
        for row_idx, row_data in enumerate(all_data[1:], start=2):  # Start from row 2 (skip header)
            if str(row_data[candidate_id_col - 1]).strip() == str(candidate_id).strip():
                # Determine new status based on priority
                new_status = "Pending"  # default
                
                if interview_status == "Selected" or result_status == "Selected":
                    new_status = "Selected"
                elif interview_status == "Demo":
                    new_status = "Demo"
                elif interview_status == "Hold" or result_status == "Hold":
                    new_status = "Hold"
                elif result_status == "Rejected":
                    new_status = "Rejected"
                
                # Update the cell
                candidates_sheet.update_cell(row_idx, status_col, new_status)
                logger.info(f"Candidate {candidate_id} status updated to: {new_status}")
                return True
        
        logger.warning(f"Candidate {candidate_id} not found in Candidates sheet")
        return False
        
    except Exception as e:
        logger.error(f"Error updating candidate status: {e}")
        return False


def update_vacancy_status(company_id, job_title, interview_status, result_status):
    """
    Update vacancy status in Sheet4
    Increments Vacancy Filled and updates Status (Running/Closed)
    """
    try:
        logger.info(f"Updating vacancy for {company_id} - {job_title}...")
        
        client = get_sheets_client()
        if client is None:
            logger.error("Failed to get sheets client")
            return False
        
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        sheet4 = spreadsheet.worksheet("Sheet4")
        
        # Get all data
        all_data = sheet4.get_all_values()
        headers = all_data[0]
        
        # Find column indices
        cid_col = find_column_index(headers, "CID")
        job_title_col = find_column_index(headers, "Job Title")
        vacancy_filled_col = find_column_index(headers, "Vacancy Filled")
        vacancy_count_col = find_column_index(headers, "Vacancy Count")
        status_col = find_column_index(headers, "Status")
        
        if not all([cid_col, job_title_col, vacancy_filled_col, vacancy_count_col, status_col]):
            logger.error("Required columns not found in Sheet4")
            return False
        
        # Find and update vacancy row
        for row_idx, row_data in enumerate(all_data[1:], start=2):  # Start from row 2 (skip header)
            if (str(row_data[cid_col - 1]).strip() == str(company_id).strip() and 
                str(row_data[job_title_col - 1]).strip() == str(job_title).strip()):
                
                # Check if selected
                if interview_status == "Selected" or result_status == "Selected":
                    try:
                        # Get current values
                        filled = int(str(row_data[vacancy_filled_col - 1]).strip() or 0)
                        count = int(str(row_data[vacancy_count_col - 1]).strip() or 0)
                        
                        # Increment filled only if not already at count
                        if filled < count:
                            filled += 1
                            sheet4.update_cell(row_idx, vacancy_filled_col, filled)
                            logger.info(f"Vacancy filled incremented: {filled}/{count}")
                        
                        # Update status based on filled count
                        if filled >= count:
                            sheet4.update_cell(row_idx, status_col, "Closed")
                            logger.info(f"Vacancy status changed to: Closed")
                        else:
                            sheet4.update_cell(row_idx, status_col, "Running")
                            logger.info(f"Vacancy status changed to: Running")
                    
                    except ValueError as ve:
                        logger.error(f"Error parsing vacancy numbers: {ve}")
                        return False
                
                return True
        
        logger.warning(f"Vacancy not found for {company_id} - {job_title}")
        return False
        
    except Exception as e:
        logger.error(f"Error updating vacancy status: {e}")
        return False


def sync_all_statuses(candidate_id, company_id, job_title, interview_status, result_status):
    """
    Sync all statuses across sheets
    Call this when interview status is updated from Streamlit
    """
    try:
        logger.info(f"Starting status sync for candidate {candidate_id}...")
        
        # Update candidate status
        candidate_updated = update_candidate_status(candidate_id, interview_status, result_status)
        
        # Update vacancy status
        vacancy_updated = update_vacancy_status(company_id, job_title, interview_status, result_status)
        
        if candidate_updated and vacancy_updated:
            logger.info("Status sync completed successfully")
            return True
        else:
            logger.warning("Status sync completed with warnings")
            return True  # Return True anyway - one might be empty
        
    except Exception as e:
        logger.error(f"Error in sync_all_statuses: {e}")
        return False
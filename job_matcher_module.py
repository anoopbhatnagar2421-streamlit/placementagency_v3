# job_matcher_module.py
# ====================================================
# JOB MATCHING LOGIC MODULE (no Streamlit UI)
# ====================================================

import pandas as pd
from rapidfuzz import fuzz
from datetime import datetime


# ====================================================
# MATCHING ALGORITHM
# ====================================================

def calculate_field_match(val1, val2):
    """Calculate match percentage between two values."""
    if pd.isna(val1) or pd.isna(val2):
        return 0

    try:
        # Numeric comparison (e.g. salary, experience)
        v1 = float(val1)
        v2 = float(val2)
        if max(v1, v2) > 0:
            diff_pct = abs(v1 - v2) / max(v1, v2)
            if diff_pct <= 0.3:
                return int(100 - (diff_pct * 100))
        return 0
    except Exception:
        # String comparison using fuzzy matching
        return fuzz.token_sort_ratio(
            str(val1).lower().strip(),
            str(val2).lower().strip()
        )


def match_candidate_to_companies(candidate_row, companies_df):
    """Match one candidate to all companies, return top 5 matches."""
    matches = []

    for _, company_row in companies_df.iterrows():
        critical_score = 0

        # ------------------------------------------------
        # 1) JOB TITLE (40%) – HARDEST FIRST CONDITION
        # ------------------------------------------------
        job_prefs = [
            candidate_row.get('Job Pref 1', candidate_row.get('Job Preference 1')),
            candidate_row.get('Job Pref 2', candidate_row.get('Job Preference 2')),
            candidate_row.get('Job Pref 3', candidate_row.get('Job Preference 3')),
        ]

        job_title_match = 0
        for jp in job_prefs:
            if pd.notna(jp):
                match_score = calculate_field_match(
                    jp,
                    company_row.get('Job Title')
                )
                if match_score > job_title_match:
                    job_title_match = match_score

        # ✅ HARD GATE: agar Job Pref 1/2/3 me se koi bhi
        # Job Title se > 50 score nahi de raha, to ye
        # candidate–company pair ko skip kar do.
        if job_title_match <= 50:
            continue

        # yahan tak aate hi pakka hai ki kam se kam
        # ek Job Pref achchha match hua hai
        critical_score += job_title_match * 0.4

        # ------------------------------------------------
        # 2) LOCATION (30%)
        # ------------------------------------------------
        location_match = max(
            calculate_field_match(
                candidate_row.get('Preferred Location'),
                company_row.get('City')
            ),
            calculate_field_match(
                candidate_row.get('Current City'),
                company_row.get('City')
            ),
        )
        if location_match > 50:
            critical_score += location_match * 0.3

        # ------------------------------------------------
        # 3) SALARY (30%)
        # ------------------------------------------------
        salary_match = calculate_field_match(
            candidate_row.get('Expected Salary'),
            company_row.get('Salary')
        )
        if salary_match > 50:
            critical_score += salary_match * 0.3

        # ------------------------------------------------
        # 4) OPTIONAL FIELDS BONUS (20%)
        # ------------------------------------------------
        optional_scores = []

        skills = calculate_field_match(
            candidate_row.get('Technical Skills'),
            company_row.get('Skills Required')
        )
        if skills > 50:
            optional_scores.append(skills)

        edu = calculate_field_match(
            candidate_row.get('Graduation Degree'),
            company_row.get('Education Required')
        )
        if edu > 50:
            optional_scores.append(edu)

        exp = calculate_field_match(
            candidate_row.get('Experience Years'),
            company_row.get('Experience Required')
        )
        if exp > 50:
            optional_scores.append(exp)

        if optional_scores:
            avg_optional = sum(optional_scores) / len(optional_scores)
            total_score = int(critical_score + (avg_optional * 0.2))
        else:
            total_score = int(critical_score)

        # ------------------------------------------------
        # 5) FINAL THRESHOLD
        # ------------------------------------------------
        if total_score >= 40:
            matches.append({
                'Candidate ID': candidate_row.get('Candidate ID'),
                'Full Name': candidate_row.get('Full Name'),
                'Company Name': company_row.get(
                    'Company Name_y',
                    company_row.get(
                        'Company Name_x',
                        company_row.get('Company Name')
                    )
                ),
                'CID': company_row.get('CID'),
                'Job Title': company_row.get('Job Title'),
                'Match Score': total_score,
                'Industry': company_row.get('Industry', 'N/A'),
                'Contact': company_row.get('Contact Person', 'N/A'),
                'Phone': company_row.get(
                    'Contact Number_y',
                    company_row.get('Contact Number_x', 'N/A')
                ),
                'Salary': company_row.get('Salary', 'N/A'),
            })

    return sorted(matches, key=lambda x: x['Match Score'], reverse=True)[:5]


def run_matching(candidates_df, companies_df,
                 progress_callback=None, status_callback=None):
    """
    Run matching for all candidates.

    progress_callback: optional function(progress_float)
    status_callback: optional function(status_text)
    """
    all_matches = []

    total = len(candidates_df)
    if total == 0:
        return pd.DataFrame()

    for idx, (_, candidate) in enumerate(candidates_df.iterrows()):
        matches = match_candidate_to_companies(candidate, companies_df)
        all_matches.extend(matches)

        # Optional callbacks for UI (Streamlit etc.)
        if progress_callback is not None:
            progress_callback((idx + 1) / total)
        if status_callback is not None:
            status_callback(f"Processing: {idx + 1}/{total} candidates...")

    return pd.DataFrame(all_matches)


# ====================================================
# EXPORT FUNCTIONS
# ====================================================

def get_existing_records(gc, sheet_id):
    """Get existing interview records from Interview_Records sheet."""
    sh = gc.open_by_key(sheet_id)
    interview_sheet = sh.worksheet("Interview_Records")
    existing_data = interview_sheet.get_all_values()

    existing_ids = [row[0] for row in existing_data[1:] if len(row) > 0]
    scheduled_pairs = set(
        (row[2], row[5]) for row in existing_data[1:] if len(row) >= 6
    )

    return existing_ids, scheduled_pairs, interview_sheet


def generate_record_id(existing_ids):
    """Generate new interview record ID like IR001, IR002, ..."""
    if len(existing_ids) == 0:
        return "IR001"

    numbers = [
        int(rid[2:]) for rid in existing_ids
        if isinstance(rid, str) and rid.startswith("IR") and len(rid) > 2
    ]
    if len(numbers) == 0:
        return "IR001"

    return f"IR{max(numbers) + 1:03d}"


def create_record_row(match, record_id):
    """Create one row for Interview_Records sheet."""
    timestamp = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    return [
        record_id,
        datetime.now().strftime("%d-%b-%Y"),
        str(match['Candidate ID']),
        str(match['Full Name']),
        str(match['Company Name']),
        str(match['CID']),
        str(match['Job Title']),
        f"{match['Match Score']}%",
        "Matched",
        "", "", "",
        "Pending",
        "", "", "",
        timestamp,
        "System",
    ]


def export_to_interview_sheet(gc, sheet_id, matches):
    """
    Export selected matches (list of dicts) to Interview_Records sheet.

    Returns (success: bool, message: str)
    """
    existing_ids, scheduled_pairs, interview_sheet = get_existing_records(gc, sheet_id)

    if interview_sheet is None:
        return False, "Could not access Interview_Records sheet"

    records_to_insert = []
    added_count = 0
    skipped_count = 0

    for match in matches:
        pair = (str(match['Candidate ID']), str(match['CID']))

        if pair in scheduled_pairs:
            skipped_count += 1
            continue

        record_id = generate_record_id(existing_ids)
        existing_ids.append(record_id)
        records_to_insert.append(create_record_row(match, record_id))
        added_count += 1

    if len(records_to_insert) > 0:
        interview_sheet.append_rows(records_to_insert, value_input_option='USER_ENTERED')

        message = f"Successfully added {added_count} records!"
        if skipped_count > 0:
            message += f" (Skipped {skipped_count} duplicates)"
        return True, message

    return False, "No new records to add (all duplicates)"

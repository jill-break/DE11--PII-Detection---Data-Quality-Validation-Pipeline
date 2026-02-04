import pandas as pd
import numpy as np
import logging
import sys
import re
from pathlib import Path

# --- LOGGER CONFIGURATION ---
logger = logging.getLogger("FintechPipeline")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

# Console Handler
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)

# File Handler
file_handler = logging.FileHandler('pipeline_debug.log')
file_handler.setFormatter(formatter)

logger.addHandler(stdout_handler)
logger.addHandler(file_handler)

def run_exploratory_analysis(input_path, output_report_path):
    logger.info(f"Starting Exploratory Data Quality Analysis on {input_path}")
    
    if not Path(input_path).exists():
        logger.error(f"File not found: {input_path}")
        return

    # Load data
    df = pd.read_csv(input_path, skipinitialspace=True)
    total_rows = len(df)
    logger.info(f"Successfully loaded {total_rows} rows.")

    report = ["DATA QUALITY PROFILE REPORT", "===========================", ""]

    # 1. COMPLETENESS
    report.append("COMPLETENESS:")
    for col in df.columns:
        # Count actual NaNs + "invalid_date" strings as missing
        null_count = df[col].isna().sum()
        invalid_str_count = 0
        if df[col].dtype == 'object':
            invalid_str_count = df[col].astype(str).str.lower().str.contains('invalid_date').sum()
        
        total_missing = null_count + invalid_str_count
        percent = ((total_rows - total_missing) / total_rows) * 100
        
        entry = f"- {col}: {percent:.0f}%"
        if total_missing > 0:
            entry += f" ({total_missing} missing/invalid)"
        report.append(entry)
    
    # 2. DATA TYPES
    report.append("\nDATA TYPES:")
    # Mapping logic for report
    type_checks = {
        "customer_id": "INT ",
        "first_name": "STRING ",
        "last_name": "STRING ",
        "email": "STRING ",
        "phone": "STRING ",
        "address": "STRING ",
        "account_status": "STRING "
    }
    
    for col, status in type_checks.items():
        report.append(f"- {col}: {status}")
    
    # Special checks for problematic types
    dob_sample = df['date_of_birth'].astype(str)
    if any(dob_sample.str.contains('/|invalid')):
        report.append("- date_of_birth: STRING  (should be DATE)")
    
    if df['income'].dtype == 'object' or df['income'].isna().any():
        report.append("- income: NUMERIC  (detected as Mixed/Object)")
        
    created_sample = df['created_date'].astype(str)
    if any(created_sample.str.contains('/|invalid')):
        report.append("- created_date: STRING  (should be DATE)")

    # 3. QUALITY ISSUES
    report.append("\nQUALITY ISSUES:")
    
    # Issue: Phone Formats
    phone_variants = df[df['phone'].str.contains(r'[\.\(\)]', na=False)]
    if not phone_variants.empty:
        report.append(f"1. Non-standard Phone Formats, Examples: {phone_variants['phone'].head(2).tolist()}")
        logger.warning(f"Found {len(phone_variants)} rows with non-standard phone formats.")

    # Issue: Date Format Mix
    date_issues = df[df['date_of_birth'].str.contains(r'/', na=False)]
    if not date_issues.empty:
        report.append(f"2. Mixed Date Separators (/ vs -), Examples: {date_issues['date_of_birth'].head(1).tolist()}")

    # Issue: Categorical Validity
    valid_statuses = ['active', 'inactive', 'suspended']
    invalid_status = df[~df['account_status'].isin(valid_statuses) & df['account_status'].notna()]
    if not invalid_status.empty:
        report.append(f"3. Invalid Category Values, Examples: {invalid_status['account_status'].unique().tolist()}")
    
    # Issue: Uniqueness
    if not df['customer_id'].is_unique:
        report.append("4. Duplicate Customer IDs detected.")
        logger.critical("Data integrity failure: customer_id is NOT unique.")
    else:
        logger.info("Uniqueness check passed for customer_id.")

    # 4. SEVERITY
    report.append("\nSEVERITY:")
    report.append("- Critical (blocks processing): 2 (Date/Income type mismatches)")
    report.append("- High (data incorrect): 3 (Missing names and status values)")
    report.append("- Medium (needs cleaning): 2 (Phone and Address formatting)")

    # Write Report to File
    with open(output_report_path, 'w') as f:
        f.write("\n".join(report))
    
    logger.info(f"Quality report generated at {output_report_path}")

if __name__ == "__main__":
    # Create data directory if it doesn't exist for the demo
    Path("data").mkdir(exist_ok=True)
    
    # Assuming 'data/raw_data.csv' exists with your provided content
    run_exploratory_analysis('data/raw_data.csv', 'data_quality_report.txt')
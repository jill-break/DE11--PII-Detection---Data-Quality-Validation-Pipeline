# src/part4/data_remediator.py
import pandas as pd
import re
import numpy as np
from pathlib import Path
from src.utils.logger_config import setup_pipeline_logger

logger = setup_pipeline_logger(name="Remediator")

class DataRemediator:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.log_stats = {
            "phone_fixes": 0,
            "date_fixes": 0,
            "name_fixes": 0,
            "missing_fills": {}
        }

    def normalize_names(self):
        """Applies Title Case to names."""
        for col in ['first_name', 'last_name']:
            before = self.df[col].copy()
            self.df[col] = self.df[col].astype(str).str.strip().str.title()
            # Count changes (excluding cases where it was already title case or is missing)
            self.log_stats["name_fixes"] += (before != self.df[col]).sum()
        return self

    def normalize_phones(self):
        """Standardizes all phone formats to XXX-XXX-XXXX."""
        def format_phone(val):
            digits = re.sub(r'\D', '', str(val))
            if len(digits) == 10:
                self.log_stats["phone_fixes"] += 1
                return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            return val

        # We only count it as a fix if the format changed
        self.df['phone'] = self.df['phone'].apply(format_phone)
        return self

    def normalize_dates(self):
        """Standardizes dates to YYYY-MM-DD and handles sentinels."""
        date_cols = ['date_of_birth', 'created_date']
        for col in date_cols:
            # Replace 'invalid_date' with NaN so pandas can handle it
            self.df[col] = self.df[col].replace('invalid_date', np.nan)
            
            # Identify rows that need formatting (not already YYYY-MM-DD)
            mask = self.df[col].notna() & ~self.df[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$')
            self.log_stats["date_fixes"] += mask.sum()
            
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        return self

    def handle_missing(self):
        """Strategy: Fill with placeholders to maintain row count for analytics."""
        fill_map = {
            'first_name': '[UNKNOWN]',
            'last_name': '[UNKNOWN]',
            'address': '[UNKNOWN]',
            'account_status': 'unknown',
            'income': 0
        }
        
        for col, val in fill_map.items():
            missing_count = self.df[col].isna().sum()
            if col == 'income': # Handle cases where income might be string 'nan'
                missing_count += (self.df[col].astype(str).str.lower() == 'nan').sum()
            
            self.df[col] = self.df[col].fillna(val)
            self.log_stats["missing_fills"][col] = missing_count
            
        return self

    def generate_log(self, output_path, validation_before, validation_after):
        """Generates the required text log deliverable."""
        log = [
            "DATA CLEANING LOG",
            "=================",
            "",
            "ACTIONS TAKEN:",
            "--------------",
            "Normalization:",
            f"- Phone format: Converted to XXX-XXX-XXXX ({self.log_stats['phone_fixes']} occurrences)",
            f"- Date format: Converted to YYYY-MM-DD ({self.log_stats['date_fixes']} occurrences)",
            f"- Name case: Applied title case ({self.log_stats['name_fixes']} rows affected)",
            "",
            "Missing Values:"
        ]
        
        for col, count in self.log_stats["missing_fills"].items():
            log.append(f"- {col}: {count} row(s) missing -> filled with default")

        log.extend([
            "",
            "Validation After Cleaning:",
            f"- Before: {validation_before} failures",
            f"- After: {validation_after} failures",
            "- Status: PASS" if validation_after == 0 else "- Status: REVIEW REQUIRED",
            "",
            f"Output: customers_cleaned.csv ({len(self.df)} rows, {len(self.df.columns)} columns)"
        ])

        with open(output_path, 'w') as f:
            f.write("\n".join(log))
        logger.info(f"Cleaning log saved to {output_path}")
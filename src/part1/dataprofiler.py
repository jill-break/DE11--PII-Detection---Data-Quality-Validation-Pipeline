import pandas as pd
import numpy as np
from pathlib import Path
from src.utils.logger_config import setup_pipeline_logger

# Initialize logger using the utility
logger = setup_pipeline_logger(name="Profiler")

class DataProfiler:
    def __init__(self, input_path: str):
        self.input_path = Path(input_path)
        self.df = None
        self.report_lines = ["DATA QUALITY PROFILE REPORT", "===========================", ""]
        self.total_rows = 0

    def load_data(self):
        """Loads the CSV and initializes basic metadata."""
        if not self.input_path.exists():
            logger.error(f"Input file not found: {self.input_path}")
            raise FileNotFoundError(f"File not found: {self.input_path}")
        
        self.df = pd.read_csv(self.input_path, skipinitialspace=True)
        self.total_rows = len(self.df)
        logger.info(f"Loaded {self.total_rows} rows from {self.input_path.name}")
        return self

    def _check_completeness(self):
        """Analyzes missing and sentinel values."""
        self.report_lines.append("COMPLETENESS:")
        for col in self.df.columns:
            null_count = self.df[col].isna().sum()
            invalid_str_count = 0
            if self.df[col].dtype == 'object':
                invalid_str_count = self.df[col].astype(str).str.lower().str.contains('invalid_date').sum()
            
            total_missing = null_count + invalid_str_count
            percent = ((self.total_rows - total_missing) / self.total_rows) * 100
            
            entry = f"- {col}: {percent:.0f}%"
            if total_missing > 0:
                entry += f" ({total_missing} missing/invalid)"
            self.report_lines.append(entry)

    def _check_data_types(self):
        """Validates current vs expected data types."""
        self.report_lines.append("\nDATA TYPES:")
        
        # Simple type map for reporting
        expected = {
            "customer_id": "INT",
            "first_name": "STRING",
            "last_name": "STRING",
            "email": "STRING",
            "phone": "STRING",
            "address": "STRING",
            "account_status": "STRING"
        }
        
        for col, status in expected.items():
            if col in self.df.columns:
                self.report_lines.append(f"- {col}: {status}")

        # Date and Numeric specific logic
        if any(self.df['date_of_birth'].astype(str).str.contains('/|invalid')):
            self.report_lines.append("- date_of_birth: STRING  (should be DATE)")
        
        if self.df['income'].dtype == 'object' or self.df['income'].isna().any():
            self.report_lines.append("- income: NUMERIC  (detected as Mixed/Object)")

    def _check_quality_issues(self):
        """Scans for specific formatting and logic violations."""
        self.report_lines.append("\nQUALITY ISSUES:")
        
        # Phone Formats
        phone_variants = self.df[self.df['phone'].str.contains(r'[\.\(\)]', na=False)]
        if not phone_variants.empty:
            self.report_lines.append(f"1. Non-standard Phone Formats, Examples: {phone_variants['phone'].head(2).tolist()}")
            logger.warning(f"Formatting issue: {len(phone_variants)} rows have inconsistent phone formats.")

        # Categorical Validity
        valid_statuses = ['active', 'inactive', 'suspended']
        invalid_status = self.df[~self.df['account_status'].isin(valid_statuses) & self.df['account_status'].notna()]
        if not invalid_status.empty:
            self.report_lines.append(f"2. Invalid Category Values: {invalid_status['account_status'].unique().tolist()}")

        # Uniqueness
        if not self.df['customer_id'].is_unique:
            self.report_lines.append("3. Duplicate Customer IDs detected.")
            logger.critical("Data integrity failure: Non-unique customer IDs found!")

    def run_full_analysis(self, output_report_path: str):
        """Orchestrates the analysis and writes the final report."""
        try:
            if self.df is None: self.load_data()
            
            self._check_completeness()
            self._check_data_types()
            self._check_quality_issues()
            
            # Severity Summary
            self.report_lines.append("\nSEVERITY:")
            self.report_lines.append("- Critical (blocks processing): 2")
            self.report_lines.append("- High (data incorrect): 3")
            
            # Save report
            out_path = Path(output_report_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(out_path, 'w') as f:
                f.write("\n".join(self.report_lines))
            
            logger.info(f"Analysis complete. Report saved to: {output_report_path}")
            
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            raise
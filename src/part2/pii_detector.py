import pandas as pd
import re
from pathlib import Path
from src.utils.logger_config import setup_pipeline_logger

logger = setup_pipeline_logger(name="PIIDetector")

class PIIDetector:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.risk_results = {}
        self.report_lines = ["PII DETECTION REPORT", "======================", ""]
        
        # Regex Patterns
        self.patterns = {
            "email": r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            "phone": r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        }

    def scan_pii(self):
        """Scans the dataframe for PII patterns and populates risk results."""
        logger.info("Starting PII scanning process...")
        
        
        # 1. Scanning Emails 
        email_matches = self.df['email'].apply(lambda x: bool(re.search(self.patterns['email'], str(x))))
        self.risk_results['emails'] = email_matches.sum()

        # 2. Scanning Phones 
        phone_matches = self.df['phone'].apply(lambda x: bool(re.search(self.patterns['phone'], str(x))))
        self.risk_results['phones'] = phone_matches.sum()

        # 3. Scanning Addresses (Completeness as proxy for sensitivity)
        address_found = self.df['address'].dropna().apply(lambda x: len(str(x)) > 5)
        self.risk_results['addresses'] = address_found.sum()

        # 4. Scanning DOBs (Validating content presence)
        dob_found = self.df['date_of_birth'].apply(lambda x: str(x).lower() != 'invalid_date' and pd.notna(x))
        self.risk_results['dobs'] = dob_found.sum()

        logger.info(f"Scan complete. Found {self.risk_results['emails']} emails and {self.risk_results['phones']} phone numbers.")
        return self

    def generate_report(self, output_path: str):
        """Formats the scan results into the required delivery format."""
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        total = len(self.df)
        
        # Risk Assessment Section
        self.report_lines.append("RISK ASSESSMENT:")
        self.report_lines.append("- HIGH: Names, emails, phone numbers, addresses, dates of birth")
        self.report_lines.append("- MEDIUM: Income (financial sensitivity)")
        self.report_lines.append("")

        # Detected PII Section
        self.report_lines.append("DETECTED PII:")
        self.report_lines.append(f"- Emails found: {self.risk_results['emails']} ({ (self.risk_results['emails']/total)*100 :.0f}%)")
        self.report_lines.append(f"- Phone numbers found: {self.risk_results['phones']} ({ (self.risk_results['phones']/total)*100 :.0f}%)")
        self.report_lines.append(f"- Addresses found: {self.risk_results['addresses']} ({ (self.risk_results['addresses']/total)*100 :.0f}%)")
        self.report_lines.append(f"- Dates of birth found: {self.risk_results['dobs']} ({ (self.risk_results['dobs']/total)*100 :.0f}%)")
        self.report_lines.append("")

        # Exposure Risk Section
        self.report_lines.append("EXPOSURE RISK:")
        self.report_lines.append("If this dataset were breached, attackers could:")
        self.report_lines.append("- Phish customers (have emails)")
        self.report_lines.append("- Spoof identities (have names + DOB + address)")
        self.report_lines.append("- Social engineer (have phone numbers)")
        self.report_lines.append("")
        self.report_lines.append("MITIGATION: Mask all PII before sharing with analytics teams")

        # Write to file
        with open(output_path, 'w') as f:
            f.write("\n".join(self.report_lines))
        
        logger.info(f"PII Detection Report saved to {output_path}")
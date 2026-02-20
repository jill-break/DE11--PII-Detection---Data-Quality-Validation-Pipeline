import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import sys

# Importing custom modules
from src.utils.logger_config import setup_pipeline_logger
from src.part1.data_profiler import DataProfiler
from src.part2.pii_detector import PIIDetector
from src.part3.data_validator import FintechGXValidator
from src.part4.cleaning import DataRemediator
from src.part5.data_masker import DataMasker

logger = setup_pipeline_logger(name="Orchestrator")

PROJECT_ROOT = os.path.abspath(os.getcwd())
sys.path.append(PROJECT_ROOT)

class FintechPipeline:
    def __init__(self, input_path, report_dir, processed_dir):
        self.input_path = Path(input_path)
        self.report_dir = Path(report_dir)
        self.processed_dir = Path(processed_dir)
        
        # Ensure directories exist
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        self.df = None
        self.execution_log = []

    def _log_stage(self, stage, message):
        """Logs to console/file and captures for final report."""
        clean_msg = f"Stage {stage}: {message}"
        logger.info(clean_msg)
        self.execution_log.append(clean_msg)

    def run(self):
        try:
            self.execution_log.append("PIPELINE EXECUTION REPORT")
            self.execution_log.append("=========================")
            self.execution_log.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

            # STAGE 1: LOAD
            profiler = DataProfiler(self.input_path).load_data()
            raw_df = profiler.df.copy()
            self._log_stage("1: LOAD", f"[DONE] Loaded {self.input_path.name}\n- {len(raw_df)} rows, {len(raw_df.columns)} columns")

            # STAGE 2: INITIAL VALIDATE (PRE-CLEANING)
            validator_raw = FintechGXValidator(raw_df, suite_name="raw_suite")
            results_raw = validator_raw.build_expectations().validate(self.report_dir / "validation_results_raw.txt")
            errors_before = results_raw.statistics['unsuccessful_expectations']
            self._log_stage("2: VALIDATE (RAW)", f"[DONE] Initial validation: {errors_before} failures")

            # STAGE 2: PROFILE
            profiler.run_full_analysis(output_report_path=self.report_dir / "data_profile_report.txt")

            # STAGE 3: CLEAN
            remediator = DataRemediator(raw_df)
            remediator.normalize_names().normalize_emails().normalize_phones().normalize_dates().handle_missing()
            cleaned_df = remediator.df
            self._log_stage("3: CLEAN", (
                f"[DONE] Normalized phone formats ({remediator.log_stats['phone_fixes']} rows)\n"
                f"[DONE] Normalized email formats ({remediator.log_stats['email_fixes']} rows)\n"
                f"[DONE] Normalized date formats ({remediator.log_stats['date_fixes']} rows)\n"
                f"[DONE] Fixed capitalization ({remediator.log_stats['name_fixes']} rows)\n"
                f"[DONE] Filled missing values ({sum(remediator.log_stats['missing_fills'].values())} rows)"
            ))

            # STAGE 4: RE-VALIDATE (POST-CLEANING)
            validator_clean = FintechGXValidator(cleaned_df, suite_name="cleaned_suite")
            results_clean = validator_clean.build_expectations().validate(self.report_dir / "validation_results.txt")
            errors_after = results_clean.statistics['unsuccessful_expectations']
            status = "PASS" if results_clean.success else "FAIL"
            self._log_stage("4: VALIDATE (CLEANED)", f"[{status}] post-cleaning validation: {errors_after} failures")

            # STAGE 5: GENERATE CLEANING LOG
            remediator.generate_log(
                output_path=self.report_dir / 'cleaning_log.txt',
                validation_before=errors_before,
                validation_after=errors_after
            )

            # STAGE 6: DETECT PII
            detector = PIIDetector(cleaned_df).scan_pii()
            detector.generate_report(self.report_dir / "pii_detection_report.txt")
            self._log_stage("6: DETECT PII", (
                f"[DONE] Scanned for PII patterns\n"
                f"- Emails found: {detector.risk_results['emails']}\n"
                f"- DOBs found: {detector.risk_results['dobs']}"
            ))

            # STAGE 7: MASK
            masker = DataMasker(cleaned_df)
            masker.mask_names().mask_emails().mask_phones().mask_addresses().mask_dob()
            final_df = masker.df
            masker.generate_masked_sample(raw_df, self.report_dir / "masked_sample.txt")
            self._log_stage("7: MASK", "[DONE] Masked all PII fields (Names, Emails, Phones, Address, DOB)")

            # STAGE 8: SAVE OUTPUTS
            cleaned_out = self.processed_dir / "customers_cleaned.csv"
            masked_out = self.processed_dir / "customers_masked.csv"
            
            cleaned_df.to_csv(cleaned_out, index=False, encoding='utf-8')
            final_df.to_csv(masked_out, index=False, encoding='utf-8')
            
            self._log_stage("8: SAVE", f"[DONE] Saved outputs:\n- {cleaned_out}\n- {masked_out}")

            # FINAL SUMMARY
            summary = (
                f"\nSUMMARY:\n"
                f"- Input: {len(raw_df)} rows\n"
                f"- Output: {len(final_df)} rows\n"
                f"- Quality Status: {status}\n"
                f"- Execution Status: SUCCESS"
            )
            self.execution_log.append(summary)

        except Exception as e:
            err_msg = f"[CRASH] Pipeline failed: {str(e)}"
            logger.error(err_msg)
            self.execution_log.append(f"\nSTATUS: FAILED\nError: {str(e)}")
        
        finally:
            self._write_final_report()

    def _write_final_report(self):
        report_path = self.report_dir / "pipeline_execution_report.txt"
        with open(report_path, "w", encoding='utf-8') as f:
            f.write("\n".join(self.execution_log))

if __name__ == "__main__":
    import os
    pipeline = FintechPipeline(
        input_path='data/raw/customers_raw.csv', 
        report_dir='data/reports/',
        processed_dir='data/processed/'
    )
    pipeline.run()
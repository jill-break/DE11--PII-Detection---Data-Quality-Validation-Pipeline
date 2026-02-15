import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# Importing custom modules
from src.utils.logger_config import setup_pipeline_logger
from src.part1.dataprofiler import DataProfiler
from src.part2.pii_detector import PIIDetector
from src.part3.data_validator import FintechGXValidator
from src.part4.cleaning import DataRemediator
from src.part5.data_masker import DataMasker

logger = setup_pipeline_logger(name="Orchestrator")

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
            self.df = profiler.df
            self._log_stage("1: LOAD", f"[DONE] Loaded {self.input_path.name}\n- {len(self.df)} rows, {len(self.df.columns)} columns")

            # STAGE 2: CLEAN
            remediator = DataRemediator(self.df)
            remediator.normalize_names().normalize_phones().normalize_dates().handle_missing()
            self.df = remediator.df
            self._log_stage("2: CLEAN", (
                f"[DONE] Normalized phone formats ({remediator.log_stats['phone_fixes']} rows)\n"
                f"[DONE] Normalized date formats ({remediator.log_stats['date_fixes']} rows)\n"
                f"[DONE] Fixed capitalization ({remediator.log_stats['name_fixes']} rows)\n"
                f"[DONE] Filled missing values ({sum(remediator.log_stats['missing_fills'].values())} rows)"
            ))

            # STAGE 3: VALIDATE
            validator = FintechGXValidator(self.df)
            results = validator.build_expectations().validate(self.report_dir / "validation_results.txt")
            status = "PASS" if results.success else "FAIL"
            self._log_stage("3: VALIDATE", f"[{status}] schema validation\n- Results saved to validation_results.txt")

            # STAGE 4: DETECT PII
            detector = PIIDetector(self.df).scan_pii()
            detector.generate_report(self.report_dir / "pii_detection_report.txt")
            self._log_stage("4: DETECT PII", (
                f"[DONE] Scanned for PII patterns:\n"
                f"- Emails: {detector.risk_results['emails']}\n"
                f"- Phones: {detector.risk_results['phones']}\n"
                f"- Addresses: {detector.risk_results['addresses']}\n"
                f"- DOBs: {detector.risk_results['dobs']}"
            ))

            # STAGE 5: MASK
            masker = DataMasker(self.df)
            masker.mask_names().mask_emails().mask_phones().mask_addresses().mask_dob()
            masked_df = masker.df
            self._log_stage("5: MASK", "[DONE] Masked all PII fields (Names, Emails, Phones, Addresses, DOBs)")

            # STAGE 6: SAVE
            output_file = self.processed_dir / "customers_masked.csv"
            masked_df.to_csv(output_file, index=False, encoding='utf-8')
            self._log_stage("6: SAVE", f"[DONE] Saved outputs:\n- {output_file}\n- Validation and PII reports generated.")

            # FINAL SUMMARY
            summary = (
                f"\nSUMMARY:\n"
                f"- Input: {len(self.df)} rows (raw)\n"
                f"- Output: {len(self.df)} rows (processed)\n"
                f"- Quality Status: {status}\n"
                f"- Execution Status: SUCCESS"
            )
            self.execution_log.append(summary)

        except Exception as e:
            err_msg = f"[CRASH] Pipeline failed: {str(e)}"
            logger.error(err_msg)
            self.execution_log.append(f"\nSTATUS: FAILED\nError: {str(e)}")
        
        finally:
            # Always attempt to write the report, even on failure
            self._write_final_report()

    def _write_final_report(self):
        """Writes the captured log to disk using UTF-8 to prevent Windows encoding errors."""
        report_path = self.report_dir / "pipeline_execution_report.txt"
        try:
            with open(report_path, "w", encoding='utf-8') as f:
                f.write("\n".join(self.execution_log))
            logger.info(f"Final execution report successfully written to {report_path}")
        except Exception as e:
            # Fallback for extreme cases
            print(f"FAILED TO WRITE REPORT: {str(e)}", file=sys.stderr)

if __name__ == "__main__":
    pipeline = FintechPipeline(
        input_path='data/raw/customers_raw.csv', 
        report_dir='data/reports/',
        processed_dir='../data/processed/'
    )
    pipeline.run()
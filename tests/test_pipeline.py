"""
Integration tests for main.py — FintechPipeline
"""
import pytest
import shutil
from pathlib import Path
from main import FintechPipeline


class TestFintechPipelineIntegration:
    """End-to-end integration tests for the full pipeline."""

    @pytest.fixture
    def pipeline_env(self, tmp_path):
        """
        Sets up a temporary environment with the real raw CSV
        and temporary output directories for the pipeline.
        """
        # Copy the real data file into the temp environment
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        src_csv = Path("data/raw/customers_raw.csv")
        if src_csv.exists():
            shutil.copy(src_csv, raw_dir / "customers_raw.csv")
        else:
            pytest.skip("Raw data file not available for integration test")

        report_dir = tmp_path / "data" / "reports"
        processed_dir = tmp_path / "data" / "processed"

        return {
            "input_path": str(raw_dir / "customers_raw.csv"),
            "report_dir": str(report_dir),
            "processed_dir": str(processed_dir),
        }

    def test_pipeline_runs_end_to_end(self, pipeline_env):
        """Full pipeline should complete without raising an exception."""
        pipeline = FintechPipeline(**pipeline_env)
        pipeline.run()  # Should not raise

    def test_masked_csv_created(self, pipeline_env):
        """Pipeline should produce a masked output CSV."""
        pipeline = FintechPipeline(**pipeline_env)
        pipeline.run()

        masked_file = Path(pipeline_env["processed_dir"]) / "customers_masked.csv"
        assert masked_file.exists(), "Masked CSV was not created"

    def test_reports_created(self, pipeline_env):
        """Pipeline should produce validation and PII report files."""
        pipeline = FintechPipeline(**pipeline_env)
        pipeline.run()

        report_dir = Path(pipeline_env["report_dir"])
        assert (report_dir / "validation_results.txt").exists()
        assert (report_dir / "pii_detection_report.txt").exists()
        assert (report_dir / "pipeline_execution_report.txt").exists()

    def test_execution_log_populated(self, pipeline_env):
        """Pipeline execution_log list should contain entries after run."""
        pipeline = FintechPipeline(**pipeline_env)
        pipeline.run()

        assert len(pipeline.execution_log) > 0
        full_log = "\n".join(pipeline.execution_log)
        assert "PIPELINE EXECUTION REPORT" in full_log
        assert "SUMMARY:" in full_log

    def test_pipeline_handles_missing_file_gracefully(self, tmp_path):
        """Pipeline should not crash when given a non-existent input file."""
        pipeline = FintechPipeline(
            input_path=str(tmp_path / "nonexistent.csv"),
            report_dir=str(tmp_path / "reports"),
            processed_dir=str(tmp_path / "processed"),
        )
        # Should not raise — error is caught internally
        pipeline.run()

        # The execution log should contain an error indicator
        full_log = "\n".join(pipeline.execution_log)
        assert "FAILED" in full_log or "Error" in full_log

"""
Tests for src/part1/dataprofiler.py — DataProfiler
"""
import pytest
import pandas as pd
from pathlib import Path
from src.part1.dataprofiler import DataProfiler


class TestDataProfilerLoadData:
    """Tests for loading CSV data."""

    def test_load_data_success(self, tmp_path):
        """load_data() should read a CSV and populate df + total_rows."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "customer_id,first_name,last_name\n1,John,Doe\n2,Jane,Smith\n"
        )
        profiler = DataProfiler(str(csv_file))
        result = profiler.load_data()

        assert profiler.df is not None
        assert profiler.total_rows == 2
        assert result is profiler  # returns self for chaining

    def test_load_data_file_not_found(self):
        """load_data() should raise FileNotFoundError for a missing path."""
        profiler = DataProfiler("non_existent_file.csv")
        with pytest.raises(FileNotFoundError):
            profiler.load_data()


class TestDataProfilerAnalysis:
    """Tests for the profiling analysis methods."""

    @pytest.fixture
    def loaded_profiler(self, sample_df, tmp_path):
        """Creates a DataProfiler with a pre-loaded DataFrame."""
        csv_path = tmp_path / "test_data.csv"
        sample_df.to_csv(csv_path, index=False)
        profiler = DataProfiler(str(csv_path))
        profiler.load_data()
        return profiler

    def test_check_completeness(self, loaded_profiler):
        """_check_completeness() should add COMPLETENESS entries to report_lines."""
        loaded_profiler._check_completeness()
        report_text = "\n".join(loaded_profiler.report_lines)
        assert "COMPLETENESS:" in report_text

    def test_check_data_types(self, loaded_profiler):
        """_check_data_types() should add DATA TYPES entries to report_lines."""
        loaded_profiler._check_data_types()
        report_text = "\n".join(loaded_profiler.report_lines)
        assert "DATA TYPES:" in report_text
        assert "customer_id" in report_text

    def test_check_quality_issues_phones(self, loaded_profiler):
        """_check_quality_issues() should flag non-standard phone formats."""
        loaded_profiler._check_quality_issues()
        report_text = "\n".join(loaded_profiler.report_lines)
        assert "QUALITY ISSUES:" in report_text
        assert "Phone" in report_text or "phone" in report_text.lower()

    def test_run_full_analysis_writes_report(self, loaded_profiler, tmp_path):
        """run_full_analysis() should write a complete report file to disk."""
        report_path = tmp_path / "reports" / "profile_report.txt"
        loaded_profiler.run_full_analysis(str(report_path))

        assert report_path.exists()
        content = report_path.read_text()
        assert "DATA QUALITY PROFILE REPORT" in content
        assert "COMPLETENESS:" in content
        assert "DATA TYPES:" in content
        assert "QUALITY ISSUES:" in content
        assert "SEVERITY:" in content

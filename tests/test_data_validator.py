"""
Tests for src/part3/data_validator.py — FintechGXValidator

NOTE: Great Expectations creates an in-memory context for each test,
so these tests run independently without needing external configuration.
"""
import pytest
import pandas as pd
from pathlib import Path
from src.part3.data_validator import FintechGXValidator


class TestFintechGXValidator:
    """Tests for the Great Expectations-based validator."""

    def test_build_expectations_returns_self(self, clean_df):
        """build_expectations() should return self for method chaining."""
        validator = FintechGXValidator(clean_df, suite_name="test_suite_chain")
        result = validator.build_expectations()
        assert result is validator

    def test_validate_clean_data_passes(self, clean_df, tmp_path):
        """A fully clean DataFrame should pass all GX expectations."""
        validator = FintechGXValidator(clean_df, suite_name="test_suite_clean")
        validator.build_expectations()

        report_path = tmp_path / "validation_report.txt"
        result = validator.validate(str(report_path))

        assert result.success is True

    def test_validate_dirty_data_fails(self, sample_df, tmp_path):
        """A DataFrame with quality issues should fail some expectations."""
        validator = FintechGXValidator(sample_df, suite_name="test_suite_dirty")
        validator.build_expectations()

        report_path = tmp_path / "validation_report.txt"
        result = validator.validate(str(report_path))

        assert result.success is False

    def test_report_file_created(self, clean_df, tmp_path):
        """validate() should write a report file at the given path."""
        validator = FintechGXValidator(clean_df, suite_name="test_suite_report")
        validator.build_expectations()

        report_path = tmp_path / "reports" / "validation_results.txt"
        validator.validate(str(report_path))

        assert report_path.exists()
        content = report_path.read_text()
        assert "VALIDATION RESULTS" in content

    def test_report_contains_statistics(self, sample_df, tmp_path):
        """Report for dirty data should list failures with column details."""
        validator = FintechGXValidator(sample_df, suite_name="test_suite_stats")
        validator.build_expectations()

        report_path = tmp_path / "validation_report.txt"
        validator.validate(str(report_path))

        content = report_path.read_text()
        assert "PASS:" in content
        assert "FAIL:" in content
        assert "SUCCESS RATE:" in content

"""
Tests for src/part2/pii_detector.py — PIIDetector
"""
import pytest
import pandas as pd
from pathlib import Path
from src.part2.pii_detector import PIIDetector


class TestPIIDetectorScan:
    """Tests for PII scanning logic."""

    def test_scan_pii_emails(self, sample_df):
        """scan_pii() should count all valid email addresses."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()
        # All 5 rows have valid emails
        assert detector.risk_results["emails"] == 5

    def test_scan_pii_phones(self, sample_df):
        """scan_pii() should count phone numbers matching the regex pattern."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()
        # All 5 rows have phone numbers that match the pattern
        assert detector.risk_results["phones"] == 5

    def test_scan_pii_addresses(self, sample_df):
        """scan_pii() should count non-null addresses with length > 5."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()
        # Row index 1 has a None address, so 4 valid addresses
        assert detector.risk_results["addresses"] == 4

    def test_scan_pii_dobs(self, sample_df):
        """scan_pii() should count valid DOBs (exclude invalid_date and NaN)."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()
        # Row index 2 has 'invalid_date', so 4 valid DOBs
        assert detector.risk_results["dobs"] == 4

    def test_scan_pii_returns_self(self, sample_df):
        """scan_pii() should return self for method chaining."""
        detector = PIIDetector(sample_df)
        result = detector.scan_pii()
        assert result is detector


class TestPIIDetectorReport:
    """Tests for PII report generation."""

    def test_generate_report_creates_file(self, sample_df, tmp_path):
        """generate_report() should write a report file to disk."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()

        report_path = tmp_path / "pii_report.txt"
        detector.generate_report(str(report_path))

        assert report_path.exists()

    def test_generate_report_contains_sections(self, sample_df, tmp_path):
        """Report should contain Risk Assessment, Detected PII, and Exposure Risk."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()

        report_path = tmp_path / "pii_report.txt"
        detector.generate_report(str(report_path))

        content = report_path.read_text()
        assert "RISK ASSESSMENT:" in content
        assert "DETECTED PII:" in content
        assert "EXPOSURE RISK:" in content
        assert "MITIGATION:" in content

    def test_generate_report_percentages(self, sample_df, tmp_path):
        """Report should include percentage calculations for each PII type."""
        detector = PIIDetector(sample_df)
        detector.scan_pii()

        report_path = tmp_path / "pii_report.txt"
        detector.generate_report(str(report_path))

        content = report_path.read_text()
        assert "Emails found:" in content
        assert "Phone numbers found:" in content
        assert "Addresses found:" in content
        assert "Dates of birth found:" in content

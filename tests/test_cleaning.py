"""
Tests for src/part4/cleaning.py — DataRemediator
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from src.part4.cleaning import DataRemediator


class TestNormalizeNames:
    """Tests for name normalization (title casing)."""

    def test_lowercase_names_are_title_cased(self, sample_df):
        """'jane' should become 'Jane'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_names()
        assert remediator.df["first_name"].iloc[1] == "Jane"

    def test_already_correct_names_unchanged(self, sample_df):
        """'John' should remain 'John'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_names()
        assert remediator.df["first_name"].iloc[0] == "John"

    def test_name_fixes_counted(self, sample_df):
        """log_stats should record the number of name changes."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_names()
        assert remediator.log_stats["name_fixes"] > 0

    def test_returns_self(self, sample_df):
        """normalize_names() should return self for chaining."""
        remediator = DataRemediator(sample_df)
        result = remediator.normalize_names()
        assert result is remediator


class TestNormalizePhones:
    """Tests for phone number standardization."""

    def test_parenthesized_phone_normalized(self, sample_df):
        """'(555) 234-5678' should become '555-234-5678'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_phones()
        assert remediator.df["phone"].iloc[2] == "555-234-5678"

    def test_dotted_phone_normalized(self, sample_df):
        """'555.345.6789' should become '555-345-6789'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_phones()
        assert remediator.df["phone"].iloc[3] == "555-345-6789"

    def test_continuous_digits_normalized(self, sample_df):
        """'5554567890' should become '555-456-7890'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_phones()
        assert remediator.df["phone"].iloc[4] == "555-456-7890"

    def test_already_formatted_phone_unchanged(self, sample_df):
        """'555-123-4567' should stay the same."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_phones()
        assert remediator.df["phone"].iloc[0] == "555-123-4567"

    def test_email_normalization(self, sample_df):
        """Verifies emails are converted to lowercase."""
        sample_df.loc[0, 'email'] = 'JOHN.DOE@GMAIL.COM'
        remediator = DataRemediator(sample_df)
        remediator.normalize_emails()
        assert remediator.df.loc[0, 'email'] == 'john.doe@gmail.com'
        assert remediator.log_stats['email_fixes'] >= 1

    def test_phone_fixes_counted(self, sample_df):
        """log_stats should record the number of phone normalizations."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_phones()
        assert remediator.log_stats["phone_fixes"] > 0


class TestNormalizeDates:
    """Tests for date standardization."""

    def test_slash_date_handled(self, sample_df):
        """'1975/05/10' should be converted to '1975-05-10'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_dates()
        result = remediator.df["date_of_birth"].iloc[3]
        assert result == "1975-05-10"

    def test_invalid_date_becomes_nat(self, sample_df):
        """'invalid_date' should become NaT (displayed as 'NaT')."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_dates()
        assert remediator.df["date_of_birth"].iloc[2] == "NaT" or pd.isna(
            remediator.df["date_of_birth"].iloc[2]
        )

    def test_valid_iso_date_unchanged(self, sample_df):
        """'1985-03-15' should remain '1985-03-15'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_dates()
        assert remediator.df["date_of_birth"].iloc[0] == "1985-03-15"

    def test_date_fixes_counted(self, sample_df):
        """log_stats should record the number of date format changes."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_dates()
        assert remediator.log_stats["date_fixes"] > 0


class TestHandleMissing:
    """Tests for missing value imputation."""

    def test_missing_name_filled_with_unknown(self, sample_df):
        """NaN first_name should be filled with '[UNKNOWN]'."""
        # Row 2 in sample_df has a missing first_name (None)
        remediator = DataRemediator(sample_df)
        remediator.normalize_names().handle_missing()
        assert remediator.df["first_name"].iloc[2] == "[UNKNOWN]"

    def test_missing_income_filled_with_zero(self, sample_df):
        """NaN income should be filled with 0."""
        remediator = DataRemediator(sample_df)
        remediator.handle_missing()
        assert remediator.df["income"].iloc[2] == 0

    def test_missing_status_filled_with_unknown(self, sample_df):
        """NaN account_status should be filled with 'unknown'."""
        remediator = DataRemediator(sample_df)
        remediator.handle_missing()
        assert remediator.df["account_status"].iloc[3] == "unknown"

    def test_missing_fills_tracked(self, sample_df):
        """log_stats['missing_fills'] should contain counts per column."""
        remediator = DataRemediator(sample_df)
        remediator.handle_missing()
        assert "income" in remediator.log_stats["missing_fills"]
        assert "account_status" in remediator.log_stats["missing_fills"]


    def test_missing_date_filled_with_unknown(self, sample_df):
        """NaN or invalid date_of_birth should be filled with '[UNKNOWN]'."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_dates().handle_missing()
        # 'invalid_date' at index 2 should become '[UNKNOWN]'
        assert remediator.df["date_of_birth"].iloc[2] == "[UNKNOWN]"
        # '01/15/2024' at index 4 should become '2024-01-15'
        assert remediator.df["created_date"].iloc[4] == "2024-01-15"

class TestGenerateLog:
    """Tests for cleaning log output."""

    def test_generate_log_creates_file(self, sample_df, tmp_path):
        """generate_log() should write a log file to disk."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_names().normalize_phones().normalize_dates().handle_missing()

        log_path = tmp_path / "cleaning_log.txt"
        remediator.generate_log(str(log_path), validation_before=5, validation_after=0)

        assert log_path.exists()

    def test_generate_log_contents(self, sample_df, tmp_path):
        """Log should contain actions taken and validation summary."""
        remediator = DataRemediator(sample_df)
        remediator.normalize_names().normalize_phones().normalize_dates().handle_missing()

        log_path = tmp_path / "cleaning_log.txt"
        remediator.generate_log(str(log_path), validation_before=5, validation_after=0)

        content = log_path.read_text()
        assert "DATA CLEANING LOG" in content
        assert "ACTIONS TAKEN:" in content
        assert "Phone format:" in content
        assert "Date format:" in content
        assert "Name case:" in content
        assert "Status: PASS" in content


class TestMethodChaining:
    """Tests for the fluent API pattern."""

    def test_full_chain(self, sample_df):
        """All normalization methods should be chainable."""
        remediator = DataRemediator(sample_df)
        result = (
            remediator
            .normalize_names()
            .normalize_phones()
            .normalize_dates()
            .handle_missing()
        )
        assert result is remediator

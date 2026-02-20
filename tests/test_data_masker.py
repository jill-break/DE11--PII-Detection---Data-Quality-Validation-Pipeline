"""
Tests for src/part5/data_masker.py — DataMasker
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from src.part5.data_masker import DataMasker


@pytest.fixture
def masker_df():
    """
    Returns a pre-cleaned DataFrame suitable for masking tests.
    All fields are in their expected cleaned format.
    """
    return pd.DataFrame({
        "customer_id": [1, 2, 3],
        "first_name": ["John", "Jane", "[UNKNOWN]"],
        "last_name": ["Doe", "Smith", "Johnson"],
        "email": ["john.doe@gmail.com", "jane.smith@company.com", "bob@email.com"],
        "phone": ["555-123-4567", "555-987-6543", "555-234-5678"],
        "date_of_birth": ["1985-03-15", "1990-07-22", "1988-11-08"],
        "address": [
            "123 Main St New York NY 10001",
            "456 Oak Ave Los Angeles CA 90001",
            "789 Pine Rd Chicago IL 60601",
        ],
        "income": [75000, 95000, 60000],
        "account_status": ["active", "inactive", "suspended"],
        "created_date": ["2024-01-10", "2024-01-11", "2024-01-12"],
    })


class TestMaskNames:
    """Tests for name masking."""

    def test_first_name_masked(self, masker_df):
        """'John' should become 'J***'."""
        masker = DataMasker(masker_df)
        masker.mask_names()
        assert masker.df["first_name"].iloc[0] == "J***"

    def test_last_name_masked(self, masker_df):
        """'Doe' should become 'D***'."""
        masker = DataMasker(masker_df)
        masker.mask_names()
        assert masker.df["last_name"].iloc[0] == "D***"

    def test_unknown_name_preserved(self, masker_df):
        """'[UNKNOWN]' names should NOT be masked."""
        masker = DataMasker(masker_df)
        masker.mask_names()
        assert masker.df["first_name"].iloc[2] == "[UNKNOWN]"

    def test_returns_self(self, masker_df):
        """mask_names() should return self for chaining."""
        masker = DataMasker(masker_df)
        assert masker.mask_names() is masker


class TestMaskEmails:
    """Tests for email masking."""

    def test_email_masked(self, masker_df):
        """'john.doe@gmail.com' should become 'j***@gmail.com'."""
        masker = DataMasker(masker_df)
        masker.mask_emails()
        assert masker.df["email"].iloc[0] == "j***@gmail.com"

    def test_domain_preserved(self, masker_df):
        """The domain part of the email should remain intact."""
        masker = DataMasker(masker_df)
        masker.mask_emails()
        assert masker.df["email"].iloc[1].endswith("@company.com")

    def test_nan_email_handled(self, masker_df):
        """NaN emails should not cause errors."""
        masker_df.loc[0, "email"] = np.nan
        masker = DataMasker(masker_df)
        masker.mask_emails()
        assert pd.isna(masker.df["email"].iloc[0])

    def test_returns_self(self, masker_df):
        """mask_emails() should return self for chaining."""
        masker = DataMasker(masker_df)
        assert masker.mask_emails() is masker


class TestMaskPhones:
    """Tests for phone masking."""

    def test_phone_masked(self, masker_df):
        """'555-123-4567' should become '***-***-4567'."""
        masker = DataMasker(masker_df)
        masker.mask_phones()
        assert masker.df["phone"].iloc[0] == "***-***-4567"

    def test_last_four_preserved(self, masker_df):
        """The last 4 digits should be visible."""
        masker = DataMasker(masker_df)
        masker.mask_phones()
        assert masker.df["phone"].iloc[1].endswith("6543")

    def test_returns_self(self, masker_df):
        """mask_phones() should return self for chaining."""
        masker = DataMasker(masker_df)
        assert masker.mask_phones() is masker


class TestMaskAddresses:
    """Tests for address masking."""

    def test_all_addresses_masked(self, masker_df):
        """Every address should become '[MASKED ADDRESS]'."""
        masker = DataMasker(masker_df)
        masker.mask_addresses()
        assert (masker.df["address"] == "[MASKED ADDRESS]").all()

    def test_returns_self(self, masker_df):
        """mask_addresses() should return self for chaining."""
        masker = DataMasker(masker_df)
        assert masker.mask_addresses() is masker


class TestMaskDob:
    """Tests for date of birth masking."""

    def test_dob_masked(self, masker_df):
        """'1985-03-15' should become '1985-**-**'."""
        masker = DataMasker(masker_df)
        masker.mask_dob()
        assert masker.df["date_of_birth"].iloc[0] == "1985-**-**"

    def test_year_preserved(self, masker_df):
        """The year portion should remain visible."""
        masker = DataMasker(masker_df)
        masker.mask_dob()
        assert masker.df["date_of_birth"].iloc[1].startswith("1990")

    def test_returns_self(self, masker_df):
        """mask_dob() should return self for chaining."""
        masker = DataMasker(masker_df)
        assert masker.mask_dob() is masker


class TestSaveAndSample:
    """Tests for file output methods."""

    def test_save_masked_data(self, masker_df, tmp_path):
        """save_masked_data() should write a CSV file."""
        masker = DataMasker(masker_df)
        masker.mask_names().mask_emails().mask_phones().mask_addresses().mask_dob()

        output_path = tmp_path / "masked_output.csv"
        masker.save_masked_data(str(output_path))

        assert output_path.exists()
        saved_df = pd.read_csv(output_path)
        assert len(saved_df) == 3

    def test_generate_masked_sample(self, masker_df, tmp_path):
        """generate_masked_sample() should create a before/after comparison file."""
        original = masker_df.copy()
        masker = DataMasker(masker_df)
        masker.mask_names().mask_emails().mask_phones().mask_addresses().mask_dob()

        sample_path = tmp_path / "masked_sample.txt"
        masker.generate_masked_sample(original, str(sample_path))

        assert sample_path.exists()
        content = sample_path.read_text()
        assert "BEFORE MASKING" in content
        assert "AFTER MASKING" in content
        assert "ANALYSIS:" in content


class TestFullMaskingChain:
    """Tests for the complete masking pipeline."""

    def test_full_chain(self, masker_df):
        """All masking methods should be chainable in a fluent API."""
        masker = DataMasker(masker_df)
        result = (
            masker
            .mask_names()
            .mask_emails()
            .mask_phones()
            .mask_addresses()
            .mask_dob()
        )
        assert result is masker

    def test_original_df_not_mutated(self, masker_df):
        """DataMasker should work on a copy and not mutate the original."""
        original_name = masker_df["first_name"].iloc[0]
        masker = DataMasker(masker_df)
        masker.mask_names()
        assert masker_df["first_name"].iloc[0] == original_name

"""
Shared pytest fixtures for the Fintech PII Detection & Data Quality Pipeline.
"""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_df():
    """
    Returns a small DataFrame matching the customers_raw.csv schema.
    Includes deliberate quality issues (missing values, invalid_date,
    inconsistent phone formats, bad casing) for thorough testing.
    """
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "first_name": ["John", "jane", None, "Mary", "Robert"],
        "last_name": ["Doe", "Smith", "Johnson", "Brown", None],
        "email": [
            "john.doe@gmail.com",
            "jane.smith@company.com",
            "bob.johnson@email.com",
            "mary.brown@gmail.com",
            "robert.wilson@yahoo.com",
        ],
        "phone": [
            "555-123-4567",
            "555-987-6543",
            "(555) 234-5678",
            "555.345.6789",
            "5554567890",
        ],
        "date_of_birth": [
            "1985-03-15",
            "1990-07-22",
            "invalid_date",
            "1975/05/10",
            "2005-12-25",
        ],
        "address": [
            "123 Main St New York NY 10001",
            None,
            "456 Oak Ave Los Angeles CA 90001",
            "789 Pine Rd Chicago IL 60601",
            "892 Elm St Houston TX 77001",
        ],
        "income": [75000, 95000, np.nan, 120000, 55000],
        "account_status": ["active", "active", "suspended", None, "active"],
        "created_date": [
            "2024-01-10",
            "2024-01-11",
            "2024-01-12",
            "2024-01-13",
            "01/15/2024",
        ],
    })


@pytest.fixture
def clean_df():
    """
    Returns a fully clean DataFrame that should pass all GX expectations.
    No nulls, proper formats, valid statuses.
    """
    return pd.DataFrame({
        "customer_id": [1, 2, 3],
        "first_name": ["John", "Jane", "Bob"],
        "last_name": ["Doe", "Smith", "Johnson"],
        "email": [
            "john.doe@gmail.com",
            "jane.smith@company.com",
            "bob.johnson@email.com",
        ],
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


# Fintech Customer Ingestion & PII Protection Pipeline
A Python pipeline designed to ingest, clean, validate, and anonymize sensitive customer data for financial analytics. This project demonstrates the transition from "messy" raw data to a GDPR-compliant "Golden Record."

### 1. The Repository Structure
```
fintech-ingestion-pipeline/
├── data/
│   ├── raw/                # Original messy CSV
│   ├── processed/          # Final masked/cleaned CSV
│   └── reports/            # All generated .txt reports
├── src/
│   ├── __init__.py
│   ├── part1/              # Profiling logic
│   ├── part2/              # PII detection
│   ├── part3/              # GX Validation
│   ├── part4/              # Cleaning/Remediation
│   ├── part5/              # Masking logic
│   └── utils/
│       └── logger_config.py
├── main.py                 # The Orchestrator
├── requirements.txt        # Dependencies
├── .gitignore              # Files to keep out of GitHub (e.g., .venv, logs)
└── README.md               # The project documentation

```
## Key Features
- **Automated Data Profiling:** Identifies missing values, data type inconsistencies, and structural issues.
- **PII Detection:** Scans data for sensitive patterns (Emails, Phones, DOBs, Addresses) using Regex.
- **Strict Validation:** Uses **Great Expectations 1.x** to enforce a data contract (e.g., Income <= $10M).
- **Automated Remediation:** Standardizes dates, phone formats, and handles missing values.
- **Data Masking:** Anonymizes PII (e.g., `John Doe` -> `J*** D***`) to preserve analytics utility while ensuring privacy.

## Tech Stack
- **Language:** Python 3.10+
- **Data Logic:** Pandas
- **Validation Framework:** Great Expectations (GX)
- **Logging:** Python Logging Module
- **Environment:** Windows/Linux compatible (UTF-8 Enforced)

## Installation & Setup
1. Clone the repository:
   ```bash
   git clone [https://github.com/jill-break/DE11--PII-Detection---Data-Quality-Validation-Pipeline.git]
```
```
2.  Create and activate a virtual environment:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # Windows: .venv\Scripts\activate
    ```
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
## Running the Pipeline
To execute the end-to-end workflow and generate reports:
```bash
python main.py
```
##  Deliverables Generated

-   `customers_masked.csv`: The finalized, safe-to-share dataset.
    
-   `pipeline_execution_report.txt`: A summary of the orchestration stages.
    
-   `pii_detection_report.txt`: Risk assessment of sensitive data found.
    
-   `validation_results.txt`: Forensic audit of data quality failures.
---
### 4. The `requirements.txt`

Ensure everyone running your code has the same library versions.
```
pandas>=2.0.0
great-expectations>=1.0.0
numpy
pathlib

```
    
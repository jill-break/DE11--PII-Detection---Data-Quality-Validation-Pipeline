import great_expectations as gx
import pandas as pd
from pathlib import Path
from src.utils.logger_config import setup_pipeline_logger

logger = setup_pipeline_logger(name="GXValidator_V1")

class FintechGXValidator:
    def __init__(self, df: pd.DataFrame, suite_name: str = "fintech_suite"):
        self.df = df
        self.suite_name = suite_name
        self.context = gx.get_context()
        
        # Setup the Suite
        self.suite = self.context.suites.add(
            gx.ExpectationSuite(name=self.suite_name)
        )
        
    def build_expectations(self):
        logger.info(f"Building Strict Expectations for suite: {self.suite_name}")
        
        expectations = [
            # 1. ID: Must be unique, positive, and NOT NULL
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="customer_id", min_value=1),
            
            # 2. Names: Must NOT BE NULL + Length + Regex
            gx.expectations.ExpectColumnValuesToNotBeNull(column="first_name"),
            gx.expectations.ExpectColumnValueLengthsToBeBetween(column="first_name", min_value=2, max_value=50),
            gx.expectations.ExpectColumnValuesToMatchRegex(column="first_name", regex=r"^[a-zA-Z\s]+$"),

            gx.expectations.ExpectColumnValuesToNotBeNull(column="last_name"),
            gx.expectations.ExpectColumnValueLengthsToBeBetween(column="last_name", min_value=2, max_value=50),
            
            # 3. Income: Must NOT BE NULL + Range
            gx.expectations.ExpectColumnValuesToNotBeNull(column="income"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="income", min_value=0, max_value=10_000_000),
            
            # 4. Account Status: Must NOT BE NULL + Set
            gx.expectations.ExpectColumnValuesToNotBeNull(column="account_status"),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="account_status", 
                value_set=["active", "inactive", "suspended"]
            ),
            
            # 5. Dates: Valid Format
            gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
                column="date_of_birth", strftime_format="%Y-%m-%d"
            ),
            gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
                column="created_date", strftime_format="%Y-%m-%d"
            )
        ]
        
        for exp in expectations:
            self.suite.add_expectation(exp)
            
        return self

    def validate(self, report_path: str):
        logger.info("Starting GX 1.x Validation execution...")
        
        # Setup Data Source & Asset (Fluent API)
        # Using a unique name to avoid collisions if re-run in a notebook
        ds_name = f"pandas_datasource_{pd.Timestamp.now().strftime('%M%S')}"
        datasource = self.context.data_sources.add_pandas(name=ds_name)
        asset = datasource.add_dataframe_asset(name="raw_customers")
        
        # Create Batch Definition & Validation Definition
        batch_definition = asset.add_batch_definition_whole_dataframe("all_rows")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": self.df})
        
        validation_definition = self.context.validation_definitions.add(
            gx.ValidationDefinition(
                name="fintech_validation_def",
                data=batch_definition,
                suite=self.suite
            )
        )
        
        # Run Validation
        result = validation_definition.run(batch_parameters={"dataframe": self.df})
        
        # Extract results for the deliverable
        self._generate_custom_report(result, report_path)
        
        return result

    def _generate_custom_report(self, result, report_path):
        """Forensic mapping of GX results to the required deliverable format."""
        stats = result.statistics
        # Calculate row-level failure estimation (GX doesn't give this directly, so we infer)
        failed_indices = set()
        
        report = [
            "VALIDATION RESULTS (GX 1.x)",
            "==================",
            f"PASS: {stats['successful_expectations']} expectations met",
            f"FAIL: {stats['unsuccessful_expectations']} expectations failed",
            f"SUCCESS RATE: {stats['success_percent']:.2f}%",
            "",
            "DETAILED FAILURES BY COLUMN:",
            "-------------------"
        ]
        
        for res in result.results:
            if not res.success:
                col = res.expectation_config.kwargs.get('column', 'Table-Level')
                # Get the actual indices of the bad rows
                indices = res.result.get('unexpected_index_list', [])
                # Convert to 1-based indexing for business reporting
                human_indices = [idx + 1 for idx in indices]
                
                # Tracking unique failed rows for the summary
                failed_indices.update(human_indices)
                
                report.append(f"{col}:")
                report.append(f"- Issue: {res.expectation_config.type}")
                
                if human_indices:
                    # Format as: Row 3, Row 5, Row 10
                    rows_str = ", ".join([f"Row {i}" for i in human_indices[:10]])
                    report.append(f"- Failed Rows: {rows_str}")
                
                # Show the actual bad data found in those rows
                bad_values = res.result.get('partial_unexpected_list', [])
                if bad_values:
                    report.append(f"- Sample Values: {bad_values[:3]}")
                report.append("")

        # Add a summary of total impacted rows
        report.insert(5, f"IMPACT: {len(failed_indices)} distinct rows failed at least one check.")

        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w') as f:
            f.write("\n".join(report))
            
        logger.info(f"Forensic GX Report saved to {report_path}")
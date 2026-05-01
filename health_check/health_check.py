#!/usr/bin/env python3
"""
Health Check Script for project_voltstream Gold Tables

This script performs comprehensive health checks on the gold layer tables to ensure:
1. Tables are not empty (contain data)
2. No critical NULL values in key columns
3. Data freshness (recent ingestion timestamps)
4. Data quality metrics are within acceptable ranges
5. No duplicate records in dimension tables

Usage:
    python health_check.py

Can be scheduled as a Databricks job to run periodically.
"""

from logger.custom_logging import set_up_logger, get_job_logger
from utils.shared import notebook_exit
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
import logging

# Add project root to path
sys.path.insert(0, "/Workspace/Users/nseekeminiudo@gmail.com/project_voltstream")


class HealthCheckStatus:
    """Health check status tracking"""

    def __init__(self):
        self.passed = []
        self.failed = []
        self.warnings = []

    def add_pass(self, check_name, message):
        self.passed.append((check_name, message))

    def add_fail(self, check_name, message):
        self.failed.append((check_name, message))

    def add_warning(self, check_name, message):
        self.warnings.append((check_name, message))

    def is_healthy(self):
        return len(self.failed) == 0

    def summary(self):
        return {
            "passed": len(self.passed),
            "failed": len(self.failed),
            "warnings": len(self.warnings),
            "healthy": self.is_healthy(),
        }


class GoldTableHealthCheck:
    """Health check for gold layer tables"""

    def __init__(self, spark, catalog="gold_dev", schema="electrovolt"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.status = HealthCheckStatus()

        # Set up logging
        self.logger = set_up_logger()
        self.log_info = {
            "layer": "gold",
            "job": "health_check",
            "dataset": f"{catalog}.{schema}",
        }
        try:
            run_id = spark.conf.get(
                "spark.databricks.clusterUsageTags.runId",
                spark.conf.get("pipeline_run_id", "manual_run"),
            )
        except Exception:
            run_id = "manual_run"

        self.log = get_job_logger(self.logger, **self.log_info, run_id=run_id)

        # Define tables to check
        self.tables = {
            "station_facts": f"{catalog}.{schema}.station_facts",
            "station_dim": f"{catalog}.{schema}.station_dim",
            "weather_dim": f"{catalog}.{schema}.weather_dim",
        }

    def check_table_exists(self, table_name, full_table_name):
        """Check if table exists"""
        check_name = f"Table Exists: {table_name}"
        try:
            exists = self.spark.catalog.tableExists(full_table_name)
            if exists:
                self.status.add_pass(check_name, f"Table {full_table_name} exists")
                self.log(logging.INFO, f"✓ {check_name}")
                return True
            else:
                self.status.add_fail(
                    check_name, f"Table {full_table_name} does not exist"
                )
                self.log(logging.ERROR, f"✗ {check_name}")
                return False
        except Exception as e:
            self.status.add_fail(
                check_name, f"Error checking table existence: {str(e)}"
            )
            self.log(logging.ERROR, f"✗ {check_name}: {str(e)}")
            return False

    def check_table_not_empty(self, table_name, full_table_name):
        """Check if table contains data"""
        check_name = f"Table Not Empty: {table_name}"
        try:
            df = self.spark.table(full_table_name)
            row_count = df.count()

            if row_count > 0:
                self.status.add_pass(check_name, f"Table has {row_count:,} rows")
                self.log(logging.INFO, f"✓ {check_name}: {row_count:,} rows")
                return True
            else:
                self.status.add_fail(check_name, f"Table {full_table_name} is EMPTY")
                self.log(logging.ERROR, f"✗ {check_name}: Table is EMPTY")
                return False
        except Exception as e:
            self.status.add_fail(check_name, f"Error checking row count: {str(e)}")
            self.log(logging.ERROR, f"✗ {check_name}: {str(e)}")
            return False

    def check_null_values(self, table_name, full_table_name, critical_columns):
        """Check for NULL values in critical columns"""
        check_name = f"NULL Values Check: {table_name}"
        try:
            df = self.spark.table(full_table_name)
            total_rows = df.count()

            null_issues = []
            for col_name in critical_columns:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    null_percentage = (
                        (null_count / total_rows * 100) if total_rows > 0 else 0
                    )

                    if null_count > 0:
                        null_issues.append(
                            f"{col_name}: {null_count} NULLs ({null_percentage:.2f}%)"
                        )

            if len(null_issues) == 0:
                self.status.add_pass(check_name, f"No NULL values in critical columns")
                self.log(logging.INFO, f"✓ {check_name}")
                return True
            else:
                self.status.add_fail(
                    check_name, f"NULL values found: {', '.join(null_issues)}"
                )
                self.log(logging.ERROR, f"✗ {check_name}: {', '.join(null_issues)}")
                return False
        except Exception as e:
            self.status.add_fail(check_name, f"Error checking NULL values: {str(e)}")
            self.log(logging.ERROR, f"✗ {check_name}: {str(e)}")
            return False

    def check_data_freshness(self, table_name, full_table_name, max_age_hours=48):
        """Check if data has been recently ingested"""
        check_name = f"Data Freshness: {table_name}"
        try:
            df = self.spark.table(full_table_name)

            if "ingest_timestamp" not in df.columns:
                self.status.add_warning(
                    check_name, f"No ingest_timestamp column in {table_name}"
                )
                self.log(logging.WARNING, f"⚠ {check_name}: No ingest_timestamp column")
                return True

            max_timestamp = df.select(spark_max("ingest_timestamp")).collect()[0][0]

            if max_timestamp is None:
                self.status.add_fail(check_name, f"No timestamp data in {table_name}")
                self.log(logging.ERROR, f"✗ {check_name}: No timestamp data")
                return False

            age_hours = (datetime.now() - max_timestamp).total_seconds() / 3600

            if age_hours <= max_age_hours:
                self.status.add_pass(
                    check_name, f"Data is fresh ({age_hours:.1f} hours old)"
                )
                self.log(logging.INFO, f"✓ {check_name}: {age_hours:.1f} hours old")
                return True
            else:
                self.status.add_warning(
                    check_name,
                    f"Data is stale ({age_hours:.1f} hours old, max: {max_age_hours})",
                )
                self.log(logging.WARNING, f"⚠ {check_name}: {age_hours:.1f} hours old")
                return True
        except Exception as e:
            self.status.add_fail(check_name, f"Error checking data freshness: {str(e)}")
            self.log(logging.ERROR, f"✗ {check_name}: {str(e)}")
            return False

    def check_data_quality_metrics(self, table_name, full_table_name):
        """Check data quality metrics specific to each table"""
        check_name = f"Data Quality Metrics: {table_name}"

        try:
            df = self.spark.table(full_table_name)

            if table_name == "station_facts":
                # Check risk scores are in valid range (0-100+)
                if "risk_score" in df.columns:
                    invalid_risk = df.filter(
                        (col("risk_score") < 0) | (col("risk_score") > 100)
                    ).count()
                    if invalid_risk > 0:
                        self.status.add_warning(
                            check_name,
                            f"{invalid_risk} records with invalid risk_score",
                        )
                        self.log(
                            logging.WARNING,
                            f"⚠ {check_name}: {invalid_risk} invalid risk scores",
                        )

                # Check availability is between 0 and 1
                if "availability" in df.columns:
                    invalid_avail = df.filter(
                        (col("availability") < 0) | (col("availability") > 1)
                    ).count()
                    if invalid_avail > 0:
                        self.status.add_warning(
                            check_name,
                            f"{invalid_avail} records with invalid availability",
                        )
                        self.log(
                            logging.WARNING,
                            f"⚠ {check_name}: {invalid_avail} invalid availability",
                        )

            elif table_name == "station_dim":
                # Check for duplicate station_ids
                total_count = df.count()
                distinct_count = df.select("station_id").distinct().count()
                if total_count != distinct_count:
                    duplicates = total_count - distinct_count
                    self.status.add_fail(
                        check_name, f"{duplicates} duplicate station_ids found"
                    )
                    self.log(logging.ERROR, f"✗ {check_name}: {duplicates} duplicates")
                    return False

            elif table_name == "weather_dim":
                # Check temperature is in reasonable range (-50 to 150
                # Fahrenheit)
                if "temp" in df.columns:
                    invalid_temp = df.filter(
                        (col("temp") < -50) | (col("temp") > 150)
                    ).count()
                    if invalid_temp > 0:
                        self.status.add_warning(
                            check_name,
                            f"{invalid_temp} records with invalid temperature",
                        )
                        self.log(
                            logging.WARNING,
                            f"⚠ {check_name}: {invalid_temp} invalid temperatures",
                        )

            self.status.add_pass(
                check_name, f"Data quality metrics passed for {table_name}"
            )
            self.log(logging.INFO, f"✓ {check_name}")
            return True

        except Exception as e:
            self.status.add_fail(check_name, f"Error checking data quality: {str(e)}")
            self.log(logging.ERROR, f"✗ {check_name}: {str(e)}")
            return False

    def run_all_checks(self):
        """Run all health checks for all tables"""
        self.log(logging.INFO, "=" * 60)
        self.log(logging.INFO, "Starting Gold Table Health Checks")
        self.log(logging.INFO, "=" * 60)

        for table_name, full_table_name in self.tables.items():
            self.log(logging.INFO, f"\nChecking table: {table_name}")
            self.log(logging.INFO, "-" * 40)

            # Check if table exists
            if not self.check_table_exists(table_name, full_table_name):
                continue  # Skip other checks if table doesn't exist

            # Check if table is not empty
            if not self.check_table_not_empty(table_name, full_table_name):
                continue  # Skip other checks if table is empty

            # Define critical columns for each table
            critical_columns = {
                "station_facts": [
                    "is_operational",
                    "risk_score",
                    "avg_energy_cost_per_hour",
                ],
                "station_dim": ["station_id", "latitude", "longitude"],
                "weather_dim": ["weather_sk", "weather", "temp"],
            }

            # Check for NULL values in critical columns
            self.check_null_values(
                table_name, full_table_name, critical_columns.get(table_name, [])
            )

            # Check data freshness
            self.check_data_freshness(table_name, full_table_name)

            # Check data quality metrics
            self.check_data_quality_metrics(table_name, full_table_name)

        # Print summary
        self.print_summary()

        return self.status.is_healthy()

    def print_summary(self):
        """Print health check summary"""
        summary = self.status.summary()

        self.log(logging.INFO, "\n" + "=" * 60)
        self.log(logging.INFO, "HEALTH CHECK SUMMARY")
        self.log(logging.INFO, "=" * 60)
        self.log(logging.INFO, f"✓ Passed: {summary['passed']}")
        self.log(logging.INFO, f"✗ Failed: {summary['failed']}")
        self.log(logging.INFO, f"⚠ Warnings: {summary['warnings']}")
        self.log(logging.INFO, "=" * 60)

        if summary["healthy"]:
            self.log(logging.INFO, "✓ OVERALL STATUS: HEALTHY")
        else:
            self.log(logging.ERROR, "✗ OVERALL STATUS: UNHEALTHY")
            self.log(logging.ERROR, "\nFailed Checks:")
            for check_name, message in self.status.failed:
                self.log(logging.ERROR, f"  - {check_name}: {message}")

        if len(self.status.warnings) > 0:
            self.log(logging.WARNING, "\nWarnings:")
            for check_name, message in self.status.warnings:
                self.log(logging.WARNING, f"  - {check_name}: {message}")

        self.log(logging.INFO, "=" * 60)


def main():
    """Main execution function"""
    # Initialize Spark session
    spark = SparkSession.builder.appName("GoldTableHealthCheck").getOrCreate()

    try:
        # Run health checks
        health_checker = GoldTableHealthCheck(spark)
        is_healthy = health_checker.run_all_checks()

        # Exit with appropriate status code
        if is_healthy:
            print("\n✓ All health checks passed!")
            notebook_exit("SUCCESS")
        else:
            print("\n✗ Some health checks failed. See logs for details.")
            notebook_exit("FAILED")
    except Exception as e:
        print(f"\n✗ Health check script failed with error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(2)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

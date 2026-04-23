# Gold Layer Health Check

Production-ready health check system for `project_voltstream` gold layer tables. Validates data quality, freshness, and integrity to ensure downstream analytics consume accurate, timely data.

## Overview

The health check script (`health_check.py`) performs comprehensive validation on gold layer tables:

1. **Data Availability** - Tables exist and contain data
2. **Data Integrity** - No NULL values in critical columns  
3. **Data Freshness** - Recent data ingestion (< 48 hours by default)
4. **Data Quality** - Metrics within acceptable ranges
5. **No Duplicates** - Unique records in dimension tables
6. **Business Rules** - Risk scores, availability percentages validated

**Use Cases:**
* Run as final task in Databricks Workflows (fails job if unhealthy)
* Schedule as standalone monitoring job (hourly/daily)
* Manual execution for ad-hoc data quality audits
* CI/CD pipeline validation gate

---

## Health Check Categories

### 1. Table Existence Check

**What it validates:**
* Tables exist in the catalog/schema
* Tables are accessible (permissions)

**Fails if:**
* Table does not exist in `gold_dev.electrovolt` catalog

**Why it matters:**
* Prevents downstream errors from missing tables
* Early detection of dropped or renamed tables

---

### 2. Non-Empty Table Check

**What it validates:**
* Tables contain at least one row
* Data has been loaded successfully

**Fails if:**
* `row_count == 0`

**Why it matters:**
* Empty tables indicate pipeline failures
* Analytics queries on empty tables produce misleading results

---

### 3. NULL Values Check

**What it validates:**
* Critical columns contain no NULL values

**Critical columns by table:**

| Table | Critical Columns |
|-------|------------------|
| `station_facts` | `is_operational`, `risk_score`, `avg_energy_cost_per_hour` |
| `station_dim` | `station_id`, `latitude`, `longitude` |
| `weather_dim` | `weather_sk`, `weather`, `temp` |

**Fails if:**
* Any critical column has NULL values
* Reports: `{col_name}: {count} NULLs ({percentage}%)`

**Why it matters:**
* NULLs in key columns break joins and aggregations
* Business logic depends on these fields

---

### 4. Data Freshness Check

**What it validates:**
* Maximum `ingest_timestamp` is recent (default: < 48 hours)

**Warnings if:**
* Data age > 48 hours (configurable threshold)

**Fails if:**
* No `ingest_timestamp` data exists

**Why it matters:**
* Stale data leads to outdated analytics
* Indicates pipeline scheduling issues

---

### 5. Data Quality Metrics

#### station_facts Validation

**Risk Score:**
* Valid range: 0 - 100
* Warning if: `risk_score < 0 OR risk_score > 100`
* Business logic: Base 10 + weather (40) + temp (25) + charging level (25) = max ~100

**Availability:**
* Valid range: 0.0 - 1.0 (percentage)
* Warning if: `availability < 0 OR availability > 1`
* Business logic: `1 - (offline_duration / total_duration)`

#### station_dim Validation

**Duplicate Detection:**
* Ensures `station_id` is unique
* Fails if: `total_count != distinct_count`
* Why: `station_id` is the natural key; duplicates break joins

#### weather_dim Validation

**Temperature Range:**
* Valid range: -50°F to 150°F
* Warning if: Outside range
* Why: Extreme values indicate sensor errors

---

## Usage

### Manual Execution

```bash
# Run from project root
python health_check/health_check.py

# Or with full path
python /Workspace/Users/<your-email>/project_volltstream/health_check/health_check.py
```

---

### Programmatic Usage

```python
from pyspark.sql import SparkSession
from health_check.health_check import GoldTableHealthCheck

# Initialize Spark
spark = SparkSession.builder.appName("HealthCheck").getOrCreate()

# Run health checks
health_checker = GoldTableHealthCheck(
    spark=spark,
    catalog="gold_dev",
    schema="electrovolt"
)

is_healthy = health_checker.run_all_checks()

if is_healthy:
    print("✓ Data is healthy!")
else:
    print("✗ Data quality issues detected!")
    # Trigger alert, send notification, etc.
```

---

### Notebook Usage

```python
# Cell 1: Run health check
%run /Workspace/Users/<your-email>/project_volltstream/health_check/health_check.py

# Cell 2: Exit with status (for job orchestration)
if not is_healthy:
    dbutils.notebook.exit("FAILED")
```

---

## Configuration

### Modify Catalog/Schema

Edit `health_check.py` or pass parameters:

```python
health_checker = GoldTableHealthCheck(
    spark=spark,
    catalog="gold_prod",      # Change to production catalog
    schema="electrovolt_v2"   # Change schema
)
```

---

### Adjust Data Freshness Threshold

Default: 48 hours. Modify in script:

```python
# In check_data_freshness() calls (line 269)
self.check_data_freshness(table_name, full_table_name, max_age_hours=24)  # 24 hours
```

---

### Add Custom Tables

Edit the `tables` dictionary in `__init__()` (lines 85-89):

```python
self.tables = {
    "station_facts": f"{catalog}.{schema}.station_facts",
    "station_dim": f"{catalog}.{schema}.station_dim",
    "weather_dim": f"{catalog}.{schema}.weather_dim",
    "new_table": f"{catalog}.{schema}.new_table"  # Add new table
}
```

---

### Customize Critical Columns

Edit `critical_columns` dictionary in `run_all_checks()` (lines 259-263):

```python
critical_columns = {
    "station_facts": ["is_operational", "risk_score", "avg_energy_cost_per_hour"],
    "station_dim": ["station_id", "latitude", "longitude"],
    "weather_dim": ["weather_sk", "weather", "temp"],
    "new_table": ["id", "value"]  # Add new table's critical columns
}
```

---

## Scheduling

### Option 1: Databricks Workflow Task (Recommended)

Add as the **final task** in the `project_voltstream` job:

**Task Configuration:**
* **Task Key:** `validate_gold_data`
* **Type:** Notebook task
* **Path:** `/Workspace/Users/<email>/project_volltstream/notebooks/validation/health_check`
* **Depends On:** `join_station_and_weather_data`
* **Effect:** Job fails if health checks fail

**Benefits:**
* Integrated validation gate
* Prevents downstream consumption of bad data
* Automatic failure alerts

---

### Option 2: Standalone Scheduled Job

Create a separate job for monitoring:

**Job Configuration:**
* **Name:** `gold_health_check`
* **Task Type:** Python script
* **Path:** `/Workspace/Users/<email>/project_volltstream/health_check/health_check.py`
* **Schedule:** Hourly (e.g., `0 0 */1 * * ? *`)
* **Cluster:** Serverless or small cluster
* **Alerts:** Email on failure

**Benefits:**
* Independent monitoring
* Frequent checks (hourly)
* Doesn't block pipeline execution

---

## Exit Codes

The script uses standard exit codes for automation:

| Exit Code | Status | Meaning |
|-----------|--------|---------|
| `0` | ✅ SUCCESS | All health checks passed |
| `1` | ❌ FAILURE | One or more checks failed |
| `2` | ⚠️ ERROR | Script execution error (exception) |

**Usage in Workflows:**
* Exit code `0` → Task succeeds, job continues
* Exit code `1` or `2` → Task fails, job fails, alerts triggered

**Usage in CI/CD:**
```bash
python health_check/health_check.py
if [ $? -ne 0 ]; then
    echo "Health checks failed! Blocking deployment."
    exit 1
fi
```

---

## Example Output

```
============================================================
Starting Gold Table Health Checks
============================================================

Checking table: station_facts
----------------------------------------
✓ Table Exists: station_facts
✓ Table Not Empty: station_facts: 1,234 rows
✓ NULL Values Check: station_facts
✓ Data Freshness: station_facts: 12.3 hours old
✓ Data Quality Metrics: station_facts

Checking table: station_dim
----------------------------------------
✓ Table Exists: station_dim
✓ Table Not Empty: station_dim: 456 rows
✓ NULL Values Check: station_dim
⚠ Data Freshness: station_dim: 50.2 hours old (max: 48)
✓ Data Quality Metrics: station_dim

Checking table: weather_dim
----------------------------------------
✓ Table Exists: weather_dim
✓ Table Not Empty: weather_dim: 789 rows
✓ NULL Values Check: weather_dim
✓ Data Freshness: weather_dim: 15.7 hours old
✓ Data Quality Metrics: weather_dim

============================================================
HEALTH CHECK SUMMARY
============================================================
✓ Passed: 14
✗ Failed: 0
⚠ Warnings: 1
============================================================
✓ OVERALL STATUS: HEALTHY
============================================================

✓ All health checks passed!
```

---

## Architecture

### Class Structure

```
HealthCheckStatus (lines 31-56)
├── __init__()
├── add_pass(check_name, message)
├── add_fail(check_name, message)
├── add_warning(check_name, message)
├── is_healthy() → bool
└── summary() → dict

GoldTableHealthCheck (lines 59-305)
├── __init__(spark, catalog, schema)
├── check_table_exists(table_name, full_table_name)
├── check_table_not_empty(table_name, full_table_name)
├── check_null_values(table_name, full_table_name, critical_columns)
├── check_data_freshness(table_name, full_table_name, max_age_hours=48)
├── check_data_quality_metrics(table_name, full_table_name)
├── run_all_checks() → bool
└── print_summary()
```

---

### Execution Flow

```
1. Initialize GoldTableHealthCheck (lines 62-89)
   └── Load table list: station_facts, station_dim, weather_dim

2. For each table (lines 246-272):
   ├── Check table exists → If fail, skip remaining checks
   ├── Check table not empty → If fail, skip remaining checks
   ├── Check NULL values in critical columns
   ├── Check data freshness (max ingest_timestamp)
   └── Check data quality metrics (specific to table)

3. Print summary (lines 279-304)
   ├── ✓ Passed checks count
   ├── ✗ Failed checks count
   └── ⚠ Warnings count

4. Return: True if healthy (0 failures), False if unhealthy
```

---

### Status Tracking

The `HealthCheckStatus` class tracks results:

**Attributes:**
* `passed`: List of (check_name, message) tuples
* `failed`: List of (check_name, message) tuples
* `warnings`: List of (check_name, message) tuples

**Methods:**
* `is_healthy()`: Returns `True` if `len(failed) == 0`
* `summary()`: Returns dict with counts

---

## Troubleshooting

### Issue: "Table does not exist"

**Cause:**
* Wrong catalog/schema name
* Table not created yet
* Permissions issue

**Solution:**
```sql
-- Verify catalog/schema
SHOW CATALOGS;
SHOW SCHEMAS IN gold_dev;

-- Check table exists
SELECT * FROM gold_dev.electrovolt.station_facts LIMIT 1;

-- Grant permissions if needed
GRANT SELECT ON TABLE gold_dev.electrovolt.* TO <user>;
```

---

### Issue: "ModuleNotFoundError: No module named 'logger'"

**Cause:**
* Python path not set correctly
* Running from wrong directory

**Solution:**
```bash
# Run from project root
cd /Workspace/Users/<email>/project_volltstream
python health_check/health_check.py
```

Or update path in script (line 26):
```python
sys.path.insert(0, '/Workspace/Users/<email>/project_volltstream')
```

---

### Issue: Health checks pass but data looks wrong

**Cause:**
* Checks are not comprehensive enough
* Business logic changed

**Solution:**

Add custom validation in `check_data_quality_metrics()` (lines 191-238):

```python
# Example: Check downtime cost is not negative
if table_name == "station_facts":
    if "est_downtime_cost" in df.columns:
        invalid_cost = df.filter(col("est_downtime_cost") < 0).count()
        if invalid_cost > 0:
            self.status.add_fail(check_name, f"{invalid_cost} negative downtime costs")
```

---

### Issue: Script times out

**Cause:**
* Large tables (> 1M rows)
* Complex quality checks

**Solution:**

1. Increase job timeout in Databricks
2. Sample data for quality checks:
```python
df_sample = df.sample(0.1)  # 10% sample
null_count = df_sample.filter(col("column").isNull()).count()
```
3. Use approximate count methods

---

## Integration with Workflows

### Databricks Workflow Integration

**Current project_voltstream job:**

```
extract_stations 
  → if_updated_data (conditional task)
  → process_stations
  → extract_weather_data
  → process_station_connectors (parallel)
  → process_weather_data
  → join_station_and_weather_data
  → validate_gold_data ← Health Check Here
```

**Behavior:**
* Health check runs **only if** all upstream tasks succeed
* If health check **fails**, entire job fails
* Failed job triggers email alerts (if configured)
* Downstream systems won't consume bad data

---

### CI/CD Pipeline Integration

Add to `.github/workflows/deploy.yml` (after deployment):

```yaml
- name: Run Health Checks
  run: |
    databricks jobs run-now --job-id <health_check_job_id>
    # Wait for completion and check status
    if [ $? -ne 0 ]; then
      echo "Health checks failed after deployment!"
      exit 1
    fi
```

---

## Logging

Health checks use the project's custom logger (`logger.custom_logging`):

**Log Context (lines 70-74):**
* `layer`: "gold"
* `job`: "health_check"
* `dataset`: "{catalog}.{schema}"
* `run_id`: Job run ID (or "manual_run")

**Log Levels:**
* `INFO`: Passed checks, summary
* `WARNING`: Data freshness warnings, quality warnings
* `ERROR`: Failed checks, exceptions

**Log Output:**
* Console (stdout)
* Databricks job logs
* Custom log destination (if configured)

**Log Format Examples:**
```
✓ Table Exists: station_facts
✓ Table Not Empty: station_facts: 1,234 rows
✗ NULL Values Check: station_facts: risk_score: 5 NULLs (0.41%)
⚠ Data Freshness: station_dim: 50.2 hours old (max: 48)
```

---

## Best Practices

1. **Run after every pipeline execution** - Catch issues immediately
2. **Set appropriate thresholds** - Balance sensitivity vs. false positives
3. **Monitor trends** - Track warning counts over time
4. **Alert on failures** - Email, Slack, PagerDuty integration
5. **Document failures** - Add context to failed check messages
6. **Test health checks** - Intentionally break data to verify detection
7. **Version control** - Track changes to validation logic in Git
8. **Review warnings regularly** - Don't ignore warnings (today's warning = tomorrow's failure)
9. **Customize for your data** - Add domain-specific validations
10. **Keep it fast** - Use sampling for large tables

---

## Key Implementation Details

### Early Exit on Critical Failures

The script implements smart early exit (lines 251-256):
* If table doesn't exist → skip remaining checks for that table
* If table is empty → skip remaining checks for that table
* Prevents cascading errors and unnecessary compute

### Table-Specific Validations

Each table has custom quality checks (lines 198-230):
* **station_facts**: Risk score range, availability range
* **station_dim**: Duplicate detection
* **weather_dim**: Temperature range

### Exception Handling

All check methods have try-catch blocks (lines 104-107, 125-127, etc.):
* Catches and logs exceptions
* Continues checking other tables
* Returns appropriate boolean status

---

## Version History

### v1.0 (Current)
* Initial health check implementation
* 5 validation categories
* 3 gold tables validated (station_facts, station_dim, weather_dim)
* Logging integration with custom logger
* Databricks Workflow integration
* Exit code support for automation
* Configurable freshness threshold (default: 48 hours)
* Early exit on critical failures
* Table-specific quality validations

---

## Related Documentation

* [project_voltstream README](../README.md) - Main project documentation
* [Databricks Workflows Orchestration](../README.md#databricks-workflows-orchestration) - Job configuration
* [CI/CD Pipeline](../README.md#cicd-pipeline) - Automated deployment
* [Test Suite](../tests/README.md) - Unit and integration tests

---

**For questions or issues, contact the Data Engineering team.**

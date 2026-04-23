# project_voltstream Test Suite

Comprehensive test suite for the project_voltstream EV charging station analytics pipeline using **pytest**.

## Overview

This test suite provides unit tests and integration tests for all utility functions across the bronze, silver, and gold layers of the data pipeline. The tests follow a **functional programming style** using pytest, which aligns with the functional approach used in the pipeline utilities.

## Test Structure

```
tests/
├── README.md                 # This file
├── conftest.py              # Pytest configuration and shared fixtures
├── run_tests.py             # Test runner script (pytest wrapper)
├── test_shared.py           # Unit tests for shared utilities
├── test_bronze.py           # Unit tests for bronze layer utilities
├── test_silver.py           # Unit tests for silver layer utilities
├── test_gold.py             # Unit tests for gold layer utilities
└── test_integration.py      # Integration tests for end-to-end flows
```

## Test Files

### Unit Tests

#### `test_shared.py`
Tests for shared utilities used across all pipeline layers:
* `fetch_data()` - API data fetching with retry logic
* `load_checkpoint()` - Checkpoint loading for incremental updates
* `load_table()` - Delta table initialization
* `add_timestamp()` - Timestamp column addition
* `filter_uningested_data()` - Incremental data filtering
* `deduplicate()` - Data deduplication
* `write_to_table()` - Table writing operations
* `create_dataframe()` - DataFrame creation
* `update_table()` - Delta table merge operations

#### `test_bronze.py`
Tests for bronze layer utilities (data ingestion):
* `generate_grid()` - Grid generation for API pagination
* `add_buffer()` - Timestamp buffer addition
* `get_all_stations_data()` - Bulk station data fetching
* `convert_to_json_string()` - JSON string conversion
* `create_bronze_stations_df()` - Bronze stations DataFrame creation
* `get_weather_zone()` - Weather zone extraction
* `get_weather_zone_data()` - Weather data fetching
* `create_bronze_weather_df()` - Bronze weather DataFrame creation
* `NoUpdatesError` - Custom exception handling

#### `test_silver.py`
Tests for silver layer utilities (data transformation):
* `extract_col_from_json_string()` - JSON column extraction
* `validate_columns()` - Column validation
* `validate_and_quarantine_rows()` - Row validation and quarantine
* `load_current_data()` - Current dimension loading
* `join_current_and_incoming_data()` - Data joining
* `add_scd_logic()` - Slowly Changing Dimension logic
* `add_columns_for_idempotency()` - Idempotency column addition
* `identify_rows_to_expire()` - SCD expiration logic
* `select_station_columns()` - Station column selection
* `convert_lat_lon_type()` - Coordinate type conversion
* `add_weather_zone_coordinates()` - Weather zone coordinate addition
* `explode_connections()` - Connection array flattening
* `transform_weather()` - Weather data transformation

#### `test_gold.py`
Tests for gold layer utilities (business logic):
* `create_view()` - Global temporary view creation
* `join_tables()` - Multi-table joining
* `get_station_status_facts()` - Fact table creation
* `add_station_facts_metadata()` - Business metadata addition
* `get_station_dim()` - Station dimension creation
* `get_weather_dim()` - Weather dimension creation
* Risk score calculation logic
* Availability calculation logic

### Integration Tests

#### `test_integration.py`
End-to-end integration tests:
* Bronze to Silver data flow for stations
* Bronze to Silver data flow for weather
* Silver to Gold complete pipeline flow
* End-to-end data quality validation
* Incremental updates and idempotency
* Multi-layer transformation validation

## Running Tests

### Prerequisites

Install testing dependencies:

```bash
pip install -r requirements_test.txt
```

Or install pytest directly:

```bash
pip install pytest pytest-cov
```

### Run All Tests

```bash
# Using the test runner script
python tests/run_tests.py

# Using pytest directly
pytest tests/

# With verbose output
pytest tests/ -v
```

### Run Specific Test Files

```bash
# Run unit tests for shared utilities
pytest tests/test_shared.py

# Run unit tests for bronze layer
pytest tests/test_bronze.py

# Run unit tests for silver layer
pytest tests/test_silver.py

# Run unit tests for gold layer
pytest tests/test_gold.py

# Run integration tests
pytest tests/test_integration.py
```

### Run Specific Tests

```bash
# Run specific test function
pytest tests/test_bronze.py::test_generate_grid

# Run tests matching a pattern
pytest tests/ -k "test_fetch"

# Run tests with verbose output
pytest tests/test_bronze.py -v
```

### Run Tests by Category

Tests can be marked with custom markers (defined in `pytest.ini`):

```bash
# Run only unit tests
python tests/run_tests.py --unit-only

# Run only integration tests
python tests/run_tests.py --integration-only

# Skip slow tests
pytest tests/ -m "not slow"
```

### Run Tests with Coverage

```bash
# Using the test runner script
python tests/run_tests.py --coverage

# Using pytest directly
pytest tests/ --cov=utils --cov-report=html --cov-report=term-missing

# View HTML coverage report
open htmlcov/index.html
```

### Stop on First Failure

```bash
# Using the test runner
python tests/run_tests.py -x

# Using pytest directly
pytest tests/ -x
```

### Run Tests in Parallel

```bash
# Install pytest-xdist first
pip install pytest-xdist

# Run tests in parallel (4 workers)
pytest tests/ -n 4
```

## Test Configuration

### Prerequisites

* PySpark installed
* Databricks environment (or local Spark setup)
* Access to test catalogs and schemas
* Required Python packages (see `requirements_test.txt`):
  * `pytest>=7.4.0`
  * `pytest-cov>=4.1.0`
  * `pyspark`
  * `requests`
  * `numpy`

### Shared Fixtures

The `conftest.py` file provides shared fixtures for all tests:

* **`spark` fixture**: Creates a Spark session for all tests (session scope)
  * Automatically available to any test function that includes `spark` as a parameter
  * Cleans up automatically after all tests complete

Example usage:
```python
def test_my_function(spark):
    # Spark session is automatically injected
    df = spark.createDataFrame(...)
    assert df.count() == 10
```

### Environment Setup

Tests use a local Spark session for unit tests. For integration tests that need access to actual tables, ensure:

1. You have access to the following catalogs:
   * `bronze_dev.electrovolt`
   * `silver_dev.electrovolt`
   * `gold_dev.electrovolt`

2. Required environment variables (if applicable):
   ```bash
   export SPARK_HOME=/path/to/spark
   export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python
   ```

## Pytest Configuration

The `pytest.ini` file in the project root configures pytest behavior:

* Test discovery patterns
* Output formatting
* Custom markers (unit, integration, slow)
* Coverage settings

## Test Coverage

### Current Coverage

* **Shared utilities**: 9/9 functions (100%)
* **Bronze layer**: 9/9 functions (100%)
* **Silver layer**: 14/14 functions (100%)
* **Gold layer**: 6/6 functions (100%)
* **Integration tests**: 5 end-to-end scenarios

### Coverage Areas

✅ **Covered**:
* API data fetching and error handling
* Data transformation and type conversions
* SCD (Slowly Changing Dimension) logic
* Business logic and calculations
* Data validation and quarantine
* End-to-end pipeline flows

⚠️ **Limitations**:
* Tests use mock data, not live API calls
* Some tests require manual verification in production environment
* Performance/load testing not included

## Continuous Integration

To integrate with CI/CD:

```bash
# Add to your CI pipeline
python tests/run_tests.py || exit 1

# Or use pytest directly
pytest tests/ --cov=utils --cov-report=xml || exit 1
```

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'utils'`
* **Solution**: Ensure you're running tests from the project root, or the path is correctly set in `conftest.py`

**Issue**: Spark session errors
* **Solution**: Verify Spark is installed and `SPARK_HOME` is set correctly

**Issue**: `pytest: command not found`
* **Solution**: Install pytest: `pip install pytest`

## Contributing

When adding new tests:

1. Write tests as simple functions (not methods in a class)
2. Use descriptive test names starting with `test_`
3. Use fixtures for shared resources (like the `spark` fixture)
4. Add docstrings to explain what the test validates
5. Use `assert` statements with clear error messages
6. Mark slow tests with `@pytest.mark.slow`
7. Run tests locally before committing: `pytest tests/`

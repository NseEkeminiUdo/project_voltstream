import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
import os

from utils.shared import (
    fetch_data,
    load_table,
    add_timestamp,
    filter_uningested_data,
    deduplicate,
    write_to_table,
    update_table,
)


# SparkSession fixture for standalone execution
@pytest.fixture(scope="session")
def spark():
    """
    Spark session fixture that works in both Databricks and local environments.
    In Databricks, returns the existing session. Locally, creates a new session.
    """
    builder = SparkSession.builder.appName("test-shared")

    # Only set master if NOT in Databricks (Spark Connect doesn't allow it)
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()

    # Set log level to reduce noise during tests
    try:
        spark.sparkContext.setLogLevel("ERROR")
    except Exception:
        pass

    yield spark

    # Only stop in local runs (don't stop in Databricks)
    try:
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            spark.stop()
    except Exception:
        pass


# Tests for fetch_data
@patch("utils.shared.requests.get")
def test_fetch_data_success(mock_get):
    """Test successful API data fetch"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "test"}
    mock_get.return_value = mock_response

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    result = fetch_data("http://test.com", {"param": "value"}, **log_info)

    assert result == {"data": "test"}
    mock_get.assert_called_once()


@patch("utils.shared.requests.get")
@patch("utils.shared.time.sleep")
def test_fetch_data_retry_on_failure(mock_sleep, mock_get):
    """Test retry mechanism on API failure"""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Server Error"
    mock_get.return_value = mock_response

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    with pytest.raises(Exception):
        fetch_data("http://test.com", {"param": "value"}, **log_info)

    # Should retry 5 times
    assert mock_get.call_count == 5


# Tests for load_table
@pytest.mark.spark
def test_load_table_create_new(spark):
    """Test creating a new table if it doesn't exist"""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    df = spark.createDataFrame([], schema)
    table_name = "test_new_table"

    # Ensure table doesn't exist
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    table = load_table(spark, table_name, df)

    # Verify table was created
    assert spark.catalog.tableExists(table_name)

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# Tests for add_timestamp
@pytest.mark.spark
def test_add_timestamp(spark):
    """Test adding timestamp column to dataframe"""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    result_df = add_timestamp(df)

    assert "ingest_timestamp" in result_df.columns
    assert result_df.count() == 2


# Tests for filter_uningested_data
@pytest.mark.spark
def test_filter_uningested_data(spark):
    """Test filtering out already ingested data"""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("ingest_timestamp", TimestampType(), True),
        ]
    )
    test_data = [
        (1, datetime(2025, 1, 1, 10, 0, 0)),
        (2, datetime(2025, 1, 2, 11, 0, 0)),
        (3, datetime(2025, 1, 3, 12, 0, 0)),
    ]
    df = spark.createDataFrame(test_data, schema)

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    max_time = datetime(2025, 1, 2, 0, 0, 0)
    result_df = filter_uningested_data(df, max_time, **log_info)

    # Should only have records after max_time
    assert result_df.count() == 2


# Tests for deduplicate
@pytest.mark.spark
def test_deduplicate_all_columns(spark):
    """Test deduplication on all columns"""
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["id", "value"])
    result_df = deduplicate(df)

    assert result_df.count() == 2


@pytest.mark.spark
def test_deduplicate_specific_columns(spark):
    """Test deduplication on specific columns"""
    df = spark.createDataFrame(
        [(1, "a", "x"), (1, "a", "y"), (2, "b", "z")], ["id", "value", "extra"]
    )
    result_df = deduplicate(df, ["id", "value"])

    assert result_df.count() == 2


# Tests for write_to_table
@pytest.mark.spark
def test_write_to_table(spark):
    """Test writing dataframe to table"""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    table_name = "test_write_table"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    # Cleanup first
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    write_to_table(df, table_name, "overwrite", **log_info)

    # Verify table exists and has correct count
    assert spark.catalog.tableExists(table_name)
    result_df = spark.table(table_name)
    assert result_df.count() == 2

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# Tests for update_table
@pytest.mark.spark
def test_update_table_merge_operations(spark):
    """Test update_table with merge operations"""
    from delta.tables import DeltaTable

    # Create initial table
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )
    initial_data = [(1, "a"), (2, "b")]
    df_initial = spark.createDataFrame(initial_data, schema)
    table_name = "test_update_table"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    df_initial.write.format("delta").mode("overwrite").saveAsTable(table_name)

    # Create updates (update existing + insert new)
    update_data = [(1, "updated"), (3, "c")]
    df_updates = spark.createDataFrame(update_data, schema)

    # Get DeltaTable and update
    delta_table = DeltaTable.forName(spark, table_name)
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    update_table(df_updates, delta_table, "id", **log_info)

    # Verify results
    result_df = spark.table(table_name)
    assert result_df.count() == 3

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

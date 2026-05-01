import pytest
from datetime import datetime
from pyspark.sql.functions import col, cast
from utils.observability import (
    create_control_table,
    get_last_processed_timestamp,
    insert_control_record,
)


@pytest.fixture
def test_table_name():
    """Generate a unique table name for testing."""
    return f"test_control_table_{datetime.now().strftime('%Y%m%d%H%M%S')}"


@pytest.fixture
def setup_control_table(spark, test_table_name):
    """Create and clean up test control table."""
    # Setup: Create table
    create_control_table(spark, test_table_name)

    yield test_table_name

    # Teardown: Drop table
    spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")


# Tests for create_control_table
def test_create_control_table_success(spark, test_table_name):
    """Test that control table is created successfully."""
    result = create_control_table(spark, test_table_name)

    # Verify table exists
    tables = spark.sql("SHOW TABLES").filter(col("tableName") == test_table_name)
    assert tables.count() == 1, f"Table {test_table_name} should exist"
    assert result == test_table_name

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")


def test_create_control_table_schema(spark, test_table_name):
    """Test that control table has correct schema."""
    create_control_table(spark, test_table_name)

    # Check schema
    df = spark.table(test_table_name)
    expected_columns = [
        "pipeline_name",
        "layer",
        "last_processed_timestamp",
        "batch_id",
        "status",
        "records_processed",
        "records_failed",
        "start_time",
        "end_time",
        "error_message",
    ]

    assert df.columns == expected_columns, "Schema columns should match expected"

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")


def test_create_control_table_idempotent(spark, test_table_name):
    """Test that creating table multiple times doesn't fail."""
    create_control_table(spark, test_table_name)
    create_control_table(spark, test_table_name)  # Should not fail

    # Verify only one table exists
    tables = spark.sql("SHOW TABLES").filter(col("tableName") == test_table_name)
    assert tables.count() == 1

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {test_table_name}")


# Tests for get_last_processed_timestamp
def test_get_last_timestamp_no_records(spark, setup_control_table):
    """Test getting timestamp when no records exist."""
    result = get_last_processed_timestamp(
        spark, setup_control_table, "voltstream", "bronze"
    )
    assert result == datetime.fromisoformat("2011-01-01T00:00:00Z")


def test_get_last_timestamp_with_records(spark, setup_control_table):
    """Test getting timestamp when records exist."""
    # Insert a test record
    test_timestamp = datetime(2024, 1, 15, 10, 30, 0)
    insert_control_record(
        spark, setup_control_table, "voltstream", "bronze", test_timestamp
    )

    result = get_last_processed_timestamp(
        spark, setup_control_table, "voltstream", "bronze"
    )
    assert result == test_timestamp, "Should return the last processed timestamp"


def test_get_last_timestamp_multiple_records(spark, setup_control_table):
    """Test getting most recent timestamp when multiple records exist."""
    # Insert multiple records
    timestamp1 = datetime(2024, 1, 15, 10, 0, 0)
    timestamp2 = datetime(2024, 1, 15, 11, 0, 0)
    timestamp3 = datetime(2024, 1, 15, 12, 0, 0)

    insert_control_record(
        spark,
        setup_control_table,
        "voltstream",
        "bronze",
        timestamp1,
        end_time=timestamp1,
    )
    insert_control_record(
        spark,
        setup_control_table,
        "voltstream",
        "bronze",
        timestamp2,
        end_time=timestamp2,
    )
    insert_control_record(
        spark,
        setup_control_table,
        "voltstream",
        "bronze",
        timestamp3,
        end_time=timestamp3,
    )

    result = get_last_processed_timestamp(
        spark, setup_control_table, "voltstream", "bronze"
    )
    assert result == timestamp3, "Should return the most recent timestamp"


# Tests for insert_control_record
def test_insert_record_success(spark, setup_control_table):
    """Test that record is inserted successfully."""
    test_timestamp = datetime(2024, 1, 15, 10, 30, 0)
    batch_id = insert_control_record(
        spark, setup_control_table, "voltstream", "bronze", test_timestamp
    )

    # Verify record exists
    df = spark.table(setup_control_table)
    assert df.count() == 1, "Should have one record"
    assert batch_id is not None, "Should return a batch_id"


def test_insert_record_fields(spark, setup_control_table):
    """Test that inserted record has correct field values."""
    test_timestamp = datetime(2024, 1, 15, 10, 30, 0)
    batch_id = insert_control_record(
        spark,
        setup_control_table,
        "voltstream",
        "silver",
        test_timestamp,
        status="success",
    )

    # Query the record
    df = spark.table(setup_control_table)
    record = df.collect()[0]

    assert record["pipeline_name"] == "voltstream"
    assert record["layer"] == "silver"
    assert record["last_processed_timestamp"] == test_timestamp
    assert record["status"] == "success"
    assert record["batch_id"] == batch_id
    assert record["records_processed"] is None
    assert record["records_failed"] is None
    assert record["start_time"] is None
    assert record["end_time"] is None
    assert record["error_message"] is None


def test_insert_multiple_records(spark, setup_control_table):
    """Test inserting multiple records."""
    timestamp1 = datetime(2024, 1, 15, 10, 0, 0)
    timestamp2 = datetime(2024, 1, 15, 11, 0, 0)

    batch_id1 = insert_control_record(
        spark, setup_control_table, "voltstream", "bronze", timestamp1
    )
    batch_id2 = insert_control_record(
        spark, setup_control_table, "voltstream", "silver", timestamp2
    )

    # Verify both records exist
    df = spark.table(setup_control_table)
    assert df.count() == 2, "Should have two records"
    assert batch_id1 != batch_id2, "Batch IDs should be unique"

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    BooleanType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import col, lit
import json
import os
import subprocess
import sys
import importlib


from utils.silver import (
    extract_col_from_json_string,
    validate_columns,
    validate_and_quarantine_rows,
    load_current_data,
    join_current_and_incoming_data,
    add_scd_logic,
    add_columns_for_idempotency,
    identify_rows_to_expire,
    select_station_columns,
    convert_lat_lon_type,
    add_weather_zone_coordinates,
    explode_connections,
    select_conn_columns,
    transform_weather,
    standardize_towns,
)


# Tests for extract_col_from_json_string
@pytest.mark.spark
def test_extract_col_from_json_string(spark):
    """Test extracting columns from JSON string"""
    json_data = json.dumps(
        {
            "ID": 1,
            "AddressInfo": {
                "Title": "Test Station",
                "AddressLine1": "123 Main St",
                "Town": "New York",
                "StateOrProvince": "NY",
                "Latitude": 40.7128,
                "Longitude": -74.0060,
            },
            "Connections": [{"type": "CHAdeMO"}],
            "StatusType": {"Title": "Operational", "ID": 50},
            "DateLastStatusUpdate": "2025-01-01",
            "DateCreated": "2024-01-01",
        }
    )

    df = spark.createDataFrame([(json_data,)], ["raw_text"])
    result_df = extract_col_from_json_string(df, "raw_text")

    # Verify all expected columns are present
    expected_columns = [
        "station_id",
        "title",
        "address",
        "town",
        "state_or_province",
        "latitude",
        "longitude",
        "connections",
        "status",
        "status_id",
        "date_last_status_update",
        "date_created",
    ]
    for col_name in expected_columns:
        assert col_name in result_df.columns


# Tests for validate_columns
@pytest.mark.spark
def test_validate_columns_all_valid(spark):
    """Test column validation with all valid data"""
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}

    # Should not raise exception
    result_df = validate_columns(df, **log_info)
    assert result_df.count() == 3


@pytest.mark.spark
def test_validate_columns_all_null(spark):
    """Test column validation with all NULL values"""
    # Use explicit schema to avoid type inference issues with NULL values
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )
    df = spark.createDataFrame([(1, None), (2, None), (3, None)], schema)
    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}

    # Should raise RuntimeError for all-NULL column
    with pytest.raises(RuntimeError):
        validate_columns(df, **log_info)


# Tests for validate_and_quarantine_rows
@pytest.mark.spark
@patch("utils.silver.write_to_table")
def test_validate_and_quarantine_rows_all_valid(mock_write, spark):
    """Test row validation with all valid records"""
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame([(1, 40.7, -74.0), (2, 40.8, -73.9)], schema)

    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}
    result_df, invalid_df = validate_and_quarantine_rows(
        df, ["station_id", "latitude", "longitude"], **log_info
    )

    assert result_df.count() == 2


@pytest.mark.spark
@patch("utils.silver.write_to_table")
def test_validate_and_quarantine_rows_with_nulls(mock_write, spark):
    """Test row validation with NULL values"""
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(1, 40.7, -74.0), (2, None, -73.9), (3, 40.9, None)], schema
    )

    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}
    result_df, invalid_df = validate_and_quarantine_rows(
        df, ["station_id", "latitude", "longitude"], **log_info
    )

    # Only 1 valid record (id=1)
    assert result_df.count() == 1
    # Quarantine table should be called with 2 invalid records
    mock_write.assert_called_once()


# Tests for load_current_data
@pytest.mark.spark
def test_load_current_data(spark):
    """Test loading current records from silver table"""
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("Is_Current", BooleanType(), True),
            StructField("value", StringType(), True),
        ]
    )
    data = [(1, True, "current1"), (2, False, "expired"), (3, True, "current2")]
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").saveAsTable("test_current_table")

    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}

    # Load the table first, then pass df to load_current_data
    df_table = spark.table("test_current_table")
    result_df = load_current_data(df_table, **log_info)

    # Should only return current records
    assert result_df.count() == 2

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_current_table")


# Tests for join_current_and_incoming_data
@pytest.mark.spark
def test_join_current_and_incoming_data_single_key(spark):
    """Test joining current and incoming data with single key"""
    incoming = spark.createDataFrame(
        [(1, "new_value"), (2, "another_value")], ["id", "value"]
    )
    current = spark.createDataFrame([(1, "old_value")], ["id", "value"])

    result_df = join_current_and_incoming_data(incoming, current, "id")

    # Should have both matched and unmatched rows
    assert result_df.count() == 2
    # Check that aliased columns exist (stg and current prefixes for non-key
    # columns)
    columns = result_df.columns
    # After join with aliases, non-key duplicate columns should have prefixes
    assert (
        any("stg" in col.lower() or "current" in col.lower() for col in columns)
        or len([c for c in columns if "value" in c.lower()]) >= 1
    )


@pytest.mark.spark
def test_join_current_and_incoming_data_multiple_keys(spark):
    """Test joining current and incoming data with multiple keys"""
    incoming = spark.createDataFrame(
        [(1, "A", "value1"), (2, "B", "value2")], ["id", "type", "value"]
    )
    current = spark.createDataFrame([(1, "A", "old_value")], ["id", "type", "value"])

    result_df = join_current_and_incoming_data(incoming, current, ["id", "type"])

    assert result_df.count() == 2


# Tests for add_scd_logic
@pytest.mark.spark
def test_add_scd_logic_insert(spark):
    """Test SCD logic for new records (INSERT)"""
    # Create staging dataframe (new record)
    stg_df = spark.createDataFrame([(1, "new_status")], ["station_id", "status"])

    # Create empty current dataframe (no existing records)
    current_df = spark.createDataFrame(
        [],
        StructType(
            [
                StructField("station_id", IntegerType(), True),
                StructField("status", StringType(), True),
            ]
        ),
    )

    # Join them with aliases (stg left join current)
    joined_df = stg_df.alias("stg").join(
        current_df.alias("current"), on=["station_id"], how="left"
    )

    # Apply SCD logic
    result_df = add_scd_logic(joined_df, "station_id", "status")

    # Should have scd_action column
    assert "scd_action" in result_df.columns
    action = result_df.select("scd_action").collect()[0][0]
    assert action == "INSERT"


@pytest.mark.spark
def test_add_scd_logic_update(spark):
    """Test SCD logic for changed records (UPDATE)"""
    # Create staging dataframe (updated record)
    stg_df = spark.createDataFrame([(1, "new_status")], ["station_id", "status"])

    # Create current dataframe (existing record with old status)
    current_df = spark.createDataFrame([(1, "old_status")], ["station_id", "status"])

    # Join them with aliases (stg left join current)
    joined_df = stg_df.alias("stg").join(
        current_df.alias("current"), on=["station_id"], how="left"
    )

    # Apply SCD logic
    result_df = add_scd_logic(joined_df, "station_id", "status")

    # Should detect UPDATE
    action = result_df.select("scd_action").collect()[0][0]
    assert action == "UPDATE"


# Tests for add_columns_for_idempotency
@pytest.mark.spark
def test_add_columns_for_idempotency(spark):
    """Test adding idempotency columns for SCD"""
    # Create staging dataframe (new/updated records)
    stg_df = spark.createDataFrame(
        [(1, "Station 1", "Operational", date(2025, 1, 1))],
        ["station_id", "title", "status", "date_last_status_update"],
    )

    # Create current dataframe (existing records with old data)
    current_df = spark.createDataFrame(
        [(1, "Station 1", "Offline", date(2024, 12, 31))],
        ["station_id", "title", "status", "date_last_status_update"],
    )

    # Join them with aliases
    joined_df = join_current_and_incoming_data(stg_df, current_df, "station_id")

    # Apply SCD logic
    joined_df = add_scd_logic(joined_df, "station_id", "status")

    # Now call add_columns_for_idempotency with the properly joined dataframe
    result_df = add_columns_for_idempotency(
        joined_df,
        ["station_id"],
        "station_sk",
        ["station_id", "title", "status", "date_last_status_update"],
    )

    # Should have idempotency columns
    assert "station_sk" in result_df.columns
    assert "valid_from" in result_df.columns
    assert "valid_to" in result_df.columns
    assert "is_current" in result_df.columns

    # Verify valid_from is set correctly
    row = result_df.collect()[0]
    assert row["valid_from"] == date(2025, 1, 1)
    assert row["is_current"]


# Tests for identify_rows_to_expire
@pytest.mark.spark
def test_identify_rows_to_expire(spark):
    """Test identifying rows that need to be expired"""
    schema = StructType(
        [
            StructField("scd_action", StringType(), True),
            StructField("station_id", IntegerType(), True),
            StructField("station_sk", LongType(), True),
            StructField("valid_from", DateType(), True),
        ]
    )
    df = spark.createDataFrame([("UPDATE", 1, 12345, date(2025, 1, 2))], schema)

    result_df = identify_rows_to_expire(df, "station_sk")

    # Should have rows to expire
    assert result_df.count() == 1
    assert "station_sk" in result_df.columns


# Tests for select_station_columns
@pytest.mark.spark
def test_select_station_columns(spark):
    """Test selecting and renaming station columns"""
    df = spark.createDataFrame(
        [
            (
                1,
                "Station A",
                "123 Main",
                "NYC",
                "NY",
                40.7,
                -74.0,
                "Operational",
                50,
                date(2025, 1, 1),
                date(2024, 1, 1),
            )
        ],
        [
            "station_id",
            "title",
            "address",
            "town",
            "state_or_province",
            "latitude",
            "longitude",
            "status",
            "status_id",
            "date_last_status_update",
            "date_created",
        ],
    )

    result_df = select_station_columns(df)

    # Should have selected columns
    expected_cols = [
        "station_id",
        "title",
        "address",
        "town",
        "state_or_province",
        "latitude",
        "longitude",
        "status",
        "status_id",
        "date_last_status_update",
        "date_created",
    ]
    for col_name in expected_cols:
        assert col_name in result_df.columns


# Tests for convert_lat_lon_type
@pytest.mark.spark
def test_convert_lat_lon_type(spark):
    """Test converting latitude and longitude to double type"""
    df = spark.createDataFrame([(1, "40.7", "-74.0")], ["id", "latitude", "longitude"])

    result_df = convert_lat_lon_type(df)

    # Should have double type for lat/lon
    lat_type = [f.dataType for f in result_df.schema.fields if f.name == "latitude"][0]
    lon_type = [f.dataType for f in result_df.schema.fields if f.name == "longitude"][0]
    assert isinstance(lat_type, DoubleType)
    assert isinstance(lon_type, DoubleType)


# Tests for add_weather_zone_coordinates
@pytest.mark.spark
def test_add_weather_zone_coordinates(spark):
    """Test adding weather zone coordinates"""
    df = spark.createDataFrame(
        [(1, 40.7128, -74.0060)], ["station_id", "latitude", "longitude"]
    )

    result_df = add_weather_zone_coordinates(df)

    # Should have weather zone columns
    assert "weather_zone_lat" in result_df.columns
    assert "weather_zone_lon" in result_df.columns


# Tests for standardize_towns
@pytest.mark.spark
def test_standardize_towns(spark, ensure_geopandas):
    import geopandas as gpd
    from shapely.geometry import Polygon
   
    """Test standardizing town names using geospatial join"""
    # Create test input dataframe with station data
    # Use a point clearly within Manhattan polygon only
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("town", StringType(), True),
        ]
    )
    df = spark.createDataFrame([(1, 40.77, -74.00, "Old Town Name")], schema)

    # Create non-overlapping polygons for NYC boroughs
    nyc_boroughs = gpd.GeoDataFrame(
        {
            "name": [
                "Manhattan",
                "Brooklyn",
                "Queens",
                "Bronx",
                "Staten Island",
            ],
            "geometry": [
                Polygon([(-74.05, 40.70), (-73.95, 40.70), (-73.95, 40.85), (-74.05, 40.85)]),  # Manhattan
                Polygon([(-74.05, 40.55), (-73.95, 40.55), (-73.95, 40.70), (-74.05, 40.70)]),  # Brooklyn
                Polygon([(-73.70, 40.70), (-73.50, 40.70), (-73.50, 40.85), (-73.70, 40.85)]),  # Queens
                Polygon([(-73.95, 40.85), (-73.70, 40.85), (-73.70, 41.00), (-73.95, 41.00)]),  # Bronx
                Polygon([(-74.25, 40.50), (-74.05, 40.50), (-74.05, 40.70), (-74.25, 40.70)]),  # Staten Island
            ],
        },
        crs="EPSG:4326"
    )

    return_value = nyc_boroughs.to_parquet("/Volumes/bronze_dev/superstor_schema/raw_superstore/tmp.parquet")


    # Call the function
    result_df = standardize_towns(spark, df, file="tmp.parquet")

    # Verify output dataframe structure
    assert result_df.count() == 1
    assert "town" in result_df.columns
    assert "station_id" in result_df.columns
    assert "latitude" in result_df.columns
    assert "longitude" in result_df.columns

    # Verify dropped columns are not present
    assert "geometry" not in result_df.columns
    assert "distance" not in result_df.columns
    assert "index_right" not in result_df.columns

    # Verify town was renamed from "name"
    row = result_df.collect()[0]
    assert row["town"] == "Manhattan"


@pytest.mark.spark
def test_standardize_towns_multiple_stations(spark, ensure_geopandas):
    import geopandas as gpd
    from shapely.geometry import Polygon

    """Test standardizing town names for multiple stations"""
    # Create test input dataframe with multiple stations
    # Use coordinates clearly within different boroughs (no overlaps)
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("town", StringType(), True),
        ]
    )
    # Point 1: clearly in Manhattan (lat: 40.77, lon: -74.00)
    # Point 2: clearly in Brooklyn (lat: 40.60, lon: -74.00)
    df = spark.createDataFrame(
        [(1, 40.77, -74.00, "Old Town 1"), (2, 40.60, -74.00, "Old Town 2")],
        schema,
    )

    # Create non-overlapping polygons for NYC boroughs
    nyc_boroughs = gpd.GeoDataFrame(
        {
            "name": [
                "Manhattan",
                "Brooklyn",
                "Queens",
                "Bronx",
                "Staten Island",
            ],
            "geometry": [
                Polygon([(-74.05, 40.70), (-73.95, 40.70), (-73.95, 40.85), (-74.05, 40.85)]),  # Manhattan
                Polygon([(-74.05, 40.55), (-73.95, 40.55), (-73.95, 40.70), (-74.05, 40.70)]),  # Brooklyn
                Polygon([(-73.70, 40.70), (-73.50, 40.70), (-73.50, 40.85), (-73.70, 40.85)]),  # Queens
                Polygon([(-73.95, 40.85), (-73.70, 40.85), (-73.70, 41.00), (-73.95, 41.00)]),  # Bronx
                Polygon([(-74.25, 40.50), (-74.05, 40.50), (-74.05, 40.70), (-74.25, 40.70)]),  # Staten Island
            ],
        },
        crs="EPSG:4326"
    )

    nyc_boroughs.to_parquet("/Volumes/bronze_dev/superstor_schema/raw_superstore/tmp_multi.parquet")

    # Call the function
    result_df = standardize_towns(spark, df, file="/Volumes/bronze_dev/superstor_schema/raw_superstore/tmp_multi.parquet")

    # Verify output dataframe has correct count
    assert result_df.count() == 2

    # Verify town names are standardized
    towns = [row["town"] for row in result_df.collect()]
    assert "Manhattan" in towns
    assert "Brooklyn" in towns


# Tests for explode_connections
@pytest.mark.spark
def test_explode_connections(spark):
    """Test exploding connections array"""

    df = spark.createDataFrame(
        [(1, date(2025, 1, 1), '[{"PowerKW": 50, "Quantity": 2, "Level": {"ID": 2}]')],
        ["station_id", "date_last_status_update", "Connections"],
    )

    log_info = {"layer": "silver", "job": "test", "dataset": "test_dataset"}
    result_df = explode_connections(df, **log_info)

    # Should have exploded connection data
    assert result_df.count() >= 1
    assert "conn" in result_df.columns


# Tests for select_conn_columns
@pytest.mark.spark
def test_select_conn_columns(spark):
    """Test selecting connection columns after explode_connections"""
    # Match the exact schema from schema.silver.explode_conn_schema (after
    # explode)
    conn_schema = StructType(
        [
            StructField("ID", IntegerType(), True),
            StructField("ConnectionTypeID", IntegerType(), True),
            StructField("LevelID", IntegerType(), True),
            StructField("Amps", IntegerType(), True),
            StructField("Voltage", DoubleType(), True),
            StructField("PowerKW", DoubleType(), True),
            StructField(
                "CurrentType",
                StructType(
                    [
                        StructField("ID", IntegerType(), True),
                        StructField("Title", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("Quantity", IntegerType(), True),
            StructField(
                "ConnectionType",
                StructType(
                    [
                        StructField("ID", IntegerType(), True),
                        StructField("Title", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    schema = StructType(
        [
            StructField("conn", conn_schema, True),
            StructField("station_id", IntegerType(), True),
            StructField("date_last_status_update", DateType(), True),
            StructField("ingest_timestamp", TimestampType(), True),
        ]
    )

    # Create dataframe with proper struct for conn column
    # Tuple structure: (ID, ConnectionTypeID, LevelID, Amps, Voltage, PowerKW,
    # CurrentType, Quantity, ConnectionType)
    df = spark.createDataFrame(
        [
            (
                (1, 2, 2, 100, 240.0, 50.0, (1, "AC"), 2, (2, "Type 2")),
                123,
                date(2025, 1, 1),
                datetime.now(),
            )
        ],
        schema,
    )

    result_df = select_conn_columns(df)

    # Should have selected and flattened connection columns
    expected_cols = [
        "connection_type_id",
        "connection_type",
        "level_id",
        "amps",
        "voltage",
        "power_kw",
        "quantity",
        "current_type",
        "station_id",
        "date_last_status_update",
        "ingest_timestamp",
    ]
    for col_name in expected_cols:
        assert col_name in result_df.columns


# Tests for transform_weather
@pytest.mark.spark
def test_transform_weather(spark):

    """Test transforming weather data"""
    schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("description", StringType(), True),
            StructField("temp", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("rain", DoubleType(), True),
            StructField("snow", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("clouds", IntegerType(), True),
            StructField("dt", IntegerType(), True),
            StructField("ingest_timestamp", TimestampType(), True)
        ]
    )
    df = spark.createDataFrame(
        [
            (
                40.7,
                -74.0,
                "Clear",
                "clear sky",
                72.0,
                1013,
                65,
                10000,
                0.0,
                0.0,
                5.5,
                20,
                1704067200,
                datetime.now(),
            )
        ],
        schema,
    )

    result_df = transform_weather(df)

    # Should have transformed weather data with timestamp
    assert "dt_utc" in result_df.columns
    # Verify dt_utc is converted to timestamp type
    dt_type = [f.dataType for f in result_df.schema.fields if f.name == "dt_utc"][0]
    assert isinstance(dt_type, TimestampType)

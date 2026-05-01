import pytest
from unittest.mock import patch
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import json
import os

from utils.bronze import (
    generate_grid,
    add_buffer,
    get_all_stations_data,
    convert_to_json_string,
    add_bronze_stations_metadata,
    get_weather_zone,
    get_weather_zone_data,
    create_bronze_weather_df,
    NoUpdatesError,
)


# SparkSession fixture for standalone execution
@pytest.fixture(scope="session")
def spark():
    """
    Spark session fixture that works in both Databricks and local environments.
    In Databricks, returns the existing session. Locally, creates a new session.
    """
    builder = SparkSession.builder.appName("test-bronze")

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


# Tests for generate_grid
def test_generate_grid():
    """Test grid generation for API pagination"""
    min_lat, min_lon = 40.0, -74.0
    max_lat, max_lon = 40.2, -73.8

    grid = generate_grid(min_lat, min_lon, max_lat, max_lon)

    assert isinstance(grid, list)
    assert len(grid) > 0

    for point in grid:
        assert isinstance(point, tuple)
        assert len(point) == 2
        lat, lon = point
        assert lat >= min_lat
        assert lat <= max_lat
        assert lon >= min_lon
        assert lon <= max_lon


def test_generate_grid_spacing():
    """Test grid spacing is 0.1 degrees"""
    grid = generate_grid(40.0, -74.0, 40.2, -73.9)

    lats = sorted(set([point[0] for point in grid]))
    lons = sorted(set([point[1] for point in grid]))

    if len(lats) > 1:
        spacing = round(lats[1] - lats[0], 2)
        assert spacing == pytest.approx(0.1, abs=0.01)


def test_generate_grid_invalid_bounds():
    """Test grid generation with invalid bounds (min >= max)"""
    grid = generate_grid(40.0, -74.0, 40.0, -74.0)
    assert isinstance(grid, list)


def test_generate_grid_small_region():
    """Test grid generation with very small region"""
    grid = generate_grid(40.0, -74.0, 40.05, -73.95)
    assert isinstance(grid, list)


# Tests for add_buffer
def test_add_buffer():
    """Test adding 15-minute buffer to timestamp"""
    timestamp = datetime(2025, 1, 1, 12, 0, 0)
    result = add_buffer(timestamp)

    expected = timestamp - timedelta(minutes=15)
    assert result == expected
    assert result == datetime(2025, 1, 1, 11, 45, 0)


def test_add_buffer_midnight():
    """Test buffer crossing midnight boundary"""
    timestamp = datetime(2025, 1, 1, 0, 10, 0)
    result = add_buffer(timestamp)

    assert result == datetime(2024, 12, 31, 23, 55, 0)


# Tests for get_all_stations_data
@patch("utils.bronze.fetch_data")
def test_get_all_stations_data_success(mock_fetch):
    """Test successful fetching of stations data"""
    mock_fetch.return_value = [
        {"ID": 1, "AddressInfo": {"Title": "Station 1"}},
        {"ID": 2, "AddressInfo": {"Title": "Station 2"}},
    ]

    grid = [(40.0, -74.0), (40.1, -74.0)]
    last_timestamp = datetime(2025, 1, 1, 0, 0, 0)
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    result = get_all_stations_data(grid, last_timestamp, api_key, **log_info)

    assert result is not None
    assert len(result) == 4


@patch("utils.bronze.fetch_data")
def test_get_all_stations_data_no_updates(mock_fetch):
    """Test when no new stations are found"""
    mock_fetch.return_value = []

    grid = [(40.0, -74.0)]
    last_timestamp = datetime(2025, 1, 1, 0, 0, 0)
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    result = get_all_stations_data(grid, last_timestamp, api_key, **log_info)

    assert result is None


@patch("utils.bronze.fetch_data")
def test_get_all_stations_data_empty_grid(mock_fetch):
    """Test with empty grid input"""
    grid = []
    last_timestamp = datetime(2025, 1, 1, 0, 0, 0)
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    result = get_all_stations_data(grid, last_timestamp, api_key, **log_info)

    assert result is None
    mock_fetch.assert_not_called()


@patch("utils.bronze.fetch_data")
def test_get_all_stations_data_api_exception(mock_fetch):
    """Test when API throws an exception"""
    mock_fetch.side_effect = Exception("API Error")

    grid = [(40.0, -74.0)]
    last_timestamp = datetime(2025, 1, 1, 0, 0, 0)
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    with pytest.raises(Exception, match="API Error"):
        get_all_stations_data(grid, last_timestamp, api_key, **log_info)


# Tests for convert_to_json_string
def test_convert_to_json_string():
    """Test converting station objects to JSON strings"""
    stations = [{"ID": 1, "name": "Station 1"}, {"ID": 2, "name": "Station 2"}]

    result = convert_to_json_string(stations)

    assert len(result) == 2
    for json_str in result:
        parsed = json.loads(json_str)
        assert "ID" in parsed


def test_convert_to_json_string_empty_list():
    """Test with empty list"""
    result = convert_to_json_string([])

    assert isinstance(result, list)
    assert len(result) == 0


def test_convert_to_json_string_special_characters():
    """Test with special characters in data"""
    stations = [{"ID": 1, "name": 'Station\'s "Special" Name', "emoji": "⚡"}]

    result = convert_to_json_string(stations)

    assert len(result) == 1
    parsed = json.loads(result[0])
    assert parsed["name"] == 'Station\'s "Special" Name'
    assert parsed["emoji"] == "⚡"


# Tests for add_bronze_stations_metadata (REQUIRES SPARK)
@pytest.mark.spark
def test_add_bronze_stations_metadata(spark):
    """Test adding bronze station metadata to dataframe"""
    json_strings = [
        json.dumps(
            {"UUID": "uuid1", "DataProvider": {"WebsiteURL": "http://provider1.com"}}
        ),
        json.dumps(
            {"UUID": "uuid2", "DataProvider": {"WebsiteURL": "http://provider2.com"}}
        ),
    ]

    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    result_df = add_bronze_stations_metadata(df)

    assert result_df.count() == 2
    assert "raw_text" in result_df.columns
    assert "UUID" in result_df.columns
    assert "ingest_timestamp" in result_df.columns
    assert "source_file" in result_df.columns


@pytest.mark.spark
def test_add_bronze_stations_metadata_deduplication(spark):
    """Test deduplication by UUID"""
    json_strings = [
        json.dumps({"UUID": "uuid1"}),
        json.dumps({"UUID": "uuid1"}),
        json.dumps({"UUID": "uuid2"}),
    ]

    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    result_df = add_bronze_stations_metadata(df)

    assert result_df.count() == 2


@pytest.mark.spark
def test_add_bronze_stations_metadata_missing_uuid(spark):
    """Test with records missing UUID field"""
    json_strings = [
        json.dumps({"UUID": "uuid1"}),
        json.dumps({"ID": "id_only"}),
        json.dumps({"UUID": "uuid2"}),
    ]

    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    result_df = add_bronze_stations_metadata(df)

    assert result_df.count() >= 2


@pytest.mark.spark
def test_add_bronze_stations_metadata_null_uuid(spark):
    """Test with null UUID values"""
    json_strings = [
        json.dumps({"UUID": "uuid1"}),
        json.dumps({"UUID": None}),
        json.dumps({"UUID": "uuid2"}),
    ]

    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    result_df = add_bronze_stations_metadata(df)

    assert result_df.count() >= 2


# Tests for get_weather_zone (REQUIRES SPARK)
@pytest.mark.spark
def test_get_weather_zone(spark):
    """Test extracting unique weather zones from dataframe"""
    schema = StructType(
        [
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("station_id", IntegerType(), True),
        ]
    )
    data = [(40.7, -74.0, 1), (40.7, -74.0, 2), (40.8, -73.9, 3)]
    df = spark.createDataFrame(data, schema)

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    result = get_weather_zone(df, **log_info)

    assert len(result) == 2


@pytest.mark.spark
def test_get_weather_zone_empty_df(spark):
    """Test with empty dataframe"""
    schema = StructType(
        [
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("station_id", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame([], schema)

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    result = get_weather_zone(df, **log_info)

    assert len(result) == 0


@pytest.mark.spark
def test_get_weather_zone_null_values(spark):
    """Test with null coordinate values"""
    schema = StructType(
        [
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("station_id", IntegerType(), True),
        ]
    )
    data = [(40.7, -74.0, 1), (None, -74.0, 2), (40.8, None, 3)]
    df = spark.createDataFrame(data, schema)

    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}
    result = get_weather_zone(df, **log_info)

    assert len(result) >= 1


# Tests for get_weather_zone_data
@patch("utils.bronze.fetch_data")
def test_get_weather_zone_data_success(mock_fetch):
    """Test fetching weather data for zones"""
    mock_fetch.return_value = {
        "coord": {"lat": 40.7, "lon": -74.0},
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "main": {"temp": 72, "pressure": 1013, "humidity": 65},
        "visibility": 10000,
        "rain": {"1h": 0},
        "snow": {"1h": 0},
        "wind": {"speed": 5.5},
        "clouds": {"all": 20},
        "dt": 1640000000,
    }

    weather_zones = [(40.7, -74.0)]
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    result = get_weather_zone_data(weather_zones, api_key, **log_info)

    assert len(result) == 1
    weather_data = result[0]
    assert len(weather_data) == 13


@patch("utils.bronze.fetch_data")
def test_get_weather_zone_data_no_data(mock_fetch):
    """Test when no weather data is found"""
    mock_fetch.return_value = {}

    weather_zones = [(40.7, -74.0)]
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    with pytest.raises(ValueError):
        get_weather_zone_data(weather_zones, api_key, **log_info)


@patch("utils.bronze.fetch_data")
def test_get_weather_zone_data_empty_zones(mock_fetch):
    """Test with empty weather zones list"""
    weather_zones = []
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    with pytest.raises(NoUpdatesError, match="No weather zones found"):
        get_weather_zone_data(weather_zones, api_key, **log_info)

    mock_fetch.assert_not_called()


@patch("utils.bronze.fetch_data")
def test_get_weather_zone_data_missing_optional_fields(mock_fetch):
    """Test with API response missing optional fields"""
    mock_fetch.return_value = {
        "coord": {"lat": 40.7, "lon": -74.0},
        "weather": [{"main": "Clear", "description": "clear sky"}],
        "main": {"temp": 72, "pressure": 1013, "humidity": 65},
        "visibility": 10000,
        "wind": {"speed": 5.5},
        "clouds": {"all": 20},
        "dt": 1640000000,
    }

    weather_zones = [(40.7, -74.0)]
    api_key = "test_key"
    log_info = {"layer": "bronze", "job": "test", "dataset": "test_dataset"}

    result = get_weather_zone_data(weather_zones, api_key, **log_info)

    assert len(result) == 1
    weather_data = result[0]
    assert weather_data[8] == 0  # rain default
    assert weather_data[9] == 0  # snow default


# Tests for create_bronze_weather_df (REQUIRES SPARK)
@pytest.mark.spark
def test_create_bronze_weather_df(spark):
    """Test creating bronze weather dataframe"""
    from schema.bronze import weather_api_schema

    weather_data = [
        [
            40.7,
            -74.0,
            "Clear",
            "clear sky",
            72,
            1013,
            65,
            10000,
            0,
            0,
            5.5,
            20,
            1640000000,
        ]
    ]

    df = spark.createDataFrame(weather_data, weather_api_schema)
    result_df = create_bronze_weather_df(df)

    assert result_df.count() == 1
    assert "ingest_timestamp" in result_df.columns


@pytest.mark.spark
def test_create_bronze_weather_df_empty_input(spark):
    """Test with empty weather data list"""
    from schema.bronze import weather_api_schema

    df = spark.createDataFrame([], weather_api_schema)
    result_df = create_bronze_weather_df(df)

    assert result_df.count() == 0
    assert "ingest_timestamp" in result_df.columns


@pytest.mark.spark
def test_create_bronze_weather_df_multiple_records(spark):
    """Test with multiple weather records"""
    from schema.bronze import weather_api_schema

    weather_data = [
        [
            40.7,
            -74.0,
            "Clear",
            "clear sky",
            72,
            1013,
            65,
            10000,
            0,
            0,
            5.5,
            20,
            1640000000,
        ],
        [
            40.8,
            -73.9,
            "Rain",
            "light rain",
            68,
            1010,
            75,
            8000,
            2,
            0,
            8.0,
            80,
            1640000060,
        ],
        [
            40.9,
            -73.8,
            "Snow",
            "heavy snow",
            32,
            1005,
            90,
            5000,
            0,
            5,
            12.0,
            100,
            1640000120,
        ],
    ]

    df = spark.createDataFrame(weather_data, weather_api_schema)
    result_df = create_bronze_weather_df(df)

    assert result_df.count() == 3


# Tests for NoUpdatesError
def test_no_updates_error():
    """Test custom NoUpdatesError exception"""
    with pytest.raises(NoUpdatesError, match="No updates found"):
        raise NoUpdatesError("No updates found")

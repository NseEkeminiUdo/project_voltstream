import pytest
from unittest.mock import patch, MagicMock
from utils.bronze import convert_to_json_string, add_bronze_stations_metadata, create_bronze_weather_df
from utils.shared import deduplicate
from utils.silver import (
    extract_col_from_json_string, select_station_columns, convert_lat_lon_type,
    explode_connections, select_conn_columns, add_weather_zone_coordinates,
    standardize_towns
)
from utils.gold import (
    join_tables,
    get_station_status_facts,
    get_station_dim,
    get_weather_dim,
    add_station_facts_metadata
)
from pyspark.sql.functions import col


@pytest.mark.integration
def test_bronze_to_silver_stations_flow(spark):
    """Test the end-to-end flow from bronze to silver layer for stations data"""
    # Create sample API data
    stations_api_data = [{"ID": 1,
                          "UUID": "uuid-1",
                          "AddressInfo": {"Title": "Station 1",
                                          "AddressLine1": "123 Main St",
                                          "Town": "New York",
                                          "StateOrProvince": "NY",
                                          "Latitude": 40.7128,
                                          "Longitude": -74.0060},
                          "Connections": [{"ConnectionTypeID": 1,
                                           "ConnectionType": {"Title": "Type1"},
                                           "LevelID": 2,
                                           "Amps": 32,
                                           "Voltage": 240,
                                           "PowerKW": 7.68,
                                           "Quantity": 2,
                                           "CurrentType": {"Title": "AC"}}],
                          "StatusType": {"Title": "Operational",
                                         "ID": 50},
                          "DateLastStatusUpdate": "2025-01-15T10:00:00",
                          "DateCreated": "2024-01-01T00:00:00",
                          "DataProvider": {"WebsiteURL": "http://provider.com"}},
                         {"ID": 2,
                          "UUID": "uuid-2",
                          "AddressInfo": {"Title": "Station 2",
                                          "AddressLine1": "456 Elm St",
                                          "Town": "Newark",
                                          "StateOrProvince": "NJ",
                                          "Latitude": 40.7357,
                                          "Longitude": -74.1724},
                          "Connections": [],
                          "StatusType": {"Title": "Available",
                                         "ID": 20},
                          "DateLastStatusUpdate": "2025-01-16T12:00:00",
                          "DateCreated": "2024-02-01T00:00:00",
                          "DataProvider": {"WebsiteURL": "http://provider.com"}}]

    # Bronze layer processing
    json_strings = convert_to_json_string(stations_api_data)
    # Create DataFrame from json strings
    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    bronze_df = add_bronze_stations_metadata(df)
    bronze_df = deduplicate(bronze_df, ["UUID"])

    # Verify bronze layer
    assert bronze_df.count() == 2
    assert "UUID" in bronze_df.columns
    assert "ingest_timestamp" in bronze_df.columns

    # Silver layer processing
    df = extract_col_from_json_string(bronze_df, "raw_text")
    silver_df = select_station_columns(df)
    silver_df = convert_lat_lon_type(silver_df)

    # Verify silver layer
    assert silver_df.count() == 2
    assert "station_id" in silver_df.columns
    assert "latitude" in silver_df.columns
    assert "longitude" in silver_df.columns

    # Verify data types
    lat_type = dict(silver_df.dtypes)["latitude"]
    lon_type = dict(silver_df.dtypes)["longitude"]
    assert lat_type == "double"
    assert lon_type == "double"

    # Verify state transformation
    nj_station = silver_df.filter(silver_df["station_id"] == 2).collect()[0]
    assert nj_station["state_or_province"] == "New Jersey"

    # Test connections explosion
    connections_df = explode_connections(
        df, layer="silver", job="test", dataset="connections")
    connections_df = select_conn_columns(connections_df)

    # Verify connections
    # Although there are no connections in the second record, station_id is
    # present
    assert connections_df.count() == 2
    assert "connection_type_id" in connections_df.columns
    assert "power_kw" in connections_df.columns


@pytest.mark.integration
def test_standardize_towns_integration(spark):
    """Test standardize_towns function integration in the silver layer pipeline"""
    # Skip test if geopandas not available
    try:
        import geopandas
    except ImportError:
        return

    # Create sample API data with town names that need standardization
    stations_api_data = [
        {
            "ID": 1,
            "UUID": "uuid-1",
            "AddressInfo": {
                "Title": "Station 1",
                "AddressLine1": "123 Main St",
                "Town": "NYC",  # Will be standardized to proper town name
                "StateOrProvince": "NY",
                "Latitude": 40.7128,
                "Longitude": -74.0060
            },
            "Connections": [],
            "StatusType": {"Title": "Operational", "ID": 50},
            "DateLastStatusUpdate": "2025-01-15T10:00:00",
            "DateCreated": "2024-01-01T00:00:00",
            "DataProvider": {"WebsiteURL": "http://provider.com"}
        },
        {
            "ID": 2,
            "UUID": "uuid-2",
            "AddressInfo": {
                "Title": "Station 2",
                "AddressLine1": "456 Elm St",
                "Town": "Nwk",  # Will be standardized to proper town name
                "StateOrProvince": "NJ",
                "Latitude": 40.7357,
                "Longitude": -74.1724
            },
            "Connections": [],
            "StatusType": {"Title": "Available", "ID": 20},
            "DateLastStatusUpdate": "2025-01-16T12:00:00",
            "DateCreated": "2024-02-01T00:00:00",
            "DataProvider": {"WebsiteURL": "http://provider.com"}
        }
    ]

    # Bronze layer processing
    json_strings = convert_to_json_string(stations_api_data)
    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    bronze_df = add_bronze_stations_metadata(df)
    bronze_df = deduplicate(bronze_df, ["UUID"])

    # Silver layer processing (up to standardize_towns)
    df = extract_col_from_json_string(bronze_df, "raw_text")
    silver_df = select_station_columns(df)
    silver_df = convert_lat_lon_type(silver_df)

    with patch('geopandas.read_parquet') as mock_read_parquet, \
            patch('geopandas.sjoin_nearest') as mock_sjoin_nearest:

        # Mock the clipped parquet file (geographic boundary data)
        mock_clipped = MagicMock()
        mock_read_parquet.return_value = mock_clipped

        # Mock the result of spatial join
        import pandas as pd
        mock_joined_data = pd.DataFrame({
            "station_id": [1, 2],
            "title": ["Station 1", "Station 2"],
            "address": ["123 Main St", "456 Elm St"],
            "state_or_province": ["New York", "New Jersey"],
            "latitude": [40.7128, 40.7357],
            "longitude": [-74.0060, -74.1724],
            "status": ["Operational", "Available"],
            "status_id": [50, 20],
            "date_last_status_update": ["2025-01-15T10:00:00", "2025-01-16T12:00:00"],
            "date_created": ["2024-01-01T00:00:00", "2024-02-01T00:00:00"],
            "name": ["Manhattan", "Newark"],  # Standardized town names
            "geometry": [None, None],  # Will be dropped
            "distance": [0.1, 0.2],  # Will be dropped
            "town": ["NYC", "Nwk"],  # Original town names (will be dropped)
            "index_right": [0, 1]  # Will be dropped
        })
        mock_sjoin_nearest.return_value = mock_joined_data

        # Apply standardize_towns
        standardized_df = standardize_towns(spark, silver_df)

        # Verify standardize_towns was called with correct arguments
        mock_read_parquet.assert_called_once_with(
            "/Volumes/bronze_dev/superstor_schema/raw_superstore/clipped.parquet")
        mock_sjoin_nearest.assert_called_once()

        # Verify the integration results
        assert standardized_df.count() == 2
        assert "town" in standardized_df.columns

        # Verify dropped columns are not present
        assert "geometry" not in standardized_df.columns
        assert "distance" not in standardized_df.columns
        assert "index_right" not in standardized_df.columns

        # Verify town names are standardized
        rows = standardized_df.collect()
        town_names = [row["town"] for row in rows]
        assert "Manhattan" in town_names
        assert "Newark" in town_names

        # Verify original data integrity (other columns unchanged)
        assert standardized_df.filter(col("station_id") == 1).collect()[
            0]["title"] == "Station 1"
        assert standardized_df.filter(col("station_id") == 2).collect()[
            0]["state_or_province"] == "New Jersey"


@pytest.mark.integration
def test_bronze_to_silver_weather_flow(spark):
    """Test the end-to-end flow from bronze to silver layer for weather data"""
    # Create sample weather API data
    weather_data = [
        {
            "lat": 40.7,
            "lon": -74.0,
            "weather": "Clear",
            "description": "clear sky",
            "temp": 72.5,
            "pressure": 1013,
            "humidity": 65,
            "visibility": 10000,
            "rain": 0.0,
            "snow": 0.0,
            "wind_speed": 5.5,
            "clouds": 10,
            "dt": 1705320000
        },
        {
            "lat": 40.8,
            "lon": -74.1,
            "weather": "Rain",
            "description": "light rain",
            "temp": 68.0,
            "pressure": 1010,
            "humidity": 85,
            "visibility": 8000,
            "rain": 2.5,
            "snow": 0.0,
            "wind_speed": 8.0,
            "clouds": 75,
            "dt": 1705320000
        }
    ]

    # Bronze layer processing - create DataFrame directly
    bronze_weather_df = spark.createDataFrame(weather_data)
    bronze_weather_df = create_bronze_weather_df(bronze_weather_df)

    # Verify bronze layer
    assert bronze_weather_df.count() == 2
    assert "lat" in bronze_weather_df.columns
    assert "lon" in bronze_weather_df.columns
    assert "ingest_timestamp" in bronze_weather_df.columns

    # Verify data content
    clear_weather = bronze_weather_df.filter(
        bronze_weather_df["weather"] == "Clear").collect()[0]
    assert clear_weather["temp"] == 72.5
    assert clear_weather["humidity"] == 65

    rainy_weather = bronze_weather_df.filter(
        bronze_weather_df["weather"] == "Rain").collect()[0]
    assert rainy_weather["rain"] == 2.5


@pytest.mark.integration
def test_silver_to_gold_complete_flow(spark):
    """Test the complete flow from silver to gold layer including all transformations"""
    # Create silver stations data with weather_zone coordinates and address
    # fields
    from pyspark.sql.functions import to_date
    stations_data = [
        (1, "Station 1", "123 Main St", "New York", "New York", 40.7, -74.0,
         40.7, -74.0, 50, "2025-01-15", "2025-01-01", "2025-01-15",
         "2999-12-31"),
        (2, "Station 2", "456 Elm St", "Newark", "New Jersey", 40.8, -74.1,
         40.8, -74.1, 30, "2025-01-16", "2025-01-02", "2025-01-16",
         "2999-12-31")]
    stations_df = spark.createDataFrame(
        stations_data,
        ["station_id", "title", "address", "town", "state_or_province",
         "latitude", "longitude", "weather_zone_lat", "weather_zone_lon",
         "status_id", "date_last_status_update", "date_created", "vf", "vt"])
    stations_df = stations_df.withColumn("valid_from", to_date(col("vf")))\
        .withColumn("valid_to", to_date(col("vt")))\
        .drop("vf", "vt")

    # Create silver weather data (after transform_weather)
    from pyspark.sql.functions import to_timestamp
    weather_data = [
        (40.7, -74.0, 1, "Clear", "clear sky", 72.5, 1013, 65, 10000, 0.0, 0.0,
         5.5, 10, "2025-01-15 10:00:00", 123456),
        (40.8, -74.1, 2, "Rain", "light rain", 68.0, 1010, 85, 8000, 2.5, 0.0,
         8.0, 75, "2025-01-16 12:00:00", 234567)]
    weather_df = spark.createDataFrame(
        weather_data,
        ["lat", "lon", "weather_zone_id", "weather", "description", "temp",
         "pressure", "humidity", "visibility", "rain", "snow", "wind_speed",
         "clouds", "dt_str", "weather_sk"])
    weather_df = weather_df.withColumn(
        "dt_utc", to_timestamp(
            col("dt_str"))).drop("dt_str")

    # Create silver connectors data with valid_from and valid_to
    connectors_data = [
        (1, 1, "Type1", 2, 32, 240, 7.68, 2, "AC", "2025-01-15", "2999-12-31"),
        (2, 2, "CHAdeMO", 3, 125, 500, 62.5, 1, "DC", "2025-01-16", "2999-12-31")
    ]
    connectors_df = spark.createDataFrame(
        connectors_data,
        ["station_id", "connection_type_id", "connection_type", "level_id",
         "amps", "voltage", "power_kw", "quantity", "current_type", "vf",
         "vt"])
    connectors_df = connectors_df.withColumn("valid_from", to_date(col("vf")))\
        .withColumn("valid_to", to_date(col("vt")))\
        .drop("vf", "vt")

    # Gold layer processing - use join_tables which does everything
    joined_df = join_tables(stations_df, connectors_df, weather_df)

    # Generate facts and dimensions
    facts_df = get_station_status_facts(joined_df)
    facts_df = add_station_facts_metadata(facts_df)
    station_dim_df = get_station_dim(joined_df)
    weather_dim_df = get_weather_dim(joined_df)

    # Validate facts - join_tables filters to only operational (50) or
    # temporarily unavailable (30)
    assert facts_df.count() == 2
    assert "risk_score" in facts_df.columns
    assert "avg_energy_cost_per_hour" in facts_df.columns
    assert "availability" in facts_df.columns

    # Validate dimensions
    assert station_dim_df.count() == 2
    assert weather_dim_df.count() == 2

    # Verify business logic - both stations should be in results since
    # join_tables includes status 30 and 50
    station_1_facts = facts_df.filter(col("station_id") == 1).collect()[0]
    # Status 50 is operational
    assert station_1_facts["is_operational"] is True

    station_2_facts = facts_df.filter(col("station_id") == 2).collect()[0]
    # Status 30 is not operational
    assert station_2_facts["is_operational"] is False


@pytest.mark.integration
def test_end_to_end_data_quality(spark):
    """Test end-to-end data quality across all layers"""
    # Create sample data through the entire pipeline
    stations_api_data = [
        {
            "ID": 1,
            "UUID": "uuid-1",
            "AddressInfo": {
                "Title": "Quality Test Station",
                "AddressLine1": "789 Test Blvd",
                "Town": "Test City",
                "StateOrProvince": "NY",
                "Latitude": 40.7500,
                "Longitude": -74.0000
            },
            "Connections": [{"type": "CHAdeMO"}],
            "StatusType": {"Title": "Operational", "ID": 50},
            "DateLastStatusUpdate": "2025-01-01T10:00:00",
            "DateCreated": "2024-01-01T00:00:00",
            "DataProvider": {"WebsiteURL": "http://provider.com"}
        }
    ]

    # Bronze layer
    json_strings = convert_to_json_string(stations_api_data)
    # Create DataFrame from json strings
    df = spark.createDataFrame([(s,) for s in json_strings], ["raw_text"])
    bronze_df = add_bronze_stations_metadata(df)
    bronze_df = deduplicate(bronze_df, ["UUID"])

    # Data quality check: No duplicates
    assert bronze_df.count() == 1

    # Silver layer
    silver_df = extract_col_from_json_string(bronze_df, "raw_text")
    silver_df = select_station_columns(silver_df)
    silver_df = convert_lat_lon_type(silver_df)

    # Data quality check: All required columns present
    required_cols = ["station_id", "latitude", "longitude", "status_id"]
    for col_name in required_cols:
        assert col_name in silver_df.columns

    # Data quality check: No null values in key columns
    for col_name in required_cols:
        null_count = silver_df.filter(silver_df[col_name].isNull()).count()
        assert null_count == 0

    # Data quality check: Latitude/longitude are valid doubles
    lat_type = dict(silver_df.dtypes)["latitude"]
    lon_type = dict(silver_df.dtypes)["longitude"]
    assert lat_type == "double"
    assert lon_type == "double"


@pytest.mark.integration
def test_incremental_updates_idempotency(spark):
    """Test that the pipeline handles incremental updates correctly"""
    # This test simulates receiving the same data twice and verifies
    # deduplication

    # First batch
    stations_data_batch1 = [
        {"ID": 1, "UUID": "uuid-1", "AddressInfo": {"Title": "Station 1"}}
    ]

    # Second batch (duplicate)
    stations_data_batch2 = [
        {"ID": 1, "UUID": "uuid-1", "AddressInfo": {"Title": "Station 1"}}
    ]

    # Process first batch
    json_strings_1 = convert_to_json_string(stations_data_batch1)
    df1 = spark.createDataFrame([(s,) for s in json_strings_1], ["raw_text"])
    bronze_df1 = add_bronze_stations_metadata(df1)

    # Process second batch
    json_strings_2 = convert_to_json_string(stations_data_batch2)
    df2 = spark.createDataFrame([(s,) for s in json_strings_2], ["raw_text"])
    bronze_df2 = add_bronze_stations_metadata(df2)

    # Union both batches
    combined_df = bronze_df1.union(bronze_df2)

    # Apply deduplication
    deduplicated_df = deduplicate(combined_df, ["UUID"])

    # Verify deduplication worked
    assert deduplicated_df.count() == 1

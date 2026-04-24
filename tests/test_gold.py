import pytest
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.functions import col

from utils.gold import (
    join_tables,
    get_station_status_facts,
    add_station_facts_metadata,
    get_station_dim,
    get_weather_dim,
)


# Tests for join_tables
def test_join_tables(spark):
    """Test joining stations, connections, and weather tables"""
    # Create stations dataframe
    stations_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("status_id", IntegerType(), True),
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
        ]
    )
    df_stations = spark.createDataFrame(
        [
            (1, 50, 40.7, -74.0),  # Operational
            (2, 30, 40.8, -73.9),  # Temporarily unavailable
            (3, 20, 40.9, -73.8),  # Not operational (should be filtered)
        ],
        stations_schema,
    )

    # Create connections dataframe
    connections_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("power_kw", DoubleType(), True),
        ]
    )
    df_connections = spark.createDataFrame(
        [(1, 50.0), (2, 100.0), (3, 75.0)], connections_schema
    )

    # Create weather dataframe
    weather_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
        ]
    )
    df_weather = spark.createDataFrame(
        [(40.7, -74.0, "Clear"), (40.8, -73.9, "Cloudy"), (40.9, -73.8, "Rainy")],
        weather_schema,
    )

    result_df = join_tables(df_stations, df_connections, df_weather)

    # Should only have stations with status 50 or 30
    assert result_df.count() == 2

    # Verify is_operational column
    assert "is_operational" in result_df.columns


def test_join_tables_operational_flag(spark):
    """Test is_operational flag is correctly set"""
    # Create minimal test data
    stations = spark.createDataFrame(
        [(1, 50, 40.7, -74.0), (2, 30, 40.7, -74.0)],
        ["station_id", "status_id", "weather_zone_lat", "weather_zone_lon"],
    )
    connections = spark.createDataFrame(
        [(1, 50.0), (2, 100.0)], ["station_id", "power_kw"]
    )
    weather = spark.createDataFrame([(40.7, -74.0, "Clear")], ["lat", "lon", "weather"])

    result_df = join_tables(stations, connections, weather)

    # Station with status_id 50 should be operational
    # Use backticks for column names with dots
    operational_status = (
        result_df.filter(col("s.station_id") == 1)
        .select("is_operational")
        .collect()[0][0]
    )
    assert operational_status is True

    # Station with status_id 30 should not be operational
    non_operational_status = (
        result_df.filter(col("s.station_id") == 2)
        .select("is_operational")
        .collect()[0][0]
    )
    assert non_operational_status is False


# Tests for get_station_status_facts
def test_get_station_status_facts(spark):
    """Test creating station status facts from joined data"""
    # Create input dataframes
    stations_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("status_id", IntegerType(), True),
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("date_last_status_update", TimestampType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("date_created", TimestampType(), True),
        ]
    )
    df_stations = spark.createDataFrame(
        [
            (
                1,
                50,
                40.7,
                -74.0,
                datetime(2025, 1, 1),
                datetime(2025, 1, 1),
                datetime(2024, 1, 1),
            )
        ],
        stations_schema,
    )

    connections_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("power_kw", DoubleType(), True),
            StructField("valid_to", TimestampType(), True),
            StructField("level_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    df_connections = spark.createDataFrame(
        [(1, 50.0, datetime(2025, 1, 2), 2, 2)], connections_schema
    )

    weather_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("weather_sk", IntegerType(), True),
            StructField("weather_zone_id", IntegerType(), True),
            StructField("dt_utc", TimestampType(), True),
            StructField("description", StringType(), True),
            StructField("temp", DoubleType(), True),
        ]
    )
    df_weather = spark.createDataFrame(
        [(40.7, -74.0, "Clear", 1, 100, datetime(2025, 1, 1), "clear sky", 72.0)],
        weather_schema,
    )

    # Get joined dataframe
    joined_df = join_tables(df_stations, df_connections, df_weather)

    # Get station status facts
    result_df = get_station_status_facts(joined_df)

    # Verify expected columns (output has grouping column plus aggregated
    # columns)
    expected_cols = [
        "station_id",
        "weather_zone_id",
        "weather_sk",
        "is_operational",
        "offline_count",
        "offline_duration",
        "total_duration",
        "risk_score",
        "risk_severity_level",
        "avg_energy_cost_per_hour",
    ]
    for col_name in expected_cols:
        assert col_name in result_df.columns, f"Column {col_name} not found in result"

    assert result_df.count() == 1


# Tests for add_station_facts_metadata
def test_add_station_facts_metadata(spark):
    """Test adding business metadata to station facts"""
    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("avg_energy_cost_per_hour", DoubleType(), True),
            StructField("offline_duration", DoubleType(), True),
            StructField("total_duration", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(1, 100.0, 60.0, 1000.0)], schema  # 60 min offline out of 1000 min total
    )

    result_df = add_station_facts_metadata(df)

    # Verify new columns
    assert "est_downtime_cost" in result_df.columns
    assert "availability" in result_df.columns
    assert "ingest_timestamp" in result_df.columns

    # Verify calculations
    row = result_df.collect()[0]
    downtime_cost = row["est_downtime_cost"]
    availability = row["availability"]

    # est_downtime_cost = 100.0 * 60 / 60 = 100.0
    assert downtime_cost == 100.0

    # availability = 1 - (60/1000) = 0.94
    assert availability == 0.94


# Tests for get_station_dim
def test_get_station_dim(spark):
    """Test creating station dimension from joined data"""
    # Create input dataframes with station attributes
    stations_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("status_id", IntegerType(), True),
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("title", StringType(), True),
            StructField("address", StringType(), True),
            StructField("town", StringType(), True),
            StructField("state_or_province", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )
    df_stations = spark.createDataFrame(
        [
            (
                1,
                50,
                40.7,
                -74.0,
                "Station A",
                "123 Main St",
                "New York",
                "NY",
                40.7,
                -74.0,
            ),
            (
                2,
                50,
                40.8,
                -73.9,
                "Station B",
                "456 Oak Ave",
                "Newark",
                "NJ",
                40.8,
                -73.9,
            ),
        ],
        stations_schema,
    )

    connections_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("power_kw", DoubleType(), True),
        ]
    )
    df_connections = spark.createDataFrame(
        [(1, 50.0), (1, 100.0), (2, 75.0)],  # Same station, different connector
        connections_schema,
    )

    weather_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
        ]
    )
    df_weather = spark.createDataFrame(
        [(40.7, -74.0, "Clear"), (40.8, -73.9, "Cloudy")], weather_schema
    )

    # Get joined dataframe
    joined_df = join_tables(df_stations, df_connections, df_weather)

    # Get station dimension
    result_df = get_station_dim(joined_df)

    # Should group by station_id
    assert result_df.count() == 2

    # Verify expected columns (grouping column plus aggregated columns)
    expected_cols = [
        "station_id",
        "title",
        "address",
        "town",
        "state_or_province",
        "latitude",
        "longitude",
        "max_power_kw",
    ]
    for col_name in expected_cols:
        assert col_name in result_df.columns, f"Column {col_name} not found in result"

    # Verify max_power_kw for station 1 is 100.0
    # Use backticks for dotted column names
    station_1 = result_df.filter(col("s.station_id") == 1).collect()[0]
    assert station_1["max_power_kw"] == 100.0


# Tests for get_weather_dim
def test_get_weather_dim(spark):
    """Test creating weather dimension from joined data"""
    # Create input dataframes with weather attributes
    stations_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("status_id", IntegerType(), True),
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
        ]
    )
    df_stations = spark.createDataFrame(
        [(1, 50, 40.7, -74.0), (2, 50, 40.8, -73.9)], stations_schema
    )

    connections_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("power_kw", DoubleType(), True),
        ]
    )
    df_connections = spark.createDataFrame([(1, 50.0), (2, 100.0)], connections_schema)

    weather_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("weather_sk", IntegerType(), True),
            StructField("weather_zone_id", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("temp", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("visibility", IntegerType(), True),
            StructField("rain", DoubleType(), True),
            StructField("snow", DoubleType(), True),
        ]
    )
    df_weather = spark.createDataFrame(
        [
            (
                40.7,
                -74.0,
                "Clear",
                1,
                100,
                "clear sky",
                72.0,
                1013,
                65,
                10000,
                0.0,
                0.0,
            ),
            (
                40.7,
                -74.0,
                "Clear",
                1,
                100,
                "clear sky",
                72.0,
                1013,
                65,
                10000,
                0.0,
                0.0,
            ),  # Duplicate
            (
                40.8,
                -73.9,
                "Cloudy",
                2,
                101,
                "broken clouds",
                68.0,
                1010,
                70,
                8000,
                0.0,
                0.0,
            ),
        ],
        weather_schema,
    )

    # Get joined dataframe
    joined_df = join_tables(df_stations, df_connections, df_weather)

    # Get weather dimension
    result_df = get_weather_dim(joined_df)

    # Should group by weather_sk
    assert result_df.count() == 2

    # Verify expected columns (grouping column plus aggregated columns)
    expected_cols = [
        "weather_sk",
        "weather_zone_id",
        "weather",
        "description",
        "temp",
        "pressure",
        "humidity",
        "visibility",
        "rain",
        "snow",
    ]
    for col_name in expected_cols:
        assert col_name in result_df.columns, f"Column {col_name} not found in result"


# Tests for risk score calculation logic
def test_risk_score_calculation(spark):
    """Test risk score calculation in station status facts"""
    # Create input dataframes with high risk factors
    stations_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("status_id", IntegerType(), True),
            StructField("weather_zone_lat", DoubleType(), True),
            StructField("weather_zone_lon", DoubleType(), True),
            StructField("date_last_status_update", TimestampType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("date_created", TimestampType(), True),
        ]
    )
    df_stations = spark.createDataFrame(
        [
            (
                1,
                50,
                40.7,
                -74.0,
                datetime(2025, 1, 1),
                datetime(2025, 1, 1),
                datetime(2024, 1, 1),
            )
        ],
        stations_schema,
    )

    connections_schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("power_kw", DoubleType(), True),
            StructField("valid_to", TimestampType(), True),
            StructField("level_id", IntegerType(), True),  # Level 3 adds 25 points
            StructField("quantity", IntegerType(), True),
        ]
    )
    df_connections = spark.createDataFrame(
        [(1, 50.0, datetime(2025, 1, 2), 3, 2)], connections_schema
    )

    weather_schema = StructType(
        [
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("weather_sk", IntegerType(), True),
            StructField("weather_zone_id", IntegerType(), True),
            StructField("dt_utc", TimestampType(), True),
            StructField("description", StringType(), True),  # Storm adds 40 points
            StructField("temp", DoubleType(), True),  # Extreme temp adds 25 points
        ]
    )
    df_weather = spark.createDataFrame(
        [
            (
                40.7,
                -74.0,
                "Thunderstorm",
                1,
                100,
                datetime(2025, 1, 1),
                "thunder storm",
                100.0,
            )
        ],
        weather_schema,
    )

    # Get joined dataframe
    joined_df = join_tables(df_stations, df_connections, df_weather)

    # Get station status facts
    result_df = get_station_status_facts(joined_df)

    # Risk score should be high
    risk_score = result_df.select("risk_score").collect()[0][0]
    assert risk_score >= 75  # High risk threshold

    # Risk severity level should be "high"
    risk_level = result_df.select("risk_severity_level").collect()[0][0]
    assert risk_level == "high"

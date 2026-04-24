from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    TimestampType,
)

# Define schema for OpenWeatherMap API response
weather_api_schema = StructType(
    [
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
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
        StructField("dt", LongType(), True),
    ]
)

stations_bronze_schema = StructType(
    [
        StructField("UUID", LongType(), False),
        StructField("raw_text", StringType(), False),
        StructField("source", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)

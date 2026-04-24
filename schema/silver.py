from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    StructType,
    StructField,
    LongType,
    DateType,
    TimestampType,
    BooleanType,
)

explode_conn_schema = ArrayType(
    StructType(
        [
            StructField("ID", IntegerType()),
            StructField("ConnectionTypeID", IntegerType()),
            StructField("LevelID", IntegerType()),
            StructField("Amps", IntegerType()),
            StructField("Voltage", DoubleType()),
            StructField("PowerKW", DoubleType()),
            StructField(
                "CurrentType",
                StructType(
                    [
                        StructField("ID", IntegerType()),
                        StructField("Title", StringType()),
                    ]
                ),
            ),
            StructField("Quantity", IntegerType()),
            StructField(
                "ConnectionType",
                StructType(
                    [
                        StructField("ID", IntegerType()),
                        StructField("Title", StringType()),
                    ]
                ),
            ),
        ]
    )
)


conn_table_schema = StructType(
    [
        StructField("connection_sk", LongType()),
        StructField("connection_type_id", IntegerType()),
        StructField("connection_type", StringType()),
        StructField("level_id", IntegerType()),
        StructField("station_id", StringType()),
        StructField("amps", IntegerType()),
        StructField("voltage", DoubleType()),
        StructField("power_kw", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("current_type", StringType()),
        StructField("valid_from", DateType()),
        StructField("valid_To", DateType()),
        StructField("is_current", BooleanType()),
        StructField("ingest_timestamp", TimestampType()),
    ]
)


stations_table_schema = StructType(
    [
        StructField("station_sk", LongType()),
        StructField("station_id", StringType()),
        StructField("title", StringType()),
        StructField("address", StringType()),
        StructField("town", StringType()),
        StructField("state_or_province", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("weather_zone_lat", DoubleType()),
        StructField("weather_zone_lon", DoubleType()),
        StructField("status", StringType()),
        StructField("status_id", IntegerType()),
        StructField("date_created", TimestampType()),
        StructField("date_last_status_update", TimestampType()),
        StructField("valid_from", DateType()),
        StructField("valid_to", DateType()),
        StructField("is_current", BooleanType()),
        StructField("ingest_timestamp", TimestampType()),
    ]
)


weather_table_schema = StructType(
    [
        StructField("weather_sk", LongType(), False),
        StructField("weather_zone_id", LongType(), False),
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
        StructField("dt_utc", TimestampType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)

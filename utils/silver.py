from datetime import datetime
from pyspark.sql.functions import col, when, lit, get_json_object, date_sub, xxhash64, explode_outer, from_json, from_unixtime, round
from pyspark.sql.types import DoubleType, TimestampType
from schema.silver import explode_conn_schema
from logger.custom_logging import set_up_logger, get_job_logger
from utils.shared import write_to_table
import logging

"""
This file contains functions to be used in the silver layer of project voltstream
"""

try:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    run_id = context.tags().get("runId").get()
except BaseException:
    # Fallback for when running as a file (not a notebook)
    run_id = "unknown"

logger = set_up_logger()


# This function extracts the relevant columns for analysis from the json
# string in the bronze table
def extract_col_from_json_string(df, json_string: str):
    relevant_columns = {
        "ID": "station_id",
        "AddressInfo['Title']": "title",
        "AddressInfo['AddressLine1']": "address",
        "AddressInfo['Town']": "town",
        "AddressInfo['StateOrProvince']": "state_or_province",
        "AddressInfo['Latitude']": "latitude",
        "AddressInfo['Longitude']": "longitude",
        "Connections": "connections",
        "StatusType['Title']": "status",
        "StatusType['ID']": "status_id",
        "DateLastStatusUpdate": "date_last_status_update",
        "DateCreated": "date_created"
    }

    df = df.select([get_json_object(col(json_string), f"$.{key}").alias(
        value) for key, value in relevant_columns.items()])
    return df


# This function validates the columns in the dataframe to ensure that they
# are not empty
def validate_columns(df, **log_info):
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)
    for field in df.columns:
        null_count = df.filter(col(field).isNotNull()).count()
        if null_count == 0:
            log(logging.ERROR,
                f"Pipeline stopped: column '{field}' is all NULL")
            raise RuntimeError(
                f"Pipeline stopped: column '{field}' is all NULL")
    return df


# This function validates the records in the dataframe and quarantines the
# invalid records
def validate_and_quarantine_rows(
        df,
        required_cols,
        non_negative_cols=None,
        **log_info):
    if non_negative_cols is None:
        non_negative_cols = []
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)

    # Check columns are not null
    valid_expr = lit(True)
    for field in required_cols:
        valid_expr = valid_expr & col(field).isNotNull()

    # Check columns are not negative
    for field in non_negative_cols:
        valid_expr = valid_expr & (col(field) >= 0)

    df = df.withColumn("valid_record", valid_expr)

    # Split into valid and invalid
    df_valid = df.filter(col("valid_record"))
    df_quarantine = df.filter(~col("valid_record"))

    # Optional: drop the 'valid_record' column for downstream
    df_valid = df_valid.drop("valid_record")
    df_quarantine = df_quarantine.drop("valid_record")
    if "power_kw" in df.columns:
        table = "connectors"
    else:
        table = "stations"
    write_to_table(
        df_quarantine,
        f"silver_dev.electrovolt.quarantined_{table}",
        "append",
        **log_info)

    log(logging.INFO, f"found {df_valid.count()} valid rows")
    return df_valid, df_quarantine


# This function loads the current data of all the stations in the silver table
def load_current_data(df, **log_info):
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)
    df = df.filter(col("Is_Current"))
    log(logging.INFO, f"{df.count()} current records loaded")
    return df


# This function function joins the incoming data from the bronze table
# with the current data in the silver table
def join_current_and_incoming_data(stg_dim, current_dim, id: str | list):
    if isinstance(id, str):
        id = [id]
    return stg_dim.alias('stg').join(
        current_dim.alias('current'),
        on=[*id],
        how='left'
    )


# This function add scd logic to the joined dataframe in order to set up
# idempotency
def add_scd_logic(df, id, attributes: str | list):
    if isinstance(attributes, str):
        attributes = [attributes]
    # Handle id as list for null check
    id_col = id[0] if isinstance(id, list) else id
    scd_action = when(col(f"current.{id_col}").isNull(), "INSERT")
    for attr in attributes:
        scd_action = scd_action.when(
            col(f"stg.{attr}") != col(f"current.{attr}"), "UPDATE"
        )
    df = df.withColumn("scd_action", scd_action)
    return df


# This function adds surrogate key, valid_from, valid_to and is_current
# columns in order to track expired and current data for idempotency
def add_columns_for_idempotency(df, id: list, alias: str, attributes: list):
    return (df.filter(col("scd_action").isin(["INSERT", "UPDATE"]))
            .select((xxhash64(*[col(f"{c}") for c in id])).alias(alias),
                    *[col(f"stg.{c}") for c in attributes],
                    col("stg.date_last_status_update").alias("valid_from"),
                    lit('2999-12-31').cast('date').alias("valid_to"),
                    lit(True).alias("is_current")))


# This function identifies rows to expire based on the scd_action column
def identify_rows_to_expire(df, id: str):
    return df.filter(
        col("scd_action") == "UPDATE").select(
        col(id),
        date_sub(
            col("valid_from"),
            1).alias("valid_to"))


# This function updates the valid_to and is_current columns for expired data
def update_expired_rows(dim_delta, expire_df, id, **log_info):
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)
    dim_delta.alias("current").merge(
        expire_df.alias("updates"),
        f"current.{id} = updates.{id}"
    ).whenMatchedUpdate(
        set={
            "valid_to": col("updates.valid_to"),
            "is_current": lit(False)
        }
    ).execute()
    log(logging.INFO, f"{expire_df.count()} records expired in {dim_delta}")


# This function inserts the new rows into the silver table
def insert_new_rows(delta_dim, inserts_df, id, **log_info):
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)
    df = delta_dim.alias("current").merge(
        inserts_df.alias("updates"),
        f"1 == 0",
    ).whenNotMatchedInsertAll().execute()
    log(logging.INFO,
        f"{inserts_df.count()} records inserted into {delta_dim}")


def standardize_towns(spark, df):
    import geopandas as gpd
    import pandas as pd

    clipped = gpd.read_parquet(
        "/Workspace/Users/nseekeminiudo@gmail.com/project_volltstream/extras/clipped.parquet")
    clipped = gpd.GeoDataFrame(
        clipped,
        geometry="geometry",
        crs="EPSG:2263"
    )

    # Load Spark EV stations
    pdf = df.toPandas()

 # Create GeoDataFrame (IMPORTANT: lat/lon is EPSG:2263)
    gdf = gpd.GeoDataFrame(
        pdf,
        geometry=gpd.points_from_xy(pdf.longitude, pdf.latitude),
        crs="EPSG:2263"
    )

    print(clipped.crs)
    print(gdf.crs)

    # Spatial nearest join
    joined = gpd.sjoin_nearest(
        gdf,
        clipped,
        how="left",
        distance_col="distance"
    )

    # Back to Spark
    spark_df_final = spark.createDataFrame(
        joined.drop(
            columns=[
                "geometry",
                "distance",
                "town",
                "index_right"]))
    df = spark_df_final.withColumnRenamed("name", "town")
    return df


# This function selects only the columns needed for the stations table
def select_station_columns(df):
    cols = [
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
    df = df.select(
        *
        cols).withColumn(
        "state_or_province",
        when(
            col("state_or_province") == "NJ",
            "New Jersey").when(
                col("state_or_province") == "NY",
                "New York").otherwise(
                    col("state_or_province")))
    return df


# This function removes row with null values for latitude and longitude
def remove_nulls(df):
    return (df.filter(col("longitude").isNotNull())
            .filter(col("latitude").isNotNull()))


# This function converts the latitude and longitude columns to double type
def convert_lat_lon_type(df):
    return (df.withColumn("latitude", col("latitude").cast(DoubleType()))
            .withColumn("longitude", col("longitude").cast(DoubleType())))


# This function groups the stations to weather zones by rounding up their
# latitude and longitude to one decimal place
def add_weather_zone_coordinates(df):
    return (df.withColumn("weather_zone_lat", round(col("latitude"), 1))
            .withColumn("weather_zone_lon", round(col("longitude"), 1)))


# This function explodes the connections column in order to flatten the data
def explode_connections(df, **log_info):
    log = get_job_logger(
        logger, layer=log_info["layer"],
        job=log_info["job"],
        dataset=log_info["dataset"],
        run_id=run_id)
    df = df.select(
        explode_outer(
            from_json(col("Connections"),
                      explode_conn_schema)).alias("conn"),
        col("station_id"),
        col("date_last_status_update")).withColumn(
        "ingest_timestamp", lit(datetime.now()))

    log(logging.INFO, f"{df.count()} records exploded")
    return df


# This function selects the relevant columns from the exploded dataframe
# for the connections table
def select_conn_columns(df):
    cols = {
        "connection_type_id": "conn.ConnectionTypeID",
        "connection_type": "conn.ConnectionType.Title",
        "level_id": "conn.LevelID",
        "amps": "conn.Amps",
        "voltage": "conn.Voltage",
        "power_kw": "conn.PowerKW",
        "quantity": "conn.Quantity",
        "current_type": "conn.CurrentType.Title"
    }

    return df.select(
        *[col(v).alias(k) for k, v in cols.items()],
        col("station_id"),
        col("date_last_status_update"),
        col("ingest_timestamp")
    )


# This function transforms the weather data into the appropriate format
def transform_weather(df):
    column_exprs = {
        "weather_sk": xxhash64(
            col("lat"),
            col("lon"),
            col("ingest_timestamp")),
        "lat": round(
            col("lat"),
            1),
        "lon": round(
            col("lon"),
            1),
        "weather": col("weather"),
        "description": col("description"),
        "temp": col("temp"),
        "pressure": col("pressure"),
        "humidity": col("humidity"),
        "visibility": col("visibility"),
        "rain": col("rain"),
        "snow": col("snow"),
        "wind_speed": col("wind_speed"),
        "clouds": col("clouds"),
        "dt_utc": from_unixtime(
            col("dt")).cast(
            TimestampType()),
    }

    df = df.select(
        *[expr.alias(name) for name, expr in column_exprs.items()]
    ).withColumn("ingest_timestamp", lit(datetime.now()))\
     .withColumn("weather_zone_id", xxhash64(col("lat"), col("lon")))

    return df

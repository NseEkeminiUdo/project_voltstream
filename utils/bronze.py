from datetime import datetime, timedelta
import numpy as np
from pyspark.sql.functions import col, lit, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import json
from schema.bronze import weather_api_schema
from utils.shared import fetch_data, write_to_table
from logger.custom_logging import set_up_logger, get_job_logger
import time
import logging

"""
This file contains the functions to be used in the bronze layer of the pipeline
"""

try:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark:
        run_id = spark.conf.get(
            "spark.databricks.clusterUsageTags.runId",
            spark.conf.get("pipeline_run_id", "unknown"),
        )
    else:
        run_id = "unknown"
except Exception:
    run_id = "unknown"

logger = set_up_logger()


class NoUpdatesError(Exception):
    # Custom exception to signal no updates in the pipeline step
    pass


# Due to inadequate pagination provision in the API, we need to manually
# paginate the results with a grid search
def generate_grid(min_lat, min_lon, max_lat, max_lon):
    grid = []
    lat_range = np.arange(min_lat, max_lat, 0.1)
    lon_range = np.arange(min_lon, max_lon, 0.1)
    for lat in lat_range:
        for lon in lon_range:
            grid.append((lat, lon))
    return grid


# This function add a buffer to the last ingested timestamp to avoid
# missing updates
def add_buffer(timestamp):
    return timestamp - timedelta(minutes=15)


# This function loops through the generated grid and fetches the data from
# the API
def get_all_stations_data(grid, last_timestamp, api_key, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    all_stations = []  # List to store all the updated stations
    ocm_url = "https://api.openchargemap.io/v3/poi"  # ocm = openchargemap
    distance = 10
    maxresults = 750
    last_timestamp = last_timestamp
    for latitude, longitude in grid:
        ocm_params = {
            "latitude": latitude,
            "longitude": longitude,
            "distance": distance,
            "unit": "KM",
            "modifiedsince": last_timestamp,
            "compact": "false",
            "maxresults": maxresults,
            "output": "json",
            "key": api_key,  # load API from databricks secrets
        }

        ocm_data = fetch_data(ocm_url, ocm_params, **log_info)

        if not ocm_data:  # If no station found at this coordinates, move on
            continue

        all_stations.extend(ocm_data)  # Append all pulled stations to list

    if len(all_stations) == 0:
        log(
            logging.WARNING,
            "No new data to ingest, keeping current state at timestamp{last_timestamp}",
        )
        return None
    else:
        log(logging.INFO, f"Found {len(all_stations)} rows")
        return all_stations


# This function converts the json response from each station to a json string
def convert_to_json_string(all_stations):
    return [json.dumps(station) for station in all_stations]


# This function creates a dataframe from the json string with the schema
# expected by the bronze layer
def add_bronze_stations_metadata(df):
    # Add station metadata columns to dataframe
    return (
        df.withColumn("UUID", get_json_object(col("raw_text"), "$.UUID"))
        .withColumn("ingest_timestamp", lit(datetime.now()))
        .withColumn(
            "source_file",
            get_json_object(col("raw_text"), "$.DataProvider['WebsiteURL']"),
        )
        .coalesce(1)
        .dropDuplicates(["UUID"])
    )


# This function extracts the weather lat and lon from the dataframe
def get_weather_zone(df, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    df = df.select("weather_zone_lat", "weather_zone_lon").dropDuplicates().collect()
    log(logging.INFO, f"Found {len(df)} weather zones")
    return df


# This function creates a dataframe from the weather zone lat and lon with
# the schema expected by the bronze layer)


# This function fetches the weather data from the open weather map API for
# each weather zone
def get_weather_zone_data(weather_zone, api_key, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    api_url = "https://api.openweathermap.org/data/2.5/weather?"
    all_zones = []
    for lat, lon in weather_zone:
        api_params = f"lat={lat}&lon={lon}&appid={api_key}&units=imperial"
        response = fetch_data(api_url, api_params, **log_info)
        try:
            lat = response["coord"]["lat"]
            lon = response["coord"]["lon"]
            weather = response["weather"][0]["main"]
            description = response["weather"][0]["description"]
            temp = response["main"]["temp"]
            pressure = response["main"]["pressure"]
            humidity = response["main"]["humidity"]
            visibility = response["visibility"]
            rain = response.get("rain", {}).get("1h", 0)
            snow = response.get("snow", {}).get("1h", 0)
            wind_speed = response["wind"]["speed"]
            clouds = response["clouds"]["all"]
            dt = response["dt"]
        except (KeyError, IndexError, TypeError) as e:
            logger.exception(f"Missing key fields: {e}")
            raise ValueError("Missing key fields")
        weather_data = [
            lat,
            lon,
            weather,
            description,
            temp,
            pressure,
            humidity,
            visibility,
            rain,
            snow,
            wind_speed,
            clouds,
            dt,
        ]
        all_zones.append(weather_data)
    if len(all_zones) == 0:
        logger.info("No weather zones found")
        raise NoUpdatesError("No weather zones found")
    return all_zones


# This function creates a dataframe from the weather zone data with the
# schema expected by the bronze layer
def create_bronze_weather_df(df):
    return df.withColumn("ingest_timestamp", lit(datetime.now())).coalesce(1)

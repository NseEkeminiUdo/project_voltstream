from pyspark.sql.functions import col, max_by, sum as spark_sum, when, max as spark_max, lit, round, least, timestamp_diff, lag, sum_distinct, row_number
from pyspark.sql.window import Window
from datetime import datetime
from logger.custom_logging import set_up_logger, get_job_logger
import logging

"""
This file contains functions to be used in the gold layer of the pipeline
"""

try:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    run_id = context.tags().get("runId").get()
except BaseException:
    # Fallback for when running as a file (not a notebook)
    run_id = "unknown"

logger = set_up_logger()


# This function joins the three tables
def join_tables(df_stations, df_connections, df_weather):
    df = (df_stations.alias("s")
          .join(df_connections.alias("c"), col("s.station_id") == col("c.station_id"))
          .join(
        df_weather.alias("w"),
        (col("s.weather_zone_lat") == col("w.lat")) &
        (col("s.weather_zone_lon") == col("w.lon"))
        # Filter for stations with status 'operational' or 'Tempoarily
        # unavailable'
    )).where((col("s.status_id") == 50) | (col("s.status_id") == 30))\
        .withColumn("is_operational", col("s.status_id") == 50)  # indicates if the station is operational or not
    window = Window.partitionBy(
        col("s.station_id")).orderBy(
        col("s.date_last_status_update").desc())
    return df.withColumn(
        "prev_is_operational", lag(
            col("is_operational"), offset=1).over(window)).withColumn(
        "rn", row_number().over(window)).withColumn(
                "total_power", col("c.power_kw") * col("c.quantity"))


# This function gets the station status facts from the joined dataframe
def get_station_status_facts(df_joined):
    return df_joined.groupBy(col("s.station_id")).agg(
        spark_max(col("weather_zone_id")).alias("weather_zone_id"),
        max_by(col("w.weather_sk"), col("w.dt_utc")).alias("weather_sk"),
        max_by(col("is_operational"), col("s.date_last_status_update")).alias("is_operational"),
        spark_sum(
            when(~col("is_operational") & (col("prev_is_operational") | col("prev_is_operational").isNull()), 1)
            .otherwise(0)
        ).alias("offline_count"),
        spark_sum(
            when(~col("is_operational") & (col("prev_is_operational") | col("prev_is_operational").isNull()),
                 timestamp_diff("MINUTE", col("s.valid_from"), least(lit(datetime.now()), col("s.valid_to")))
                 ).otherwise(0)
        ).alias("offline_duration"),
        timestamp_diff("MINUTE", spark_max(col("s.date_created")), lit(datetime.now())).alias("total_duration"),
        (
            lit(15) +
            when(
                (max_by(col("w.weather"), col("w.dt_utc")).like("%storm%")) |
                (max_by(col("w.weather"), col("w.dt_utc")).like("%snow%")),
                30
            ).otherwise(0) +
            when(
                (max_by(col("w.temp"), col("w.dt_utc")) > 95) |
                (max_by(col("w.temp"), col("w.dt_utc")) < 20),
                30
            ).otherwise(0) +
            when(spark_max(col("c.level_id")) == 3, 25).otherwise(0)
        ).alias("risk_score"),
        when((col("risk_score") < 60), "low_risk")
        .when(col("risk_score") >= 60, "high_risk").alias("risk_severity_level"),
        spark_sum(when(col("rn") == 1, round(0.35 * col("total_power"))).otherwise(0)).alias("avg_energy_cost_per_hour")
    )


# This function adds the business metadata to the station status facts
def add_station_facts_metadata(df):
    return (df.withColumn(
        "est_downtime_cost",
        col("avg_energy_cost_per_hour") * col("offline_duration") / 60
    ).withColumn("availability", round((1 - (col("offline_duration") / col("total_duration"))), 2)
                 ).withColumn("ingest_timestamp", lit(datetime.now())))


# This function gets the station dimension from the joined dataframe
def get_station_dim(df_joined):
    cols = {
        "title": "s.title",
        "address": "s.address",
        "town": "s.town",
        "state_or_province": "s.state_or_province",
        "latitude": "s.latitude",
        "longitude": "s.longitude",
        "max_power_kw": "c.power_kw"
    }

    return df_joined.groupBy(col("s.station_id")).agg(
        *[spark_max(col(v)).alias(k) for k, v in cols.items()]
    ).withColumn("ingest_timestamp", lit(datetime.now()))


# This function gets the weather dimension from the joined dataframe
def get_weather_dim(df_joined):
    cols = {
        "weather_zone_id": "w.weather_zone_id",
        "weather": "w.weather",
        "description": "w.description",
        "temp": "w.temp",
        "pressure": "w.pressure",
        "humidity": "w.humidity",
        "visibility": "w.visibility",
        "rain": "w.rain",
        "snow": "w.snow"
    }
    return df_joined.groupBy(col("w.weather_sk")).agg(
        *[spark_max(col(v)).alias(k) for k, v in cols.items()]
    ).withColumn("ingest_timestamp", lit(datetime.now()))

import requests
from datetime import datetime
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from logger.custom_logging import set_up_logger, get_job_logger
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import time
import logging

"""
This file contains functions that are shared by all the layers of the pipeline
"""

def get_run_context(spark):
    """
    Get runtime context information from Spark configuration.
    
    When running as a Databricks Job, set these parameters in the task configuration:
    - pipeline_run_id = {{job.run_id}}
    - pipeline_job_id = {{job.id}}
    - pipeline_task_name = {{task.name}}
    - pipeline_env = prod/dev/test
    
    Args:
        spark: Active SparkSession
        
    Returns:
        dict: Context information with run_id, job_id, task_key, and env
    """
    return {
        "run_id": spark.conf.get("spark.databricks.clusterUsageTags.runId",
                                spark.conf.get("pipeline_run_id", "local")),
        "job_id": spark.conf.get("spark.databricks.clusterUsageTags.jobId",
                                spark.conf.get("pipeline_job_id", "local")),
        "task_name": spark.conf.get("pipeline_task_name", "local"),
        "env": spark.conf.get("pipeline_env", "dev")
    }



try:
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark:
        run_id = spark.conf.get("spark.databricks.clusterUsageTags.runId", 
                               spark.conf.get("pipeline_run_id", "unknown"))
    else:
        run_id = "unknown"
except Exception:
    run_id = "unknown"

logger = set_up_logger()

# Helper function to fetch data from the API


def fetch_data(url, params, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    retries = 5
    base_delay = 2
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)

            if response.status_code == 200:
                return response.json()
            else:
                log(logging.WARNING,
                    f"HTTP {response.status_code}: {response.text}")
                raise Exception(f"HTTP {response.status_code}")

        except Exception as e:
            log(logging.ERROR, f"Attempt {attempt + 1} failed: {e}")

            if attempt == retries - 1:
                raise

            delay = base_delay * (attempt + 1)  # incremental increase
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)


# For incremental updates, we need to keep track of the last timestamp we
# ingested
def load_checkpoint(df, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)

    # if the table exists, read the last timestamp ingested
    try:
        last_timestamp = df.select("ingest_timestamp").orderBy(
            col("ingest_timestamp").desc()).limit(1).collect()[0][0]
    # else, return a default timestamp
    except BaseException:
        last_timestamp = datetime.fromisoformat("2011-01-01T00:00:00Z")

    log(logging.INFO,
        f"{log_info['layer']} last timestamp ingested: {last_timestamp}")
    return last_timestamp


# This function creates an empty delta table with fixed schema if it
# doesn't exist
def load_table(spark, name, df):
    if spark.catalog.tableExists(name):
        return DeltaTable.forName(spark, name)

    # Create table
    df.write \
      .format("delta") \
      .mode("error") \
      .saveAsTable(name)

    return DeltaTable.forName(spark, name)


# This function adds the current timestamp to the dataframe
def add_timestamp(df):
    return df.withColumn("ingest_timestamp", lit(datetime.now()))


# This function filters out data that has already been ingested
def filter_uningested_data(df, max_time="2011-01-01T00:00:00Z", **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    df = df.where(col("ingest_timestamp") > max_time)
    log(logging.INFO,
        f"{log_info['layer']} ingesting {df.count()} new records")
    return df


# This function deduplicates the dataframe
def deduplicate(df, cols: list[str] = None):
    if cols is None or len(cols) == 0:
        # Deduplicate on all columns
        return df.dropDuplicates()
    else:
        # Deduplicate on specified columns
        return df.dropDuplicates(cols)


# This function write the dataframe to a delta table in the provide update mode
def write_to_table(df, table, mode, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    log(logging.INFO,
        f"{log_info['layer']} writing {df.count()} records to {table} in {mode} mode")
    return df.write.format("delta").mode(mode).saveAsTable(table)


# This function updates the table with new data without duplicating old data
def update_table(df, table, id, **log_info):
    log = get_job_logger(logger, **log_info, run_id=run_id)
    df_tgt = table.toDF()

    # manually extract the updated and inserted rows for logging
    inserted_rows = df.alias("s").join(
        df_tgt.alias("t"), on=id, how="leftanti")

    table.alias("t").merge(
        df.alias("s"),
        f"s.{id} = t.{id}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    log(logging.INFO,
        f"{log_info['layer']} inserted {inserted_rows.count()} rows")

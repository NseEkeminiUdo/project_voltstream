from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)
import uuid
from datetime import datetime


def create_control_table(spark, table_name="control_table"):
    """
    Create the pipeline control table if it doesn't exist.

    Args:
        spark: SparkSession instance
        table_name: Name of the control table (default: "control_table")

    Returns:
        str: The control table name
    """

    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        pipeline_name STRING,
        layer STRING,                     -- bronze / silver / gold
        last_processed_timestamp TIMESTAMP,    -- last processed point
        batch_id STRING,                  -- unique run ID
        status STRING,                    -- success / failed / running
        records_processed INT,
        records_failed INT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        error_message STRING
    )
    USING DELTA
    """
    )
    return table_name


def get_last_processed_timestamp(spark, table_name, pipeline_name, layer):
    """
    Get the last processed timestamp for a given pipeline and layer.

    Args:
        spark: SparkSession instance
        table_name: Name of the control table
        pipeline_name: Name of the pipeline
        layer: Layer name (e.g., 'bronze', 'silver', 'gold')

    Returns:
        Timestamp or None if no records exist
    """
    control_df = spark.sql(
        f"""
    SELECT last_processed_timestamp
    FROM {table_name}
    WHERE pipeline_name = '{pipeline_name}'
    AND layer = '{layer}'
    AND last_processed_timestamp IS NOT NULL
    ORDER BY end_time DESC
    LIMIT 1
    """
    )

    return (
        control_df.collect()[0][0]
        if control_df.count() > 0
        else datetime.fromisoformat("2011-01-01T00:00:00Z")
    )


def insert_control_record(
    spark,
    table_name,
    pipeline_name,
    layer,
    last_processed_timestamp=None,
    records_processed=None,
    records_failed=None,
    start_time=None,
    end_time=None,
    error_message=None,
    status="success",
):
    """
    Insert a new control record for a pipeline run.

    Args:
        spark: SparkSession instance
        table_name: Name of the control table
        pipeline_name: Name of the pipeline
        layer: Layer name (e.g., 'bronze', 'silver', 'gold')
        last_processed_timestamp: The max timestamp processed in this run
        status: Status of the run (default: "success")

    Returns:
        str: The batch_id of the inserted record
    """
    batch_id = str(uuid.uuid4())

    # Define explicit schema to handle None values properly
    schema = StructType(
        [
            StructField("pipeline_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("last_processed_timestamp", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_failed", IntegerType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("error_message", StringType(), True),
        ]
    )

    new_record = spark.createDataFrame(
        [
            (
                pipeline_name,
                layer,
                last_processed_timestamp,
                status,
                batch_id,
                records_processed,
                records_failed,
                start_time,
                end_time,
                error_message,
            )
        ],
        schema,
    )
    new_record.write.format("delta").mode("append").saveAsTable(table_name)

    return batch_id

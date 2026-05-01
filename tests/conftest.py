import pytest
import sys

sys.dont_write_bytecode = True


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # 1. Use active session (Databricks / notebook / Spark Connect)
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark

    # 2. Try fallback ONLY if truly local environment
    try:
        builder = (
            SparkSession.builder.appName("your_app")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    except Exception as e:
        pytest.skip(f"No Spark available: {e}")


# Fixture to ensure geopandas is available - RAISES ERROR ON FAILURE
@pytest.fixture(scope="session")
def ensure_geopandas():
    import geopandas as gpd
    from shapely.geometry import Polygon

    return gpd, Polygon

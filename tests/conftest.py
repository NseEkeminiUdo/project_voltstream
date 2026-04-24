import pytest
import sys
import subprocess

sys.dont_write_bytecode = True


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # 1. Use active session (Databricks / notebook / Spark Connect)
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark

    # 2. Try fallback ONLY if truly local environment
    try:
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception as e:
        pytest.skip(f"No Spark available: {e}")


# Fixture to ensure geopandas is available - RAISES ERROR ON FAILURE
@pytest.fixture(scope="session")
def ensure_geopandas():
    import geopandas as gpd

    return gpd

import pytest
from pyspark.sql import SparkSession

from src.config import settings


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession
             .builder
             .appName("study_spark")
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.sql.shuffle.partitions", settings.NUM_PARTITIONS)
             .getOrCreate())
    return spark
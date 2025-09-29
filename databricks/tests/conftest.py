
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_local():
    """Local Spark for unit tests"""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("unit_tests") \
        .getOrCreate()
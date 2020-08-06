import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark

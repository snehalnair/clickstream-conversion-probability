import pandas as pd
import pytest

from pyspark import SparkContext
from pyspark.ml import Pipeline, Transformer
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from spark_fixtures import spark
from src.transforms.time_between_visits import TimeBetweenVisits


def test_time_between_visits(spark):
    # Input and output dataframes
    input_df = spark.createDataFrame(
        [
            ("charly", 15),
            ("sam", 21),
            ("sam", 24),
            ("sam", 28),
            ("sam", 30),
            ("nick", 19),
            ("nick", 40),
        ],
        ["fullVisitorId", "visitStartTime"],
    )
    expected_output = [
        Row(fullVisitorId="charly", visitStartTime=15, timeBetweenVisits=0),
        Row(fullVisitorId="sam", visitStartTime=21, timeBetweenVisits=0),
        Row(fullVisitorId="sam", visitStartTime=24, timeBetweenVisits=3),
        Row(fullVisitorId="sam", visitStartTime=28, timeBetweenVisits=4),
        Row(fullVisitorId="sam", visitStartTime=30, timeBetweenVisits=2),
        Row(fullVisitorId="nick", visitStartTime=19, timeBetweenVisits=0),
        Row(fullVisitorId="nick", visitStartTime=40, timeBetweenVisits=21),
    ]

    transformer = TimeBetweenVisits()
    actual_transformed_df = transformer.transform(input_df)
    actual_transformed = actual_transformed_df.collect()

    assert sorted(actual_transformed) == sorted(expected_output)

"""This module writes the test codes for the function trim_min_visits"""

from pyspark.sql import Row
from spark_fixtures import spark
from src.transforms.trim_min_visits import TrimByMinVisits

def test_trim_min_visits(spark):
    """Tests the trim_min_visits code to return users with more than \
        given number of visits (constant)

                Args:
                    spark

                Returns:
                    TimeBetweenVisits Transformer for the pipeline
    """
    # Input and output dataframes
    input_df = spark.createDataFrame(
        [
            ("charly", 15),
            ("fabien", 18),
            ("sam", 21),
            ("sam", 24),
            ("sam", 23),
            ("sam", 22),
            ("nick", 19),
            ("nick", 40),
        ],
        ["fullVisitorId", "value"],
    )
    expected_output = [
        Row(fullVisitorId="sam", value=21),
        Row(fullVisitorId="sam", value=24),
        Row(fullVisitorId="sam", value=23),
        Row(fullVisitorId="sam", value=22),
    ]

    transformer = TrimByMinVisits(2)
    actual_transformed = transformer.transform(input_df).collect()
    assert actual_transformed == expected_output

    # other ways of comparing dataframes.
    assert len(actual_transformed) == len(expected_output) and set(
        actual_transformed
    ) == set(expected_output)

    assert sorted(actual_transformed) == sorted(expected_output)

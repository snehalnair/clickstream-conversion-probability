"""
NA
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.ml.linalg import Vectors, VectorUDT


class MergeIndexFeatures(Transformer):
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        array_to_vec = fn.udf(Vectors.dense, VectorUDT())
        return df.withColumn(
            "index_features",
            array_to_vec(
                fn.concat(
                    fn.col("source_index"),
                    fn.col("medium_index"),
                    fn.col("channelGrouping_index"),
                    fn.col("deviceCategory_index"),
                )
            ),
        )


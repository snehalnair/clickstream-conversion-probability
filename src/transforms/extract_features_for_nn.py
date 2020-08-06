"""
NA
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn


class ExtractFeaturesForNN(Transformer):
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        return df.withColumn(
            "label", fn.col("transactionsIn0_binary").getItem(0)
        ).select("features", "label")


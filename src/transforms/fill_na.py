"""
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn


class FillNa(Transformer):
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        return df.fillna(0)


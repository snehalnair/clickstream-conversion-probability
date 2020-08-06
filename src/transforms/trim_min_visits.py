from pyspark.sql import SparkSession, Row
from pyspark.ml import Pipeline, Transformer
import pytest
from pyspark.sql.window import Window
from pyspark.sql import functions as fn




class TrimByMinVisits(Transformer):
        """Remove visitors with less than declared minimum visits

            Args:
                None

            Returns:
                Update self.data_frame with trimmed dataset
        """
        def __init__(self, minv: int):
            self.minvisits = minv
            super(TrimByMinVisits, self).__init__()
        def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
            my_window = Window.partitionBy('fullVisitorId')
            if self.minvisits > 1:
                return df.select(
                    "*", fn.count('fullVisitorId').\
                    over(my_window).alias("rank")).where(
                        "rank > {}".format(self.minvisits)
                    )
            return df 
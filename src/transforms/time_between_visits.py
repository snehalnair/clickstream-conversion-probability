from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.sql.window import Window


class TimeBetweenVisits(Transformer):
    """Gets the time lapsed between two consecutive visits

                Args:
                    Transformer class

                Returns:
                    TimeBetweenVisits Transformer for the pipeline
    """
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
            partition_col = 'fullVisitorId'
            orderby_sort_col = 'visitStartTime'
            my_window = Window.partitionBy(partition_col
                ).orderBy(orderby_sort_col
                )

            return df.withColumn(
                "prev_value", fn.lag(fn.col(orderby_sort_col)).over(my_window)
                ).withColumn("timeBetweenVisits",fn.when(fn.isnull(
                      fn.col(orderby_sort_col) - fn.col('prev_value')), 0).otherwise(
                      fn.col(orderby_sort_col) - fn.col('prev_value'))
                ).drop('prev_value')

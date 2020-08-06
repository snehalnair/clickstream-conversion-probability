"""
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from typing import Iterable


class GroupBySortByStructPad(Transformer):
    def __init__(self, finalcols_list: Iterable[str], padlen: int):
        self.finalcols_list = finalcols_list
        self.fcol_select = [
            "sortedByStartTime." + colstr for colstr in self.finalcols_list
        ]
        self.padlength = padlen
        super(GroupBySortByStructPad, self).__init__()

    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        pad_plus_one = [
            "channelGrouping_index",
            "medium_index",
            "deviceCategory_index",
            "source_index",
            "city_index",
            "visitsIn0_binary",
            "transactionsIn0_binary",
        ]
        for col in pad_plus_one:
            df = df.withColumn(col, fn.col(col) + 1)

        win = Window.partitionBy("fullVisitorId").orderBy("visitStartTime")
        return (
            df.withColumn("rank", fn.dense_rank().over(win))
            .where("rank <= {}".format(self.padlength))
            .groupBy("fullVisitorId")
            .agg(
                fn.array_sort(fn.collect_list(fn.struct(self.finalcols_list))).alias(
                    "sortedByStartTime"
                )
            )
            .select(self.fcol_select)
        )


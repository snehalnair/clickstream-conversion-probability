"""Assert data type
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession


class AssertDataType(Transformer):
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        df.createOrReplaceTempView("df")
        spark = SparkSession.builder.getOrCreate()
        return spark.sql(
            """select
          cast(fullVisitorId as string) as fullVisitorId, 
          cast(visitStartTime as int) as visitStartTime,
          cast(channelGrouping as string) as channelGrouping,
          cast(medium as string) as medium,
          cast(source as string) as source,
          cast(city as string) as city,
          cast(deviceCategory as string) as deviceCategory,
          cast(timeOnSite as float) as timeOnSite,
          cast(timeBetweenVisits as float) as timeBetweenVisits,
          cast(pagePathLevel1 as array<string>) as pagePathLevel1,
          cast(pagePathLevel3 as array<string>) as pagePathLevel2,
          cast(pagePathLevel3 as array<string>) as pagePathLevel3,
          cast(contentGroup4 as array<string>) as contentGroup4,
          cast(visitsIn0 as double) as visitsIn0,
          cast(transactionsIn0 as double) as transactionsIn0
          from df"""
        )


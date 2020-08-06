"""
NA
"""

from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.ml.linalg import Vectors, VectorUDT


class MergeArrayVector(Transformer):
    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        def merge_array_vector(arr):
            if len(arr) == 0:
                return arr
            else:
                f = arr[0].toArray()
                for a in arr[1:]:
                    f = f + a.toArray()
                return Vectors.dense(f)

        merge_array_vector_udf = fn.udf(merge_array_vector, VectorUDT())
        return df.withColumn(
            "pagePath_vector_arr",
            fn.concat(
                fn.col("pagePathLevel1_vector"),
                fn.col("pagePathLevel2_vector"),
                fn.col("pagePathLevel3_vector"),
            ),
        ).withColumn(
            "pagePath_vector", \
                merge_array_vector_udf(fn.col("pagePath_vector_arr"))
        )

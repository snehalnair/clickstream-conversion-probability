"""
NA
"""

from typing import Iterable
from pyspark.ml import Transformer
from pyspark.sql import functions as fn
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import Vectors, VectorUDT
from src.featureengineering.params import VOCABSIZE_ARRAYCOLS


class PadVector(Transformer):
    def zero_value(self, dataType):
        if dataType == DoubleType():
            return 0.0
        elif dataType == VectorUDT():
            return Vectors.sparse(VOCABSIZE_ARRAYCOLS, [])

    def __init__(self, pad_cols: Iterable[str], pad_len: int):
        super(PadVector, self).__init__()
        self.pad_cols = pad_cols
        self.pad_len = pad_len

    def _transform(self, df: fn.DataFrame) -> fn.DataFrame:
        field_datatype = {
            f.name: f.dataType for f in df.schema.fields if f.name in self.pad_cols
        }
        schema_fields = df.schema.fields
        i = df
        for c in self.pad_cols:
            padder = fn.udf(
                lambda ar: ar
                + [
                    self.zero_value(field_datatype[c].elementType)
                    for a in range(self.pad_len - len(ar))
                ],
                field_datatype[c],
            )
            i = i.withColumn(c, padder(c))
        return i


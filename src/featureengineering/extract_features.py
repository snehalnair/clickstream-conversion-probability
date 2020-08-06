"""
"""

from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    Binarizer,
    HashingTF,
    StandardScaler,
)

from src.transforms.trim_min_visits import TrimByMinVisits
from src.transforms.time_between_visits import TimeBetweenVisits
from src.transforms.assert_data_type import AssertDataType
from src.transforms.fill_na import FillNa
from src.transforms.group_bySort_by_struct_pad import GroupBySortByStructPad
from src.transforms.pad_vector import PadVector
from src.transforms.merge_array_vector import MergeArrayVector
from src.transforms.merge_index_features import MergeIndexFeatures
from src.transforms.extract_features_for_nn import ExtractFeaturesForNN

from src.featureengineering.params import *
import ast
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.mllib.util import MLUtils


def get_stages():
    return (
        [TrimByMinVisits(MIN_VISITS), TimeBetweenVisits(), AssertDataType(), FillNa(),]
        + [
            VectorAssembler(inputCols=[column], outputCol=column + "_vect")
            for column in VAR_TYPES["X_NUM_COLS"]
        ]
        + [
            StandardScaler(
                inputCol=column + "_vect",
                outputCol=column + "_scaled",
                withStd=True,
                withMean=False,
            )
            for column in VAR_TYPES["X_NUM_COLS"]
        ]
        + [
            StringIndexer(
                inputCol=column, outputCol=column + "_index"
            ).setHandleInvalid("keep")
            for column in VAR_TYPES["STRING_COLS"]
        ]
        + [
            HashingTF(
                inputCol=column,
                outputCol=column + "_vector",
                numFeatures=VOCABSIZE_ARRAYCOLS,
            )
            for column in VAR_TYPES["ARRAY_COLS"]
        ]
        + [
            Binarizer(inputCol=column, outputCol=column + "_binary", threshold=0)
            for column in VAR_TYPES["Y_COLS"]
        ]
        + [
            GroupBySortByStructPad(VAR_TYPES["FINAL_COLS"], PAD_LEN),
            PadVector(VAR_TYPES["FINAL_COLS"], pad_len=5),
            MergeArrayVector(),
            MergeIndexFeatures(),
            VectorAssembler(
                inputCols=["index_features", "pagePath_vector"], outputCol="features"
            ),
            ExtractFeaturesForNN(),
        ]
    )


def create_features(input_path):
    spark = SparkSession.builder.appName("feature-engg").getOrCreate()

    df = spark.read.format("csv")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .load(input_path)
    )

    parse_array_udf = fn.udf(ast.literal_eval, ArrayType(StringType()))

    for col_ in ["pagePathLevel1", "pagePathLevel2", "pagePathLevel3", "contentGroup4"]:
        df = df.withColumn(col_, parse_array_udf(fn.col(col_)))

    df_allfeatures = Pipeline(stages=get_stages()).fit(df).transform(df)
    df_allfeatures.printSchema()
    
    return df_allfeatures

def features_as_pandas_df(input_path):
    df = create_features(input_path)
    df.show()
    return df.toPandas()

import unittest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from data_utils import (
    generate_spark_data_profile,
    remove_high_null_percentage_columns,
    remove_low_cardinality_columns,
    arrow_datetype_convert,
)

import importlib.util
import os
from pathlib import Path
import sys

data_utils_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'data_utils.py')
spec = importlib.util.spec_from_file_location("data_utils", data_utils_path)
data_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data_utils)

class TestSparkDataTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local")
            .appName("DataTransformationTests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_arrow_datetype_convert(self):
        df = self.spark.createDataFrame(
            [("01/Jan/2024",), ("",), (None,)], ["date_str"]
        )
        df = df.withColumn("converted_date", arrow_datetype_convert(df["date_str"]))
        result = df.select("converted_date").collect()

        self.assertEqual(result[0]["converted_date"], datetime(2024, 1, 1).date())
        self.assertEqual(result[1]["converted_date"], datetime.now().date())
        self.assertEqual(result[2]["converted_date"], datetime.now().date())

    def test_generate_spark_data_profile(self):
        data = [(1, "A"), (None, "B"), (3, "A"), (4, None)]
        schema = T.StructType(
            [
                T.StructField("numeric_col", T.IntegerType(), True),
                T.StructField("string_col", T.StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(data, schema)

        profile = generate_spark_data_profile(df)

        self.assertEqual(profile["numeric_col"]["contagem_nulos"], 1)
        self.assertEqual(profile["numeric_col"]["contagem_unicos"], 3)
        self.assertAlmostEqual(
            profile["numeric_col"]["media"], Decimal("2.6667"), places=4
        )
        self.assertEqual(profile["numeric_col"]["maximo"], Decimal("4"))
        self.assertEqual(profile["numeric_col"]["minimo"], Decimal("1"))

        self.assertEqual(profile["string_col"]["contagem_nulos"], 1)
        self.assertEqual(profile["string_col"]["contagem_unicos"], 2)

    def test_remove_high_null_percentage_columns(self):
        data = [(1, None), (2, None), (3, "text"), (None, None)]
        schema = T.StructType(
            [
                T.StructField("numeric_col", T.IntegerType(), True),
                T.StructField("text_col", T.StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(data, schema)

        filtered_df, removed_columns = remove_high_null_percentage_columns(df, 50.0)

        self.assertIn("text_col", removed_columns)
        self.assertNotIn("text_col", filtered_df.columns)
        self.assertIn("numeric_col", filtered_df.columns)

    def test_remove_low_cardinality_columns(self):
        data = [(1, "A"), (2, "A"), (3, "A"), (4, "B")]
        schema = T.StructType(
            [
                T.StructField("numeric_col", T.IntegerType(), True),
                T.StructField("text_col", T.StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(data, schema)
        filtered_df, removed_columns = remove_low_cardinality_columns(df, 2)

        self.assertIn("text_col", removed_columns)
        self.assertNotIn("text_col", filtered_df.columns)
        self.assertIn("numeric_col", filtered_df.columns)


if __name__ == "__main__":
    project_root = Path(__file__).parents[2]  
    sys.path.append(str(project_root))

    unittest.main()

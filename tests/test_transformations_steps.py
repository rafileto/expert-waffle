import unittest

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from steps import (
    rename_and_select_cols,
    extract_route_and_verb_from_str,
    extract_date_from_str,
    clean_str,
    str_to_date,
    build_date_column,
    build_day_of_week_column,
    build_time_column,
)

class TestDataFrameTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rename_and_select_cols(self):
        data = [("val1", "val3", "val4", "val5", "val6", "val7", "val8")]
        df = self.spark.createDataFrame(data, ["_c0", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7"])
        result_df = rename_and_select_cols(df)
        expected_cols = ["ip_requisicao", "http_status_code", "volume_resposta", "data_horario_requisicao", "verbo_rota"]
        self.assertEqual(result_df.columns.sort(), expected_cols.sort())

    def test_extract_route_and_verb_from_str(self):
        data = [("GET /route/path?param=value", )]
        df = self.spark.createDataFrame(data, ["verbo_rota"])
        result_df = extract_route_and_verb_from_str(df, "verbo_rota")
        result = result_df.collect()[0]
        self.assertEqual(result["verbo"], "GET")
        self.assertEqual(result["rota"], "/route/path")

    def test_extract_date_from_str(self):
        data = [("2024-10-01T12:34:56", )]
        df = self.spark.createDataFrame(data, ["data_horario"])
        result_df, col_name = extract_date_from_str(df, "data_horario", "T", 0)
        self.assertIn(col_name, result_df.columns)
        self.assertEqual(result_df.select(col_name).collect()[0][0], "2024-10-01")

    def test_clean_str(self):
        data = [("field[2024-10-01]", )]
        df = self.spark.createDataFrame(data, ["data_col"])
        result_df = clean_str(df, "data_col")
        self.assertEqual(result_df.collect()[0]["data_col"], "2024-10-01")

    def test_str_to_date(self):
        data = [("2024-10-01", )]
        date_str = "2024-10-01"
        df = self.spark.createDataFrame(data, ["raw_data_col"])
        result_df = str_to_date(df, "raw_data_col", F.to_date)
        self.assertEqual(result_df.collect()[0]["trat_data_col"], datetime.strptime(date_str, '%Y-%m-%d').date()
)

    def test_build_date_column(self):
        data = [("01/Oct/2024:12:34:56", )]
        df = self.spark.createDataFrame(data, ["data_horario"])
        result_df = build_date_column(df, "data_horario", ":", 0, F.to_date)
        self.assertTrue("trat_data_horario" in result_df.columns)

    def test_build_day_of_week_column(self):
        data = [("2024-10-01", )]
        df = self.spark.createDataFrame(data, ["trat_data_requisicao"])
        df = df.withColumn("trat_data_requisicao", F.to_date(df["trat_data_requisicao"]))
        result_df = build_day_of_week_column(df)
        self.assertTrue("trat_dia_da_semana" in result_df.columns)

    def test_build_time_column(self):
        data = [("01/Oct/2024:12:34:56", )]
        df = self.spark.createDataFrame(data, ["data_horario"])
        result_df = build_time_column(df, "data_horario")
        self.assertEqual(result_df.collect()[0]["horario"], "12:34:56")

if __name__ == "__main__":
    unittest.main()

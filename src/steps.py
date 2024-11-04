from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import day_of_week_mapping


def rename_and_select_cols(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed("_c0", "ip_requisicao")
    df = df.withColumnRenamed("_c6", "http_status_code")
    df = df.withColumnRenamed("_c7", "volume_resposta")
    df = df.withColumnRenamed("_c3", "data_horario_requisicao")
    df = df.withColumnRenamed("_c5", "verbo_rota")
    df = df.drop("_c2")
    df = df.drop("_c4")
    return df


def extract_route_and_verb_from_str(df: DataFrame, col_name: str) -> DataFrame:
    df = df.withColumn("verbo", F.split(df["verbo_rota"], " ").getItem(0))
    
    df = df.withColumn(
        "rota", F.split(df["verbo_rota"], " ").getItem(1)
    )

    df = df.withColumn("rota", F.trim(F.split(df["rota"], "\?").getItem(0)))
    df = df.drop("verbo_rota")
    return df


def extract_date_from_str(
    df: DataFrame, col_name: str, sep: str, pos: int
) -> tuple[DataFrame, str]:
    df = df.withColumn(f"raw_{col_name}", F.split(df[col_name], sep).getItem(pos))
    return df, f"raw_{col_name}"


def clean_str(df: DataFrame, col_name: str) -> DataFrame:
    df = df.withColumn(col_name, F.split(df[col_name], "\[").getItem(1))
    df = df.withColumn(col_name, F.split(df[col_name], "\]").getItem(0))
    return df


def str_to_date(df: DataFrame, col_name: str, method: callable) -> DataFrame:
    col_name_aux = col_name.split("raw_")[1]
    df = df.withColumn(f"trat_{col_name_aux}", method(F.col(col_name)))
    df = df.drop(col_name)
    return df


def build_date_column(
    df: DataFrame, col_name: str, sep: str, pos: int, method: callable
) -> DataFrame:
    df, nome_coluna = extract_date_from_str(df, col_name, ":", 0)
    df = clean_str(df, nome_coluna)
    df = str_to_date(df, nome_coluna, method)
    return df


def build_day_of_week_column(df: DataFrame) -> DataFrame:
    df = df.withColumn("dia_da_semana", F.dayofweek("trat_data_horario_requisicao"))

    mapping_expr = F.create_map([F.lit(x) for x in chain(*day_of_week_mapping.items())])
    df = df.withColumn("trat_dia_da_semana", mapping_expr[F.col("dia_da_semana")])
    df = df.drop("dia_da_semana")
    return df


def build_time_column(df: DataFrame, col_name: str) -> DataFrame:
    df = df.withColumn(
        "horario",
        F.concat(
            F.split(df[col_name], ":").getItem(1),
            F.lit(":"),
            F.split(df[col_name], ":").getItem(2),
            F.lit(":"),
            F.split(df[col_name], ":").getItem(3),
        ),
    )
    df = df.drop(col_name)
    return df

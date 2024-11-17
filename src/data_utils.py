from typing import Dict, Any
from datetime import datetime, date
from decimal import Decimal, getcontext, ROUND_HALF_UP

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql import types as T
from pyspark.sql.functions import udf

# Configuração do contexto decimal para precisão
getcontext().prec = 6
getcontext().rounding = ROUND_HALF_UP


@udf(returnType=T.DateType(), useArrow=True)  # An Arrow Python UDF
def arrow_datetype_convert(s: str | None):
    """
    Converte uma string no formato "%d/%b/%Y" para um objeto DateType do Spark.

    :param data_str: String representando a data. Se vazio ou None, retorna a data atual.
    :return: Objeto date correspondente à string fornecida ou a data atual.
    """
    if s == "" or s is None:
        return date.today()
    return datetime.strptime(s, "%d/%b/%Y")


def generate_spark_data_profile(df: DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Gera um perfil de dados com várias métricas para cada coluna de um DataFrame PySpark.

    :param df: DataFrame PySpark a ser analisado.
    :return: Dicionário onde cada chave é o nome de uma coluna e cada valor é outro dicionário
             contendo métricas para essa coluna.
    """
    profile = {}

    total_rows = df.count()

    for column in df.columns:
        column_profile = {}

        null_count = df.filter(F.col(column).isNull()).count()
        null_percent = Decimal(str(null_count / total_rows)) * Decimal(100)
        column_profile["contagem_nulos"] = null_count
        if null_percent is None:
            column_profile["percentual_nulos"] = Decimal("0.0")
        else:
            column_profile["percentual_nulos"] = null_percent

        unique_count = (
            df.filter(df[column].isNotNull()).select(column).distinct().count()
        )
        unique_percent = Decimal(str(unique_count / total_rows)) * Decimal(100)
        column_profile["contagem_unicos"] = unique_count
        if unique_percent is None:
            column_profile["percentual_unicos"] = Decimal("0.0")
        else:
            column_profile["percentual_unicos"] = unique_percent

        if df.schema[column].dataType in [
            T.IntegerType(),
            T.FloatType(),
            T.DoubleType(),
            T.LongType(),
            T.ShortType(),
            T.DecimalType(),
        ]:
            mean_value = df.agg(F.mean(column)).first()[0]
            max_value = df.agg(F.max(column)).first()[0]
            min_value = df.agg(F.min(column)).first()[0]
            column_profile["media"] = Decimal(str(mean_value))
            column_profile["maximo"] = Decimal(str(max_value))
            column_profile["minimo"] = Decimal(str(min_value))

        profile[column] = column_profile

    return profile


def remove_high_null_percentage_columns(
    df: DataFrame, max_null_percent: float
) -> tuple[DataFrame, list[str]]:
    """
    Remove colunas do DataFrame que possuem um percentual de valores nulos superior a `percentual_maximo_nulos`.

    :param df: DataFrame PySpark a ser processado.
    :param percentual_maximo_nulos: Percentual máximo de valores nulos permitidos para manter a coluna.
    :return: Tuple contendo o DataFrame modificado sem as colunas removidas e uma lista das colunas que foram removidas.
    """
    total_rows = df.count()

    columns_to_remove = []

    for column in df.columns:
        null_count = df.filter(df[column].isNull()).count()
        null_percent = (null_count / total_rows) * 100

        if null_percent > max_null_percent:
            columns_to_remove.append(column)

    df_filtered = df.drop(*columns_to_remove)

    return df_filtered, columns_to_remove


def remove_low_cardinality_columns(
    df: DataFrame, min_unique_values: int
) -> tuple[DataFrame, list[str]]:
    """
    Remove colunas do DataFrame que possuem menos de `valores_unicos_minimos` valores únicos.

    :param df: DataFrame PySpark a ser processado.
    :param valores_unicos_minimos: Número mínimo de valores únicos requeridos para manter a coluna.
    :return: Tuple contendo o DataFrame modificado sem as colunas removidas e uma lista das colunas que foram removidas.
    """
    columns_to_remove = []

    for column in df.columns:
        unique_count = df.select(column).distinct().count()
        if unique_count <= min_unique_values:
            columns_to_remove.append(column)

    df_filtered = df.drop(*columns_to_remove)

    return df_filtered, columns_to_remove

from typing import Dict, Any
from datetime import datetime, date
from decimal import Decimal, getcontext, ROUND_HALF_UP

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql import types as T
from pyspark.sql.functions import udf


@udf(returnType=T.DateType(), useArrow=True)  # An Arrow Python UDF
def arrow_datetype_convert(s: str | None):
    if s == "" or s is None:
        return date.today()
    return datetime.strptime(s, "%d/%b/%Y")


def generate_spark_data_profile(df: DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Generates a data profile with various metrics for each column in a PySpark DataFrame.

    Args:
        df (DataFrame): The PySpark DataFrame to profile.

    Returns:
        dict: A dictionary where each key is a column name, and each value is another dictionary
              containing metrics for that column.
    """
    getcontext().prec = 4
    profile = {}

    # Total number of rows
    total_rows = df.count()

    for column in df.columns:
        # Initialize a dictionary for each column
        column_profile = {}

        # Count nulls and calculate null percentage
        null_count = df.filter(F.col(column).isNull()).count()
        null_percent = Decimal(str(null_count / total_rows)) * Decimal(100)
        column_profile["contagem_nulos"] = null_count
        if null_percent is None:
            column_profile["percentual_nulos"] = Decimal('0.0')
        else:
            column_profile["percentual_nulos"] = null_percent
        

        # Count unique values and calculate unique percentage
        unique_count = df.filter(df[column].isNotNull()).select(column).distinct().count()
        unique_percent = Decimal(str(unique_count / total_rows)) * Decimal(100)
        column_profile["contagem_unicos"] = unique_count
        if unique_percent is None:
            column_profile["percentual_unicos"] = Decimal('0.0')
        else:
            column_profile["percentual_unicos"] = unique_percent

        

        # If the column is numeric, calculate mean, max, and min
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
        # else:
        #     # Set to None for non-numeric columns
        #     column_profile['mean'] = None
        #     column_profile['max'] = None
        #     column_profile['min'] = None

        # Add the column profile to the overall profile
        profile[column] = column_profile

    return profile


def remove_high_null_percentage_columns(
    df: DataFrame, max_null_percent: float
) -> tuple[DataFrame, list[str]]:
    """
    Removes columns from the DataFrame that have a higher percentage of null values than `max_null_percent`.

    Args:
        df (DataFrame): The PySpark DataFrame to process.
        max_null_percent (float): Maximum allowed percentage of null values for a column to be retained.

    Returns:
        Tuple[DataFrame, List[str]]: A tuple containing the modified DataFrame with high-null columns removed,
                                     and a list of the columns that were removed.
    """
    # Total number of rows in the DataFrame
    total_rows = df.count()

    # List to store columns to remove
    columns_to_remove = []

    # Iterate over columns to calculate null percentage
    for column in df.columns:
        # Count nulls in the column
        null_count = df.filter(df[column].isNull()).count()
        null_percent = (null_count / total_rows) * 100

        # Check if the null percentage exceeds the threshold
        if null_percent > max_null_percent:
            columns_to_remove.append(column)

    # Drop the columns with high null percentage from the DataFrame
    df_filtered = df.drop(*columns_to_remove)

    return df_filtered, columns_to_remove


def remove_low_cardinality_columns(
    df: DataFrame, min_unique_values: int
) -> tuple[DataFrame, list[str]]:
    """
    Removes columns from the DataFrame that have fewer than `min_unique_values` unique values.

    Args:
        df (DataFrame): The PySpark DataFrame to process.
        min_unique_values (int): Minimum number of unique values required to keep a column.

    Returns:
        Tuple[DataFrame, List[str]]: A tuple containing the modified DataFrame with low-cardinality columns removed,
                                     and a list of the columns that were removed.
    """
    # List to store columns to remove
    columns_to_remove = []

    # Iterate over columns and calculate unique count
    for column in df.columns:
        unique_count = df.select(column).distinct().count()
        if unique_count <= min_unique_values:
            columns_to_remove.append(column)

    # Drop the low-cardinality columns from the DataFrame
    df_filtered = df.drop(*columns_to_remove)

    return df_filtered, columns_to_remove

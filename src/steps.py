from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import day_of_week_mapping


def rename_and_select_cols(df: DataFrame) -> DataFrame:
    """
    Renomeia colunas específicas do DataFrame e remove colunas indesejadas.

    :param df: DataFrame original com colunas renomeadas.
    :return: DataFrame com colunas renomeadas e colunas indesejadas removidas.
    """
    df = df.withColumnRenamed("_c0", "ip_requisicao")
    df = df.withColumnRenamed("_c6", "http_status_code")
    df = df.withColumnRenamed("_c7", "volume_resposta")
    df = df.withColumnRenamed("_c3", "data_horario_requisicao")
    df = df.withColumnRenamed("_c5", "verbo_rota")
    df = df.drop("_c2")
    df = df.drop("_c4")
    return df


def extract_route_and_verb_from_str(df: DataFrame, col_name: str) -> DataFrame:
    """
    Extrai o verbo e a rota a partir de uma coluna de string formatada.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna que contém o verbo e a rota.
    :return: DataFrame com as novas colunas 'verbo' e 'rota' e a coluna original removida.
    """
    df = df.withColumn("verbo", F.split(df["verbo_rota"], " ").getItem(0))

    df = df.withColumn("rota", F.split(df["verbo_rota"], " ").getItem(1))

    df = df.withColumn("rota", F.trim(F.split(df["rota"], "\?").getItem(0)))
    df = df.drop("verbo_rota")
    return df


def extract_date_from_str(
    df: DataFrame, col_name: str, sep: str, pos: int
) -> tuple[DataFrame, str]:
    """
    Extrai uma parte da string de uma coluna com base em um separador e posição.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna a ser processada.
    :param separador: Separador utilizado para dividir a string.
    :param posicao: Posição do item a ser extraído após a divisão.
    :return: Tuple contendo o DataFrame atualizado e o nome da nova coluna criada.
    """
    df = df.withColumn(f"raw_{col_name}", F.split(df[col_name], sep).getItem(pos))
    return df, f"raw_{col_name}"


def clean_str(df: DataFrame, col_name: str) -> DataFrame:
    """
    Limpa a string removendo caracteres específicos.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna a ser limpa.
    :return: DataFrame com a coluna limpa.
    """
    df = df.withColumn(col_name, F.split(df[col_name], "\[").getItem(1))
    df = df.withColumn(col_name, F.split(df[col_name], "\]").getItem(0))
    return df


def str_to_date(df: DataFrame, col_name: str, method: callable) -> DataFrame:
    """
    Converte uma string para um formato de data usando um método de conversão fornecido.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna que contém a string a ser convertida.
    :param metodo_conversao: Função de conversão a ser aplicada.
    :return: DataFrame com a nova coluna de data e a coluna original removida.
    """
    col_name_aux = col_name.split("raw_")[1]
    df = df.withColumn(f"trat_{col_name_aux}", method(F.col(col_name)))
    df = df.drop(col_name)
    return df


def build_date_column(
    df: DataFrame, col_name: str, sep: str, pos: int, method: callable
) -> DataFrame:
    """
    Constrói uma coluna de data a partir de uma coluna de string utilizando extração e conversão.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna a ser processada.
    :param separador: Separador utilizado para dividir a string.
    :param posicao: Posição do item a ser extraído após a divisão.
    :param metodo_conversao: Função de conversão a ser aplicada para transformar a string em data.
    :return: DataFrame com a nova coluna de data.
    """
    df, nome_coluna = extract_date_from_str(df, col_name, ":", 0)
    df = clean_str(df, nome_coluna)
    df = str_to_date(df, nome_coluna, method)
    return df


def build_day_of_week_column(df: DataFrame) -> DataFrame:
    """
    Constrói uma coluna que representa o dia da semana com base na coluna de data.

    :param df: DataFrame original.
    :return: DataFrame com a nova coluna 'trat_dia_da_semana'.
    """
    df = df.withColumn("dia_da_semana", F.dayofweek("trat_data_horario_requisicao"))

    mapping_expr = F.create_map([F.lit(x) for x in chain(*day_of_week_mapping.items())])
    df = df.withColumn("trat_dia_da_semana", mapping_expr[F.col("dia_da_semana")])
    df = df.drop("dia_da_semana")
    return df


def build_time_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Constrói uma coluna de horário a partir de uma coluna de data e hora.

    :param df: DataFrame original.
    :param nome_coluna: Nome da coluna que contém a data e hora.
    :return: DataFrame com a nova coluna 'horario'.
    """
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

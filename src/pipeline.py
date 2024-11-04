import structlog
import json
from pprint import pprint
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import caminhos, spark_config, config_banco
from data_utils import (
    generate_spark_data_profile,
    remove_high_null_percentage_columns,
    remove_low_cardinality_columns,
    arrow_datetype_convert,
)

from steps import (
    rename_and_select_cols,
    extract_route_and_verb_from_str,
    build_day_of_week_column,
    build_date_column,
    build_time_column,
)

from infra import start_spark_session, configure_logger

configure_logger("pipeline")
logger = structlog.get_logger()


spark_session = start_spark_session(logger, "sparkapp", config_dict=spark_config)

"""
    Inicio do bloco de tratamento de dados
"""
caminho_input = caminhos["raw"].joinpath("access_log.txt")
if not caminho_input.exists():
    logger.error("Caminho de entrada não existe", caminho=caminho_input)

df = spark_session.read.option("delimiter", " ").csv(str(caminho_input))

df, colunas_removidas = remove_high_null_percentage_columns(df, 0.95)
logger.info("Colunas removidas por falta de preenchimento", colunas=colunas_removidas)
df, colunas_removidas = remove_low_cardinality_columns(df, 5)
logger.info("Colunas removidas por falta de variabilidade", colunas=colunas_removidas)
logger.info("Perfil de dados", profile=generate_spark_data_profile(df))


df = rename_and_select_cols(df)
logger.info("Renomeando colunas")

df = extract_route_and_verb_from_str(df, "verbo_rota")
logger.info("Construindo colunas rota e verbo")

df = build_date_column(df, "data_horario_requisicao", ":", 0, arrow_datetype_convert)
logger.info("Construindo coluna data")

df = build_time_column(df, "data_horario_requisicao")
logger.info("Construindo coluna horário")


logger.info("Construindo coluna dia da semana")
df = build_day_of_week_column(df)

df = df.withColumn("volume_resposta", F.col("volume_resposta").cast("float"))


logger.info("Persistindo dados em área de staging")
df.coalesce(1).write.option("overwriteSchema", "true").mode(
    saveMode="overwrite"
).format("delta").save(str(caminhos["stagging"].joinpath("tabela_processada/")))


"""
    Inicio do bloco de geração de métricas
"""
results: dict[str, list | float] = {}


stagged_df = spark_session.read.format("delta").load(
    str(caminhos["stagging"].joinpath("tabela_processada/"))
)

logger.info("Calculando top 10 clientes ip por quantidade de requisições")
top_10_clients_ip = (
    stagged_df.groupBy("ip_requisicao")
    .agg(F.count("ip_requisicao").alias("requisicoes_por_ip"))
    .sort(F.desc("requisicoes_por_ip"))
).limit(10)

results["top_10_clients_ip"] = json.loads(
    top_10_clients_ip.toPandas().to_json(orient="records")
)


logger.info("Calculando top 6 endpoints por quantidade de requisições, desconsiderando parâmetros de rota")
top_6_endpoints_ip = (
    stagged_df.groupBy("rota")
    .agg(F.count("rota").alias("requisicoes_por_rota"))
    .sort(F.desc("requisicoes_por_rota"))
).limit(6)

results["top_6_endpoints_ip"] = json.loads(
    top_6_endpoints_ip.toPandas().to_json(orient="records")
)

logger.info("Calculando número de clients (ips) distintos")
clientes_distintos = stagged_df.select("ip_requisicao").distinct().count()
results["clientes_distintos"] = clientes_distintos

logger.info("CalcCodeElevate
logger.info("Calculando métricas de volume de dados das requisições")
total_bytes_trafegados = stagged_df.agg(F.sum("volume_resposta")).first()[0]
results["total_bytes_trafegados"] = total_bytes_trafegados

max_bytes_trafegados = stagged_df.agg(F.max("volume_resposta")).first()[0]
results["max_bytes_trafegados"] = max_bytes_trafegados

min_bytes_trafegados = stagged_df.agg(F.min("volume_resposta")).first()[0]
results["min_bytes_trafegados"] = min_bytes_trafegados

avg_bytes_trafegados = stagged_df.agg(F.mean("volume_resposta")).first()[0]
results["avg_bytes_trafegados"] = avg_bytes_trafegados


logger.info("Encontrando dia da semana com maior número de erros de cliente (400)")
dia_semana_erro400 = (
    stagged_df.filter(F.col("http_status_code") == 400)
    .groupBy("trat_dia_da_semana")
    .agg(F.count("trat_dia_da_semana").alias("requisicoes_erro400_por_dia_semana"))
)
dia_semana_erro400 = dia_semana_erro400.orderBy(
    F.desc("requisicoes_erro400_por_dia_semana")
).limit(1)
results["dia_semana_erro400"] = json.loads(
    dia_semana_erro400.toPandas().to_json(orient="records")
)


logger.info("Gravando arquivo de resultados", path=caminhos["prod"].joinpath("resultado.json"))
with open(caminhos["prod"].joinpath("resultado.json"), "w") as f:
    json.dump(results, f, sort_keys=True, indent=4)

pprint(results)


# stagged_df.select(*df.columns).write.format("jdbc").option(
#     "url", config_banco["url"]
# ).option("driver", "org.postgresql.Driver").option(
#     "dbtable", config_banco["table"]
# ).option("user", config_banco["user"]).option(
#     "password", config_banco["password"]
# ).mode("overwrite").save()
# # logger.exception("Erro ao salvar no banco", exception=e)

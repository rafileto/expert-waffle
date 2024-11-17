import os
from pathlib import Path
from decimal import getcontext


caminho_base = Path(os.getenv("BASE_PATH", "/app"))
caminhos = {
    "logs": caminho_base.joinpath("logs"),
    "raw": caminho_base.joinpath("data").joinpath("raw"),
    "stagging": caminho_base.joinpath("data").joinpath("stagging"),
    "prod": caminho_base.joinpath("data").joinpath("prod"),
}

spark_config = {
    "spark.executor.memory": "8g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "4g",
    "spark.jars": "/opt/spark/jars/postgresql-42.7.4.jar",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

config_banco = {
    "url": "jdbc:postgresql://localhost:5432/warehouse",
    "table": "trat_log",
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver",
}

day_of_week_mapping = {
    "1": "Dom",
    "2": "Seg",
    "3": "Ter",
    "4": "Qua",
    "5": "Qui",
    "6": "Sex",
    "7": "Sab",
}

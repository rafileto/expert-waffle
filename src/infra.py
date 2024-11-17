import structlog
import logging
from pathlib import Path
from logging import Logger, FileHandler

import pyspark
from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

from config import caminhos


def configure_logger(nome_processo: str):
    """
    Configura o logger utilizando structlog e o módulo padrão de logging com encoding UTF-8.

    :param nome_log: Nome do logger a ser configurado.
    """

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(encoding='utf-8', errors='replace'),
            structlog.processors.JSONRenderer()
            
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.WriteLoggerFactory(
            file=caminhos["logs"].joinpath(f"{nome_processo}.log").open("wt")
        ),
    )


def start_spark_session(
    logger: Logger,
    app_name: str = "MySparkApp",
    master: str = "local[*]",
    config_dict: dict | None = None,
) -> SparkSession:
    """
    Start a Spark session with customizable options.

    Args:
        app_name (str): Name of the Spark application.
        master (str): Cluster URL to connect to, or "local" to run locally.
                      "local[*]" runs with as many worker threads as logical cores.
        config_dict (dict): Additional Spark configuration options as a dictionary.

    Returns:
        SparkSession: Configured Spark session.
    """
    # Initialize the Spark session builder
    spark_builder = SparkSession.builder.appName(app_name).master(master)
    # Apply additional configuration options if provided
    if config_dict:
        for key, value in config_dict.items():
            spark_builder = spark_builder.config(key, value)

    # Create the Spark session
    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Versão PySpark", pyspark_version=pyspark.__version__)
    logger.info("Spark configurado com os parâmetros", parametros=config_dict)

    logger.info(f"Spark session iniciada App '{app_name}' e configurações '{master}'")
    return spark

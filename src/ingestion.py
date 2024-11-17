import os
import gdown
import zipfile
from logging import Logger
from pathlib import Path


def download_input(logger: Logger, caminho_saida: Path):
    """
    Baixa um arquivo do Google Drive usando gdown e o salva no diretório especificado.

    :param logger: Instância do Logger para registrar eventos.
    :param diretorio_saida: Objeto Path representando o diretório onde o arquivo será salvo.
    :param nome_arquivo: Nome do arquivo a ser salvo localmente. Padrão é "access_log.zip".
    :return: Objeto Path apontando para o arquivo baixado.
    """
    try:
        url = "https://drive.google.com/uc?id=1p5fWpn53iuMZx06uRM9VEwRgAdgjDCii"

        logger.info("Iniciando donwload", rota=url)

        output = str(caminho_saida.joinpath("access_log.zip"))

        gdown.download(url, output, quiet=False)
        logger.info("Donwload finalizado", rota=url, caminho_local=output)

        return caminho_saida.joinpath("access_log.zip")
    except Exception as e:
        logger.error("Falha no donwload", exc_info=True)


def extract_all(zip_file_path: Path, extraction_path: Path, logger):
    """
    Extrai todo o conteúdo de um arquivo ZIP para o diretório especificado e remove o arquivo ZIP após a extração.

    :param caminho_zip: Objeto Path apontando para o arquivo ZIP.
    :param diretorio_extracao: Objeto Path representando o diretório onde os arquivos serão extraídos.
    :param logger: Instância do Logger para registrar eventos.
    """
    try:
        os.makedirs(extraction_path, exist_ok=True)

        logger.info(
            "Iniciando extração do arquivo",
            caminho=zip_file_path,
            saida=extraction_path,
        )
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(extraction_path)

        logger.info("Todos os arquivos extraídos", saida=extraction_path)
        __delete_file(zip_file_path, logger)
        logger.info("Arquivo zip removido", arquivo_removido=zip_file_path)

    except FileNotFoundError as e:
        logger.error("Arquivo não existe", exc_info=True)
    except zipfile.BadZipFile as e:
        logger.error("Arquivo zip inválido", exc_info=True)
    except PermissionError as e:
        logger.error("Sem permissão para acessar o arquivo", exc_info=True)
    except Exception as e:
        logger.error("Falha ao extrair arquivos", exc_info=True)


def __delete_file(caminho_arquivo: Path, logger: Logger) -> None:
    """
    Deleta o arquivo especificado.

    :param caminho_arquivo: Objeto Path apontando para o arquivo a ser deletado.
    :param logger: Instância do Logger para registrar eventos.
    """
    try:
        if caminho_arquivo.is_file():
            caminho_arquivo.unlink()
            logger.info(f"Arquivo deletado: {caminho_arquivo}")
        else:
            logger.warning(f"Arquivo não existe ou não é um arquivo: {caminho_arquivo}")

    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {caminho_arquivo}.", exc_info=True)
    except PermissionError as e:
        logger.error(
            f"Permissão negada ao deletar o arquivo: {caminho_arquivo}.", exc_info=True
        )
    except Exception as e:
        logger.error(f"Falha ao deletar o arquivo.", exc_info=True)

## Dependências

São necessários para a execução o binários do docker e makefile instalado para o pipeline de integração (build, testes e execução)

- Docker
- Make (Ubuntu ou WSL)

## Execução

Para executar as rotinas é necessário baixar o arquivo de https://drive.google.com/file/d/1p5fWpn53iuMZx06uRM9VEwRgAdgjDCii/view?usp=sharing, descompactar e salvá-lo no diretório data/raw.

- Entrar no diretório do projeto

```shell
cd diretorio/clone_repositorio
cp -r diretorio/download/arquivo/access_log.zip data/raw
unzip data/raw/file.zip -d data/raw/
rm -rf data/raw/file.zip
```

Após esta etapa, basta rodar o comando 'make all'.
```shell
make all
```

Caso make não esteja presente é possível executar 
```shell
export DOCKER_IMAGE=codeelevate
export SPARK_JOB_SCRIPT=src/pipeline.py

docker build -t $(DOCKER_IMAGE) .
docker run --rm $(DOCKER_IMAGE) python -m unittest discover -s tests -p "test_*.py"
docker run --rm $(DOCKER_IMAGE) spark-submit --master local[4] $(SPARK_JOB_SCRIPT)
```

## Resultados
Os resultados são apresentar como um JSON, tanto na saída da execução como arquivo no diretório data/prod.

Cada métrica no questionário possui uma entrada no arquivo seguido de sua respectiva resposta. Os dataframes foram serializados para coleções de records, valores interios são apresentados apenas com o valor
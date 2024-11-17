## Dependências

São necessários para a execução o binários do docker e makefile instalado para o pipeline de integração (build, testes e execução)

- Docker
- Make (Ubuntu ou WSL)

## Execução

O download dos inputs é automático, bastante executar o pipeline de integração

Após esta etapa, basta rodar o comando 'make all'.
```shell
make all
```

Caso make não esteja presente é possível executar 
```shell
export DOCKER_IMAGE=codeelevate
export SPARK_JOB_SCRIPT=src/pipeline.py

docker build -t $(DOCKER_IMAGE) .
docker run --rm $(DOCKER_IMAGE) BASE_PATH="/app" python -m unittest discover -s tests -p "test_*.py"
docker run --rm $(DOCKER_IMAGE) BASE_PATH="/app" spark-submit --master local[4] $(SPARK_JOB_SCRIPT)
```

## Resultados
Os resultados são apresentar como um JSON, tanto na saída da execução como arquivo no diretório data/prod.

Cada métrica no questionário possui uma entrada no arquivo seguido de sua respectiva resposta. Os dataframes foram serializados para coleções de records, valores interios são apresentados apenas com o valor
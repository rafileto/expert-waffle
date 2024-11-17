# Variables
DOCKER_IMAGE=codeelevate
SPARK_JOB_SCRIPT=src/pipeline.py




# Target to run tests in Docker container
test:
	python -m unittest discover -s tests -p "test_*.py"

# Target to build the Docker image
build:
	docker build -t $(DOCKER_IMAGE) .

# Target to run the Spark job if tests pass
run:
	docker run --rm $(DOCKER_IMAGE) spark-submit --master local[4] $(SPARK_JOB_SCRIPT)

# Full workflow: build, test, then run Spark job if tests pass
all: test build run

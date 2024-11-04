# Use an official OpenJDK runtime as a parent image
FROM ubuntu:22.04

# Set environment variables for Spark and Hadoop versions
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Install dependencies and Python 3.10

# Install dependencies and Python 3.10
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get install -y curl wget software-properties-common && \
    apt-get install -y openjdk-8-jre python3.10 python3.10-distutils python3-pip && \
    apt-get install -y python3.10 python3.10-distutils python3-pip && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Spark
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN java -version
# Install PySpark and any other dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Copy the application code into the container
COPY . /app/

# Set up the working directory
WORKDIR /app


# Set the entry point to run the PySpark application
ENTRYPOINT ["python3", "src/pipeline.py"]

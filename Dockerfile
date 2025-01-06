# Use an Airflow base image
FROM apache/airflow:2.6.3

# Set build arguments for Spark
ARG SPARK_VERSION=3.5.4
ARG HADOOP_VERSION=3

# Install Java, Bash, and dependencies
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     openjdk-17-jdk \
     wget \
     curl \
     ca-certificates \
     gnupg \
     libpq-dev \
     software-properties-common \
     bash \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

#  Explicitly create a symbolic link for Bash
RUN ln -sf /bin/bash /usr/bin/bash

# Install Spark 3.5.4
RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz \
  && tar -xzf /tmp/spark.tgz -C /opt/ \
  && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
  && rm /tmp/spark.tgz

# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Install Python packages required for Airflow DAGs
USER airflow
RUN pip install --no-cache-dir \
  nba_api \
  pandas \
  confluent-kafka \
  psycopg2-binary \
  paramiko \
  pyspark==3.4.4 \
  kafka-python \
  apache-airflow-providers-apache-spark

# Switch back to root to ensure proper permissions
USER root

# Ensure Spark and Java paths exist and adjust ownership
RUN test -d $SPARK_HOME && chown -R airflow: $SPARK_HOME || echo "$SPARK_HOME does not exist, skipping chown" \
  && test -d $JAVA_HOME && chown -R airflow: $JAVA_HOME || echo "$JAVA_HOME does not exist, skipping chown"

# Add Spark configuration directory
RUN mkdir -p $SPARK_HOME/conf \
  && chown -R airflow: $SPARK_HOME/conf

# Set default shell explicitly to Bash
SHELL ["/bin/bash"]

# Switch back to airflow user
USER airflow

# Copy Airflow configuration files, DAGs, and plugins
COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins

# Set the entrypoint to use Airflow commands by default
ENTRYPOINT ["/entrypoint"]
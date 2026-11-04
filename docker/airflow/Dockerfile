# Airflow runs Spark drivers in client mode: it needs Java, plus the same PySpark,
# Python minor version and connector jars as the Spark workers.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG SPARK_PROVIDER_VERSION=6.3.2
ARG SPARK_VERSION=4.1.4
ARG DELTA_VERSION=4.4.1
ARG GCS_CONNECTOR_VERSION=4.0.9
ARG INSTALL_CLOUD_CONNECTORS=false

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /opt/java \
    && rm -rf /var/lib/apt/lists/*
USER airflow

ENV JAVA_HOME=/opt/java \
    SPARK_CONF_DIR=/opt/airflow/config/spark \
    PYTHONPATH=/opt/airflow/src:/opt/airflow

# The slim image has no database driver. Providers follow Airflow's constraints;
# PySpark and Delta follow the project pins.
COPY requirements/base.txt requirements/spark.txt /tmp/requirements/
RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt" \
        "apache-airflow-providers-apache-spark==${SPARK_PROVIDER_VERSION}" \
        apache-airflow-providers-postgres \
    && pip install --no-cache-dir -r /tmp/requirements/spark.txt

COPY docker/common/install_spark_jars.sh /tmp/install_spark_jars.sh
RUN SPARK_VERSION="${SPARK_VERSION}" \
    DELTA_VERSION="${DELTA_VERSION}" \
    GCS_CONNECTOR_VERSION="${GCS_CONNECTOR_VERSION}" \
    INSTALL_CLOUD_CONNECTORS="${INSTALL_CLOUD_CONNECTORS}" \
    bash /tmp/install_spark_jars.sh

COPY --chown=airflow:root config /opt/airflow/config
COPY --chown=airflow:root schemas /opt/airflow/schemas
COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root jobs /opt/airflow/jobs

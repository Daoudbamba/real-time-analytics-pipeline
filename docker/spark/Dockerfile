# Built on Python rather than the official Spark image: that image gets Python
# from Ubuntu 22.04 (3.10), while drivers and workers need the same minor version
# as the Airflow image (3.12).
FROM python:3.12-slim-bookworm

ARG SPARK_VERSION=4.1.4
ARG DELTA_VERSION=4.4.1
ARG GCS_CONNECTOR_VERSION=4.0.9
ARG INSTALL_CLOUD_CONNECTORS=false

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/app/config/spark \
    PYTHONPATH=/opt/app/src:/opt/app \
    PYTHONUNBUFFERED=1 \
    LAKEHOUSE_ROOT=/lakehouse

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && ln -s /usr/lib/jvm/java-17-openjdk-* "${JAVA_HOME}" \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/base.txt requirements/spark.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/spark.txt

COPY docker/common/install_spark_jars.sh /tmp/install_spark_jars.sh
RUN SPARK_VERSION="${SPARK_VERSION}" \
    DELTA_VERSION="${DELTA_VERSION}" \
    GCS_CONNECTOR_VERSION="${GCS_CONNECTOR_VERSION}" \
    INSTALL_CLOUD_CONNECTORS="${INSTALL_CLOUD_CONNECTORS}" \
    bash /tmp/install_spark_jars.sh

RUN useradd --uid 1000 --gid 0 --create-home spark \
    && mkdir -p /opt/app /lakehouse \
    && chown spark:root /opt/app /lakehouse

COPY --chown=spark:root config /opt/app/config
COPY --chown=spark:root schemas /opt/app/schemas
COPY --chown=spark:root src /opt/app/src
COPY --chown=spark:root jobs /opt/app/jobs

USER spark
WORKDIR /opt/app
VOLUME /lakehouse

FROM python:3.12-slim-bookworm

ENV PYTHONPATH=/opt/app/src:/opt/app \
    PYTHONUNBUFFERED=1

COPY requirements/base.txt requirements/producer.txt /tmp/requirements/
RUN pip install --no-cache-dir -r /tmp/requirements/producer.txt

RUN useradd --uid 1000 --create-home producer \
    && mkdir -p /opt/app \
    && chown producer /opt/app

COPY --chown=producer config /opt/app/config
COPY --chown=producer schemas /opt/app/schemas
COPY --chown=producer src /opt/app/src
COPY --chown=producer producers /opt/app/producers

USER producer
WORKDIR /opt/app
CMD ["python", "-m", "producers.kafka_producer"]

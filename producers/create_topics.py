"""Creates the Kafka topics of config/kafka/topics.yaml. Safe to run repeatedly."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from rtap.config import load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.topics")

TOPICS_PATH = Path(__file__).resolve().parents[1] / "config" / "kafka" / "topics.yaml"
BROKER_WAIT_SECONDS = 60
TIMEOUT_SECONDS = 30


def build_topics(definition: dict[str, Any]) -> list[NewTopic]:
    """Translate the YAML definition, applying its defaults."""
    defaults = definition["defaults"]
    return [
        NewTopic(
            topic["name"],
            num_partitions=topic["partitions"],
            replication_factor=topic.get("replication_factor", defaults["replication_factor"]),
            config={"retention.ms": str(topic.get("retention_ms", defaults["retention_ms"]))},
        )
        for topic in definition["topics"]
    ]


def wait_for_broker(admin: AdminClient) -> None:
    """Wait until the broker answers, as it may still be starting."""
    deadline = time.monotonic() + BROKER_WAIT_SECONDS
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)


def create_topics(admin: AdminClient, topics: list[NewTopic]) -> None:
    """Create the missing topics. Existing ones are left unchanged."""
    for name, future in admin.create_topics(topics, request_timeout=TIMEOUT_SECONDS).items():
        try:
            future.result(timeout=TIMEOUT_SECONDS)
            logger.info("Topic created", extra={"topic": name})
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            logger.info("Topic already exists", extra={"topic": name})


def main() -> int:
    """Command line entry point. Returns the process exit code."""
    config = load_config()
    configure_logging(config.log_level)
    definition = yaml.safe_load(TOPICS_PATH.read_text(encoding="utf-8"))
    admin = AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers})
    try:
        wait_for_broker(admin)
        create_topics(admin, build_topics(definition))
    except KafkaException:
        logger.exception("Topic creation failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

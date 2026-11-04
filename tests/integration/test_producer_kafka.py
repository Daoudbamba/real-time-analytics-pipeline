"""Publishes through a real Kafka broker.

Needs a broker reachable from the host:
    KAFKA_TEST_BOOTSTRAP=localhost:9094 pytest -m integration
"""

import json
import os
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import CollectorRegistry

from producers.event_generator import EventGenerator
from producers.kafka_producer import EventPublisher, ProducerMetrics, build_client_config
from rtap.config import load_config

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP", "localhost:9094")
PARTITIONS = 3
EVENT_COUNT = 1_000


@pytest.fixture
def topic() -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    name = f"test-{uuid.uuid4().hex[:8]}"
    new_topic = NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)
    admin.create_topics([new_topic])[name].result(timeout=30)
    yield name
    admin.delete_topics([name])[name].result(timeout=30)


def consume(topic: str, expected: int) -> list:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    messages = []
    idle_polls = 0
    while idle_polls < 10 and len(messages) <= expected:
        message = consumer.poll(1.0)
        if message is None:
            idle_polls += 1
        elif message.error() is None:
            messages.append(message)
    consumer.close()
    return messages


def test_every_event_reaches_kafka_exactly_once(topic: str) -> None:
    kafka = load_config(env={"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP}).kafka
    client = Producer(build_client_config(kafka, "integration-tests"))
    publisher = EventPublisher(client, topic, ProducerMetrics(CollectorRegistry()))
    generator = EventGenerator(invalid_ratio=0.1, duplicate_ratio=0.05, seed=7)
    events = [generator.next_event() for _ in range(EVENT_COUNT)]

    for event in events:
        publisher.publish(event)
    undelivered = publisher.close(timeout=30)

    assert (undelivered, publisher.delivered, publisher.failed) == (0, EVENT_COUNT, 0)
    messages = consume(topic, EVENT_COUNT)
    assert len(messages) == EVENT_COUNT
    assert sorted(message.value() for message in messages) == sorted(e.value for e in events)
    assert len({message.partition() for message in messages}) == PARTITIONS


def test_a_user_stays_on_one_partition(topic: str) -> None:
    kafka = load_config(env={"KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP}).kafka
    client = Producer(build_client_config(kafka, "integration-tests"))
    publisher = EventPublisher(client, topic, ProducerMetrics(CollectorRegistry()))
    generator = EventGenerator(seed=8)

    for _ in range(EVENT_COUNT):
        publisher.publish(generator.next_event())
    publisher.close(timeout=30)

    partitions_by_user: dict[str, set[int]] = {}
    for message in consume(topic, EVENT_COUNT):
        user_id = json.loads(message.value())["user_id"]
        partitions_by_user.setdefault(user_id, set()).add(message.partition())
    assert all(len(partitions) == 1 for partitions in partitions_by_user.values())

"""Tests of the Kafka publishing logic, with a fake Kafka client."""

import threading
import time
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from prometheus_client import CollectorRegistry

from producers import kafka_producer
from producers.event_generator import EventGenerator, GeneratedEvent
from producers.kafka_producer import (
    EventPublisher,
    ProducerMetrics,
    PublishError,
    build_client_config,
    run,
)
from rtap.config import load_config


class FakeMessage:
    def __init__(self, topic: str, key: str | None, latency: float | None) -> None:
        self._topic, self._key, self._latency = topic, key, latency

    def topic(self) -> str:
        return self._topic

    def key(self) -> str | None:
        return self._key

    def latency(self) -> float | None:
        return self._latency


class FakeClient:
    """Records messages and can simulate a full queue or a refused event."""

    def __init__(self, full_queue_errors: int = 0, refuse: bool = False) -> None:
        self.messages: list[dict[str, Any]] = []
        self.pending: list[tuple[Callable[..., None], FakeMessage]] = []
        self.full_queue_errors = full_queue_errors
        self.refuse = refuse
        self.polls: list[float] = []
        self.unconfirmed = 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self.refuse:
            raise KafkaException(KafkaError(KafkaError._INVALID_ARG))
        if self.full_queue_errors:
            self.full_queue_errors -= 1
            raise BufferError("Queue full")
        self.messages.append({"topic": topic, **kwargs})
        message = FakeMessage(topic, kwargs["key"], 0.02)
        self.pending.append((kwargs["on_delivery"], message))

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, timeout: float) -> int:
        return self.unconfirmed

    def acknowledge_all(self, error: KafkaError | None = None) -> None:
        for callback, message in self.pending:
            callback(error, message)
        self.pending.clear()


@pytest.fixture
def registry() -> CollectorRegistry:
    return CollectorRegistry()


@pytest.fixture
def metrics(registry: CollectorRegistry) -> ProducerMetrics:
    return ProducerMetrics(registry)


EVENT = GeneratedEvent({"user_id": "U000001", "amount": 10.5}, "valid")


def test_publish_sends_key_and_json_value(metrics: ProducerMetrics) -> None:
    client = FakeClient()

    EventPublisher(client, "ecommerce-events", metrics).publish(EVENT)

    message = client.messages[0]
    assert message["topic"] == "ecommerce-events"
    assert message["key"] == "U000001"
    assert message["value"] == b'{"user_id":"U000001","amount":10.5}'


def test_acknowledged_events_are_counted(
    metrics: ProducerMetrics, registry: CollectorRegistry
) -> None:
    client = FakeClient()
    publisher = EventPublisher(client, "topic", metrics)

    publisher.publish(EVENT)
    assert (publisher.sent, publisher.delivered) == (1, 0)
    client.acknowledge_all()

    assert (publisher.delivered, publisher.failed) == (1, 0)
    delivered = registry.get_sample_value(
        "rtap_producer_events_delivered_total", {"topic": "topic"}
    )
    sent = registry.get_sample_value("rtap_producer_events_sent_total", {"kind": "valid"})
    latency_count = registry.get_sample_value("rtap_producer_delivery_latency_seconds_count")
    assert (delivered, sent, latency_count) == (1, 1, 1)


def test_delivery_failures_are_counted_by_error(
    metrics: ProducerMetrics, registry: CollectorRegistry
) -> None:
    client = FakeClient()
    publisher = EventPublisher(client, "topic", metrics)

    publisher.publish(EVENT)
    client.acknowledge_all(KafkaError(KafkaError._MSG_TIMED_OUT))

    assert (publisher.delivered, publisher.failed) == (0, 1)
    failures = registry.get_sample_value(
        "rtap_producer_delivery_failures_total", {"error": "_MSG_TIMED_OUT"}
    )
    assert failures == 1


def test_full_queue_waits_then_succeeds(
    metrics: ProducerMetrics, registry: CollectorRegistry
) -> None:
    client = FakeClient(full_queue_errors=2)
    publisher = EventPublisher(client, "topic", metrics)

    publisher.publish(EVENT)

    assert publisher.sent == 1
    assert client.polls.count(kafka_producer.BUFFER_WAIT_SECONDS) == 2
    assert registry.get_sample_value("rtap_producer_buffer_full_total") == 2


def test_queue_that_stays_full_raises(metrics: ProducerMetrics) -> None:
    client = FakeClient(full_queue_errors=kafka_producer.MAX_BUFFER_RETRIES)

    with pytest.raises(PublishError, match="stayed full"):
        EventPublisher(client, "topic", metrics).publish(EVENT)


def test_refused_event_raises(metrics: ProducerMetrics) -> None:
    with pytest.raises(PublishError, match="refused"):
        EventPublisher(FakeClient(refuse=True), "topic", metrics).publish(EVENT)


def test_close_returns_unconfirmed_events(metrics: ProducerMetrics) -> None:
    client = FakeClient()
    client.unconfirmed = 3

    assert EventPublisher(client, "topic", metrics).close(timeout=1) == 3


def test_run_stops_after_max_events(metrics: ProducerMetrics) -> None:
    client = FakeClient()
    publisher = EventPublisher(client, "topic", metrics)

    run(publisher, EventGenerator(seed=1), 100_000, threading.Event(), max_events=25)

    assert publisher.sent == 25


def test_run_stops_when_requested(metrics: ProducerMetrics) -> None:
    client = FakeClient()
    publisher = EventPublisher(client, "topic", metrics)
    stop = threading.Event()
    stop.set()

    run(publisher, EventGenerator(seed=1), 10, stop)

    assert publisher.sent == 0


def test_run_respects_the_target_rate(metrics: ProducerMetrics) -> None:
    publisher = EventPublisher(FakeClient(), "topic", metrics)

    started = time.monotonic()
    run(publisher, EventGenerator(seed=1), 100, threading.Event(), max_events=21)

    assert time.monotonic() - started >= 0.19


def test_client_config_enables_reliable_delivery() -> None:
    kafka = load_config(env={}).kafka

    config = build_client_config(kafka, "tests")

    assert config["bootstrap.servers"] == "kafka:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["delivery.timeout.ms"] == 120_000

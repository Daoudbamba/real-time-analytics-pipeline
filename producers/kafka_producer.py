"""Publishes the generated events to Kafka at a steady rate.

Run it with `python -m producers.kafka_producer --help`.

Reliability relies on the idempotent Kafka producer (acks=all): librdkafka retries
failed sends until delivery.timeout.ms without creating duplicates or reordering
messages. This module adds the local buffer back-pressure, delivery tracking and
clean shutdown. Exit code 1 means that at least one event was not delivered.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from producers.event_generator import EventGenerator, GeneratedEvent
from rtap.config import AppConfig, KafkaConfig, load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.producer")

BUFFER_WAIT_SECONDS = 0.5
MAX_BUFFER_RETRIES = 240
FLUSH_TIMEOUT_SECONDS = 150.0
MAX_LAG_SECONDS = 1.0
PROGRESS_LOG_INTERVAL_SECONDS = 10.0


class PublishError(RuntimeError):
    """Raised when an event cannot be handed over to the Kafka client."""


class KafkaClient(Protocol):
    """Subset of confluent_kafka.Producer used here, replaced by a fake in tests."""

    def produce(
        self,
        topic: str,
        *,
        key: str | None,
        value: bytes,
        on_delivery: Callable[..., None],
    ) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class ProducerMetrics:
    """Prometheus metrics of the producer. Values only come from real send results."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.events_sent = Counter(
            "rtap_producer_events_sent",
            "Events handed over to the Kafka client, by kind.",
            ["kind"],
            registry=registry,
        )
        self.events_delivered = Counter(
            "rtap_producer_events_delivered",
            "Events acknowledged by the Kafka broker.",
            ["topic"],
            registry=registry,
        )
        self.delivery_failures = Counter(
            "rtap_producer_delivery_failures",
            "Events that could not be delivered, by Kafka error.",
            ["error"],
            registry=registry,
        )
        self.buffer_full = Counter(
            "rtap_producer_buffer_full",
            "Times the local producer queue was full.",
            registry=registry,
        )
        self.delivery_latency = Histogram(
            "rtap_producer_delivery_latency_seconds",
            "Time between sending an event and its acknowledgement.",
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
            registry=registry,
        )
        self.target_rate = Gauge(
            "rtap_producer_target_events_per_second",
            "Configured event rate.",
            registry=registry,
        )


class EventPublisher:
    """Sends events to one topic and tracks what the broker acknowledged."""

    def __init__(self, client: KafkaClient, topic: str, metrics: ProducerMetrics) -> None:
        self._client = client
        self._topic = topic
        self._metrics = metrics
        self.sent = 0
        self.delivered = 0
        self.failed = 0

    def publish(self, event: GeneratedEvent) -> None:
        """Queue an event, waiting for room when the local buffer is full.

        Raises:
            PublishError: If the buffer stays full, which means Kafka is unreachable
                for longer than the delivery timeout, or if the client refuses the event.
        """
        for _ in range(MAX_BUFFER_RETRIES):
            try:
                self._client.produce(
                    self._topic,
                    key=event.key,
                    value=event.value,
                    on_delivery=self._on_delivery,
                )
            except BufferError:
                self._metrics.buffer_full.inc()
                self._client.poll(BUFFER_WAIT_SECONDS)
                continue
            except KafkaException as error:
                raise PublishError(f"Kafka client refused the event: {error}") from error
            self.sent += 1
            self._metrics.events_sent.labels(kind=event.kind).inc()
            self._client.poll(0)
            return
        raise PublishError("Local producer queue stayed full")

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.failed += 1
            self._metrics.delivery_failures.labels(error=error.name()).inc()
            logger.error(
                "Event not delivered",
                extra={"topic": message.topic(), "error": error.str(), "key": message.key()},
            )
            return
        self.delivered += 1
        self._metrics.events_delivered.labels(topic=message.topic()).inc()
        latency = message.latency()
        if latency is not None:
            self._metrics.delivery_latency.observe(latency)

    def close(self, timeout: float = FLUSH_TIMEOUT_SECONDS) -> int:
        """Wait for pending deliveries and return how many are still unconfirmed."""
        return self._client.flush(timeout)


def build_client_config(kafka: KafkaConfig, client_id: str) -> dict[str, Any]:
    """Translate config/app.yaml into librdkafka settings."""
    settings: Mapping[str, Any] = kafka.producer
    return {
        "bootstrap.servers": kafka.bootstrap_servers,
        "client.id": client_id,
        "acks": settings["acks"],
        "enable.idempotence": settings["enable_idempotence"],
        "linger.ms": settings["linger_ms"],
        "batch.size": settings["batch_size_bytes"],
        "compression.type": settings["compression_type"],
        "delivery.timeout.ms": settings["delivery_timeout_ms"],
        "retry.backoff.ms": settings["retry_backoff_ms"],
    }


def run(
    publisher: EventPublisher,
    generator: EventGenerator,
    events_per_second: float,
    stop: threading.Event,
    max_events: int | None = None,
) -> None:
    """Publish events until `stop` is set or `max_events` is reached."""
    interval = 1 / events_per_second
    next_send = time.monotonic()
    next_log = next_send + PROGRESS_LOG_INTERVAL_SECONDS
    produced = 0

    while not stop.is_set() and (max_events is None or produced < max_events):
        publisher.publish(generator.next_event())
        produced += 1

        now = time.monotonic()
        # After a stall, resume the normal rate instead of sending a burst.
        next_send = max(next_send + interval, now - MAX_LAG_SECONDS)
        if next_send > now:
            stop.wait(next_send - now)
        if now >= next_log:
            next_log = now + PROGRESS_LOG_INTERVAL_SECONDS
            logger.info(
                "Producer progress",
                extra={
                    "sent": publisher.sent,
                    "delivered": publisher.delivered,
                    "failed": publisher.failed,
                },
            )


def _parse_args(argv: list[str] | None, config: AppConfig) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish simulated e-commerce events to Kafka.")
    parser.add_argument(
        "--events-per-second", type=float, default=config.producer.events_per_second
    )
    parser.add_argument("--max-events", type=int, help="Stop after this many events.")
    parser.add_argument("--invalid-ratio", type=float, default=config.producer.invalid_ratio)
    parser.add_argument("--duplicate-ratio", type=float, default=config.producer.duplicate_ratio)
    parser.add_argument("--late-ratio", type=float, default=config.producer.late_ratio)
    parser.add_argument("--seed", type=int, help="Make the generated sequence reproducible.")
    parser.add_argument("--topic", default=config.kafka.topics.raw)
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=config.producer.metrics_port,
        help="Prometheus port. 0 disables the endpoint.",
    )
    args = parser.parse_args(argv)
    if args.events_per_second <= 0:
        parser.error("--events-per-second must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    """Command line entry point. Returns the process exit code."""
    config = load_config()
    args = _parse_args(argv, config)
    configure_logging(config.log_level)

    stop = threading.Event()
    fatal_error = threading.Event()

    def handle_signal(signum: int, _frame: object) -> None:
        logger.info("Stop requested", extra={"signal": signal.Signals(signum).name})
        stop.set()

    def handle_client_error(error: KafkaError) -> None:
        # Transient errors, such as a broker restart, are retried by the client.
        logger.warning("Kafka client error", extra={"error": error.str(), "fatal": error.fatal()})
        if error.fatal():
            fatal_error.set()
            stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    metrics = ProducerMetrics()
    metrics.target_rate.set(args.events_per_second)
    if args.metrics_port:
        start_http_server(args.metrics_port)

    client_config = build_client_config(config.kafka, config.name)
    client_config["error_cb"] = handle_client_error
    publisher = EventPublisher(Producer(client_config), args.topic, metrics)
    generator = EventGenerator(
        invalid_ratio=args.invalid_ratio,
        duplicate_ratio=args.duplicate_ratio,
        late_ratio=args.late_ratio,
        max_lateness_seconds=config.producer.max_lateness_seconds,
        seed=args.seed,
    )

    logger.info(
        "Producer started",
        extra={
            "topic": args.topic,
            "bootstrap_servers": config.kafka.bootstrap_servers,
            "events_per_second": args.events_per_second,
        },
    )
    publish_failed = False
    try:
        run(publisher, generator, args.events_per_second, stop, args.max_events)
    except PublishError:
        logger.exception("Publishing stopped")
        publish_failed = True

    undelivered = publisher.close()
    logger.info(
        "Producer stopped",
        extra={
            "sent": publisher.sent,
            "delivered": publisher.delivered,
            "failed": publisher.failed,
            "undelivered": undelivered,
        },
    )
    success = not (publish_failed or fatal_error.is_set() or publisher.failed or undelivered)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

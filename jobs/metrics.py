"""Prometheus metrics of the streaming application.

Engine metrics come from Spark's StreamingQueryProgress. Business metrics are
computed from each processed micro-batch. Counters restart at zero with the
application, which Prometheus handles with rate() and increase(). After a crash,
a replayed micro-batch can be counted twice: Delta tables stay the source of truth.
"""

from __future__ import annotations

import logging
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge
from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger("rtap.streaming")


class PipelineMetrics:
    """Every metric exposed by the Spark driver."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.input_rows = Counter(
            "rtap_streaming_input_rows",
            "Rows read by each streaming query.",
            ["query"],
            registry=registry,
        )
        self.input_rate = Gauge(
            "rtap_streaming_input_rows_per_second",
            "Input rate of the last micro-batch.",
            ["query"],
            registry=registry,
        )
        self.processed_rate = Gauge(
            "rtap_streaming_processed_rows_per_second",
            "Processing rate of the last micro-batch.",
            ["query"],
            registry=registry,
        )
        self.batch_duration = Gauge(
            "rtap_streaming_batch_duration_seconds",
            "Duration of the last micro-batch.",
            ["query"],
            registry=registry,
        )
        self.state_rows = Gauge(
            "rtap_streaming_state_rows",
            "Rows kept in the state store.",
            ["query"],
            registry=registry,
        )
        self.dropped_late_rows = Counter(
            "rtap_streaming_rows_dropped_by_watermark",
            "Rows dropped because they arrived after the watermark.",
            ["query"],
            registry=registry,
        )
        self.duplicates_dropped = Counter(
            "rtap_streaming_duplicates_dropped",
            "Rows removed by deduplication.",
            ["query"],
            registry=registry,
        )
        self.query_failures = Counter(
            "rtap_streaming_query_failures",
            "Streaming queries that stopped with an error.",
            ["query"],
            registry=registry,
        )
        self.active_queries = Gauge(
            "rtap_streaming_active_queries",
            "Streaming queries currently running.",
            registry=registry,
        )
        self.events_valid = Counter(
            "rtap_pipeline_events_valid",
            "Events accepted into Silver, by event type.",
            ["event_type"],
            registry=registry,
        )
        self.events_rejected = Counter(
            "rtap_pipeline_events_rejected",
            "Events rejected, once per rejection reason.",
            ["reason"],
            registry=registry,
        )
        self.events_quarantined = Counter(
            "rtap_pipeline_events_quarantined",
            "Events written to quarantine.",
            registry=registry,
        )
        self.transactions = Counter(
            "rtap_pipeline_transactions",
            "Purchase events accepted into Silver.",
            registry=registry,
        )
        self.revenue = Counter(
            "rtap_pipeline_revenue_euros",
            "Revenue of the purchase events accepted into Silver.",
            registry=registry,
        )
        self.latency = Gauge(
            "rtap_pipeline_latency_seconds",
            "Time from the Kafka append to the Silver commit in the last micro-batch.",
            ["quantile"],
            registry=registry,
        )


class MetricsListener(StreamingQueryListener):
    """Publishes the progress reported by Spark for every streaming query."""

    def __init__(self, metrics: PipelineMetrics) -> None:
        self._metrics = metrics
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802
        self._names[str(event.id)] = event.name
        self._metrics.active_queries.inc()
        logger.info("Streaming query started", extra={"query": event.name})

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        progress = event.progress
        query = progress.name
        metrics = self._metrics
        if progress.numInputRows:
            logger.info(
                "Micro-batch completed",
                extra={
                    "query": query,
                    "batch_id": progress.batchId,
                    "rows": progress.numInputRows,
                    "duration_ms": dict(progress.durationMs),
                },
            )
        metrics.input_rows.labels(query=query).inc(progress.numInputRows)
        metrics.input_rate.labels(query=query).set(progress.inputRowsPerSecond or 0)
        metrics.processed_rate.labels(query=query).set(progress.processedRowsPerSecond or 0)
        metrics.batch_duration.labels(query=query).set(progress.batchDuration / 1000)

        operators = progress.stateOperators or []
        metrics.state_rows.labels(query=query).set(sum(op.numRowsTotal for op in operators))
        for operator in operators:
            metrics.dropped_late_rows.labels(query=query).inc(operator.numRowsDroppedByWatermark)
            custom_metrics = operator.customMetrics or {}
            duplicates = custom_metrics.get("numDroppedDuplicateRows", 0)
            metrics.duplicates_dropped.labels(query=query).inc(duplicates)

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        """Nothing to record: no data was processed."""

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        query = self._names.pop(str(event.id), str(event.id))
        self._metrics.active_queries.dec()
        if event.exception:
            self._metrics.query_failures.labels(query=query).inc()
            logger.error("Streaming query failed", extra={"query": query, "error": event.exception})
        else:
            logger.info("Streaming query stopped", extra={"query": query})

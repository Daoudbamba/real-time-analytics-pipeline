"""Tests of the Bronze mapping, Delta helpers and metrics listener."""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from jobs.bronze.ingest import BRONZE_SCHEMA, to_bronze
from jobs.metrics import MetricsListener, PipelineMetrics
from jobs.session import append_idempotently, ensure_delta_table, storage_options
from rtap.config import StorageConfig

pytestmark = pytest.mark.spark


def test_bronze_keeps_the_message_and_its_origin(spark: Any) -> None:
    kafka = spark.createDataFrame(
        [(b"U1", b'{"raw": true}', "ecommerce-events", 2, 41, datetime(2026, 11, 1, 10))],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    bronze = to_bronze(kafka)
    row = bronze.first()

    assert bronze.schema.fieldNames() == BRONZE_SCHEMA.fieldNames()
    assert row["raw_value"] == '{"raw": true}'
    assert (row["kafka_key"], row["kafka_partition"], row["kafka_offset"]) == ("U1", 2, 41)
    assert row["ingestion_ts"] is not None


def test_ensure_delta_table_creates_it_only_once(spark: Any, tmp_path: Path) -> None:
    path = str(tmp_path / "table")

    ensure_delta_table(spark, path, BRONZE_SCHEMA, ["ingestion_date"])
    ensure_delta_table(spark, path, BRONZE_SCHEMA, ["ingestion_date"])

    assert spark.read.format("delta").load(path).count() == 0


def test_replayed_batch_is_written_only_once(spark: Any, tmp_path: Path) -> None:
    path = str(tmp_path / "table")
    batch = spark.createDataFrame([(1,), (2,)], "id int")

    append_idempotently(batch, path, "writer", 0)
    append_idempotently(batch, path, "writer", 0)
    append_idempotently(batch, path, "writer", 1)

    assert spark.read.format("delta").load(path).count() == 4


@pytest.mark.parametrize(
    ("backend", "expected_key"),
    [("s3", "spark.hadoop.fs.s3a.impl"), ("gcs", "spark.hadoop.fs.gs.impl")],
)
def test_cloud_options_hold_no_credentials(backend: str, expected_key: str) -> None:
    options = storage_options(StorageConfig(backend, "bucket"))

    assert expected_key in options
    assert not any("secret" in key.lower() or "access.key" in key.lower() for key in options)


def test_local_storage_needs_no_option() -> None:
    assert storage_options(StorageConfig("local", "/lakehouse")) == {}


def test_listener_publishes_query_progress() -> None:
    registry = CollectorRegistry()
    listener = MetricsListener(PipelineMetrics(registry))
    operator = SimpleNamespace(
        numRowsTotal=12,
        numRowsDroppedByWatermark=3,
        customMetrics={"numDroppedDuplicateRows": 2},
    )
    progress = SimpleNamespace(
        name="silver_events",
        numInputRows=100,
        inputRowsPerSecond=10.0,
        processedRowsPerSecond=20.0,
        batchDuration=1500,
        batchId=4,
        durationMs={"triggerExecution": 1500},
        stateOperators=[operator],
    )

    listener.onQueryProgress(SimpleNamespace(progress=progress))

    labels = {"query": "silver_events"}
    sample = registry.get_sample_value
    assert sample("rtap_streaming_input_rows_total", labels) == 100
    assert sample("rtap_streaming_batch_duration_seconds", labels) == 1.5
    assert sample("rtap_streaming_rows_dropped_by_watermark_total", labels) == 3
    assert sample("rtap_streaming_duplicates_dropped_total", labels) == 2


def test_listener_counts_failed_queries() -> None:
    registry = CollectorRegistry()
    listener = MetricsListener(PipelineMetrics(registry))

    listener.onQueryStarted(SimpleNamespace(id="1", name="bronze_events"))
    listener.onQueryTerminated(SimpleNamespace(id="1", exception="boom"))

    failures = registry.get_sample_value(
        "rtap_streaming_query_failures_total", {"query": "bronze_events"}
    )
    assert failures == 1
    assert registry.get_sample_value("rtap_streaming_active_queries") == 0

"""Streams Bronze into Silver and quarantine, and publishes the Kafka result topics.

Each output has its own query and checkpoint. Delta writes are idempotent per
micro-batch. Kafka writes are at-least-once: after a crash, a replayed micro-batch
can appear twice in the derived topics, while Delta keeps it once.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructField, StructType, TimestampType

from jobs.bronze.ingest import BRONZE_SCHEMA, BRONZE_TABLE
from jobs.metrics import PipelineMetrics
from jobs.session import append_idempotently, ensure_delta_table, single_row
from jobs.silver.validation import (
    TRANSACTION_TYPES,
    add_rejection_reasons,
    deduplicate,
    parse_events,
    select_invalid,
    select_valid,
)
from rtap.config import AppConfig

SILVER_TABLE = "events"
QUARANTINE_TABLE = "events"
VALID_QUERY = "silver_events"
INVALID_QUERY = "quarantine_events"


def _with_processing_time(batch: DataFrame) -> DataFrame:
    return batch.withColumn("processed_at", F.current_timestamp())


def _publish(batch: DataFrame, topic: str, key_column: str, bootstrap_servers: str) -> None:
    """Write each row of a micro-batch to a Kafka topic, as JSON."""
    (
        batch.select(
            F.col(key_column).cast("string").alias("key"),
            F.to_json(F.struct(*batch.columns)).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .save()
    )


def _record_valid_metrics(batch: DataFrame, metrics: PipelineMetrics) -> None:
    for row in batch.groupBy("event_type").count().collect():
        metrics.events_valid.labels(event_type=row["event_type"]).inc(row["count"])

    sales = batch.filter(F.col("event_type") == "purchase").agg(
        F.count("*").alias("transactions"),
        F.sum("amount").alias("revenue"),
    )
    totals = single_row(sales)
    metrics.transactions.inc(totals["transactions"])
    metrics.revenue.inc(float(totals["revenue"] or 0))

    latency_seconds = (F.unix_micros("processed_at") - F.unix_micros("kafka_timestamp")) / 1_000_000
    quantiles = single_row(
        batch.agg(F.percentile_approx(latency_seconds, [0.5, 0.95]).alias("values"))
    )["values"]
    metrics.latency.labels(quantile="0.5").set(quantiles[0])
    metrics.latency.labels(quantile="0.95").set(quantiles[1])


def _record_invalid_metrics(batch: DataFrame, metrics: PipelineMetrics) -> None:
    metrics.events_quarantined.inc(batch.count())
    reasons = batch.select(F.explode("rejection_reasons").alias("reason"))
    for row in reasons.groupBy("reason").count().collect():
        metrics.events_rejected.labels(reason=row["reason"]).inc(row["count"])


def _empty_validated_schema(spark: SparkSession) -> DataFrame:
    empty_bronze = spark.createDataFrame([], BRONZE_SCHEMA)
    return add_rejection_reasons(parse_events(empty_bronze))


def _silver_schema(spark: SparkSession) -> StructType:
    schema = select_valid(_empty_validated_schema(spark)).schema
    return StructType([*schema.fields, StructField("processed_at", TimestampType())])


def _quarantine_schema(spark: SparkSession) -> StructType:
    schema = select_invalid(_empty_validated_schema(spark)).schema
    return StructType([*schema.fields, StructField("processed_at", TimestampType())])


def _validated_stream(spark: SparkSession, config: AppConfig) -> DataFrame:
    bronze_path = config.storage.table_path("bronze", BRONZE_TABLE)
    bronze = spark.readStream.format("delta").load(bronze_path)
    return add_rejection_reasons(parse_events(bronze))


def start_silver_query(
    spark: SparkSession, config: AppConfig, metrics: PipelineMetrics
) -> StreamingQuery:
    """Start the stream of valid events: Silver, valid-events and transaction-events."""
    path = config.storage.table_path("silver", SILVER_TABLE)
    ensure_delta_table(spark, path, _silver_schema(spark), ["event_date"])
    topics = config.kafka.topics
    bootstrap_servers = config.kafka.bootstrap_servers

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        events = _with_processing_time(batch).persist()
        try:
            if events.isEmpty():
                return
            append_idempotently(events, path, VALID_QUERY, batch_id, ["event_date"])
            _publish(events, topics.valid, "user_id", bootstrap_servers)
            transactions = events.filter(F.col("event_type").isin(*TRANSACTION_TYPES))
            _publish(transactions, topics.transactions, "user_id", bootstrap_servers)
            _record_valid_metrics(events, metrics)
        finally:
            events.unpersist()

    valid_events = select_valid(_validated_stream(spark, config))
    return (
        deduplicate(valid_events, config.streaming.deduplication_watermark)
        .writeStream.queryName(VALID_QUERY)
        .foreachBatch(process_batch)
        .option("checkpointLocation", config.storage.checkpoint_path(VALID_QUERY))
        .trigger(processingTime=config.streaming.trigger_interval)
        .start()
    )


def start_quarantine_query(
    spark: SparkSession, config: AppConfig, metrics: PipelineMetrics
) -> StreamingQuery:
    """Start the stream of rejected events: quarantine and invalid-events."""
    path = config.storage.table_path("quarantine", QUARANTINE_TABLE)
    ensure_delta_table(spark, path, _quarantine_schema(spark), ["ingestion_date"])
    topic = config.kafka.topics.invalid
    bootstrap_servers = config.kafka.bootstrap_servers

    def process_batch(batch: DataFrame, batch_id: int) -> None:
        events = _with_processing_time(batch).persist()
        try:
            if events.isEmpty():
                return
            append_idempotently(events, path, INVALID_QUERY, batch_id, ["ingestion_date"])
            _publish(events, topic, "kafka_key", bootstrap_servers)
            _record_invalid_metrics(events, metrics)
        finally:
            events.unpersist()

    return (
        select_invalid(_validated_stream(spark, config))
        .writeStream.queryName(INVALID_QUERY)
        .foreachBatch(process_batch)
        .option("checkpointLocation", config.storage.checkpoint_path(INVALID_QUERY))
        .trigger(processingTime=config.streaming.trigger_interval)
        .start()
    )

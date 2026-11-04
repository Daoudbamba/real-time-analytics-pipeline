"""Streams Silver into the per-minute Gold tables.

The queries run in append mode: a window is written once, when the watermark passes
its end. Gold rows therefore appear a few minutes after the events.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from jobs.gold.aggregations import country_activity_per_window, sales_per_window
from jobs.session import ensure_delta_table
from jobs.silver.transform import SILVER_TABLE
from rtap.config import AppConfig

SALES_TABLE = "sales_per_minute"
COUNTRY_TABLE = "country_activity_per_minute"

Aggregation = Callable[[DataFrame, str], DataFrame]


def _start_aggregation(
    spark: SparkSession, config: AppConfig, table: str, aggregation: Aggregation
) -> StreamingQuery:
    silver_path = config.storage.table_path("silver", SILVER_TABLE)
    path = config.storage.table_path("gold", table)
    query_name = f"gold_{table}"
    window = config.streaming.aggregation_window

    silver_schema = spark.read.format("delta").load(silver_path).limit(0)
    ensure_delta_table(spark, path, aggregation(silver_schema, window).schema)

    events = (
        spark.readStream.format("delta")
        .load(silver_path)
        .withWatermark("event_ts", config.streaming.aggregation_watermark)
    )
    return (
        aggregation(events, window)
        .writeStream.queryName(query_name)
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.storage.checkpoint_path(query_name))
        .trigger(processingTime=config.streaming.trigger_interval)
        .start(path)
    )


def start_gold_queries(spark: SparkSession, config: AppConfig) -> list[StreamingQuery]:
    """Start one stream per Gold table."""
    return [
        _start_aggregation(spark, config, SALES_TABLE, sales_per_window),
        _start_aggregation(spark, config, COUNTRY_TABLE, country_activity_per_window),
    ]

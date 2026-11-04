"""Copies raw Kafka messages to the Bronze table, without interpreting them."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.session import ensure_delta_table
from rtap.config import AppConfig

BRONZE_TABLE = "events"
QUERY_NAME = "bronze_events"
BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("raw_value", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingestion_ts", TimestampType()),
        StructField("ingestion_date", DateType()),
    ]
)


def to_bronze(kafka_stream: DataFrame) -> DataFrame:
    """Keep the message as received, with its origin and ingestion time."""
    return kafka_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingestion_ts"),
        F.current_date().alias("ingestion_date"),
    )


def read_kafka(spark: SparkSession, config: AppConfig) -> DataFrame:
    """Open the stream of raw events."""
    consumer = config.kafka.consumer
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.topics.raw)
        .option("startingOffsets", consumer["starting_offsets"])
        .option("maxOffsetsPerTrigger", consumer["max_offsets_per_trigger"])
        .option("failOnDataLoss", str(consumer["fail_on_data_loss"]).lower())
        .load()
    )


def start_bronze_query(spark: SparkSession, config: AppConfig) -> StreamingQuery:
    """Start the Kafka to Bronze stream. The Delta sink makes it exactly-once."""
    path = config.storage.table_path("bronze", BRONZE_TABLE)
    ensure_delta_table(spark, path, BRONZE_SCHEMA, ["ingestion_date"])
    return (
        to_bronze(read_kafka(spark, config))
        .writeStream.queryName(QUERY_NAME)
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.storage.checkpoint_path(QUERY_NAME))
        .partitionBy("ingestion_date")
        .trigger(processingTime=config.streaming.trigger_interval)
        .start(path)
    )

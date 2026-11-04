"""Spark session and Delta helpers shared by the streaming and batch jobs."""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType

from rtap.config import StorageConfig


def storage_options(storage: StorageConfig) -> dict[str, str]:
    """Return the Hadoop settings of the storage backend.

    Credentials are never set here: S3 reads the standard AWS environment
    variables and GCS reads GOOGLE_APPLICATION_CREDENTIALS.
    """
    if storage.backend == "s3":
        return {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
            ),
        }
    if storage.backend == "gcs":
        return {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": (
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            ),
            "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
        }
    return {}


def build_spark(app_name: str, storage: StorageConfig) -> SparkSession:
    """Create the session. Delta and sizing settings come from spark-defaults.conf."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in storage_options(storage).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def single_row(frame: DataFrame) -> Row:
    """Return the row of a global aggregation, which always has exactly one."""
    row = frame.first()
    if row is None:
        raise ValueError("expected one row, got none")
    return row


def ensure_delta_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    partition_by: Sequence[str] = (),
) -> None:
    """Create an empty table if needed, as a Delta stream cannot start without one."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    writer = spark.createDataFrame([], schema).write.format("delta").mode("ignore")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def append_idempotently(
    batch: DataFrame,
    path: str,
    writer_id: str,
    batch_id: int,
    partition_by: Sequence[str] = (),
) -> None:
    """Append a micro-batch. Delta skips it if this writer already committed that batch."""
    writer = (
        batch.write.format("delta")
        .mode("append")
        .option("txnAppId", writer_id)
        .option("txnVersion", batch_id)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

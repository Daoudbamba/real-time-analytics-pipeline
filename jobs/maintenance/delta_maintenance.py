"""Compacts small files and removes obsolete ones.

Micro-batches create many small files, which slow every read. Both operations are
safe while the streams run: compaction does not change the data, so Delta streams
ignore it and appends do not conflict with it. VACUUM keeps the files of the retention
period, which must stay longer than the longest stream interruption.

Run it with `spark-submit jobs/maintenance/delta_maintenance.py --action optimize`.
"""

from __future__ import annotations

import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from jobs.bronze.ingest import BRONZE_TABLE
from jobs.gold.streaming import COUNTRY_TABLE, SALES_TABLE
from jobs.quality.checks import RESULTS_TABLE
from jobs.session import build_spark, single_row
from jobs.silver.transform import QUARANTINE_TABLE, SILVER_TABLE
from rtap.config import AppConfig, load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.maintenance")

ACTIONS = ("optimize", "vacuum")
TABLES = (
    ("bronze", BRONZE_TABLE),
    ("silver", SILVER_TABLE),
    ("quarantine", QUARANTINE_TABLE),
    ("gold", SALES_TABLE),
    ("gold", COUNTRY_TABLE),
    ("gold", RESULTS_TABLE),
)


def optimize_table(spark: SparkSession, path: str) -> dict[str, int]:
    """Merge the small files of a table and return what changed."""
    metrics = single_row(DeltaTable.forPath(spark, path).optimize().executeCompaction())["metrics"]
    return {"files_added": metrics["numFilesAdded"], "files_removed": metrics["numFilesRemoved"]}


def vacuum_table(spark: SparkSession, path: str, retention_hours: int) -> None:
    """Delete the files no longer referenced after the retention period."""
    DeltaTable.forPath(spark, path).vacuum(retention_hours)


def run(spark: SparkSession, config: AppConfig, action: str) -> None:
    """Apply an action to every existing table."""
    for layer, table in TABLES:
        path = config.storage.table_path(layer, table)
        if not DeltaTable.isDeltaTable(spark, path):
            logger.warning("Table not created yet", extra={"table": f"{layer}/{table}"})
            continue
        details: dict[str, int] = {}
        if action == "optimize":
            details = optimize_table(spark, path)
        else:
            vacuum_table(spark, path, config.vacuum_retention_hours)
        logger.info(
            "Maintenance done", extra={"action": action, "table": f"{layer}/{table}", **details}
        )


def main(argv: list[str] | None = None) -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description="Maintain the Delta tables.")
    parser.add_argument("--action", choices=ACTIONS, required=True)
    args = parser.parse_args(argv)

    config = load_config()
    configure_logging(config.log_level)
    spark = build_spark(f"maintenance-{args.action}", config.storage)
    run(spark, config, args.action)
    spark.stop()


if __name__ == "__main__":
    main()

"""Rebuilds the Gold tables that do not fit a streaming window.

Run by Airflow. Each run replaces the tables, so a retry is always safe.
"""

from __future__ import annotations

import argparse
import logging

from jobs.gold.aggregations import sales_trend, top_products
from jobs.session import build_spark
from jobs.silver.transform import SILVER_TABLE
from rtap.config import load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.gold.batch")

TOP_PRODUCTS_TABLE = "top_products_daily"
SALES_TREND_TABLE = "sales_trend_hourly"


def main(argv: list[str] | None = None) -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description="Rebuild the batch Gold tables.")
    parser.add_argument("--top-limit", type=int, default=10)
    args = parser.parse_args(argv)

    config = load_config()
    configure_logging(config.log_level)
    spark = build_spark("gold-batch-aggregates", config.storage)

    silver = spark.read.format("delta").load(config.storage.table_path("silver", SILVER_TABLE))
    outputs = {
        TOP_PRODUCTS_TABLE: top_products(silver, args.top_limit),
        SALES_TREND_TABLE: sales_trend(silver),
    }
    for table, data in outputs.items():
        path = config.storage.table_path("gold", table)
        data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        logger.info("Gold table rebuilt", extra={"table": table})
    spark.stop()


if __name__ == "__main__":
    main()

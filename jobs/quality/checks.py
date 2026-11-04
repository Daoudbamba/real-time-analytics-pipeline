"""Controls the Bronze, Silver and Gold tables.

Every check returns a result, also stored in gold/data_quality_results. The job exits
with code 1 if a check fails, so that Airflow marks the task as failed.

Run it with `spark-submit jobs/quality/checks.py --layer silver`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.bronze.ingest import BRONZE_TABLE
from jobs.gold.streaming import COUNTRY_TABLE, SALES_TABLE
from jobs.session import build_spark, single_row
from jobs.silver.transform import QUARANTINE_TABLE, SILVER_TABLE
from jobs.silver.validation import TRANSACTION_TYPES
from rtap.config import AppConfig, load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.quality")

RESULTS_TABLE = "data_quality_results"
LAYERS = ("bronze", "silver", "gold")
DEFAULT_LOOKBACK_HOURS = 24
ROUNDING_TOLERANCE = 0.01
REQUIRED_SILVER_COLUMNS = (
    "event_id",
    "user_id",
    "product_id",
    "category",
    "amount",
    "quantity",
    "event_type",
    "event_ts",
    "country",
)


@dataclass(frozen=True)
class CheckResult:
    layer: str
    check: str
    passed: bool
    observed: str
    expected: str


def _count_check(layer: str, check: str, bad_rows: int) -> CheckResult:
    return CheckResult(layer, check, bad_rows == 0, f"{bad_rows} rows", "0 rows")


def _not_empty(layer: str, table: DataFrame) -> CheckResult:
    rows = table.count()
    return CheckResult(layer, "not_empty", rows > 0, f"{rows} rows", "at least 1 row")


def _duplicates(table: DataFrame, *columns: str) -> int:
    return table.groupBy(*columns).count().filter(F.col("count") > 1).count()


def check_bronze(bronze: DataFrame) -> list[CheckResult]:
    """Bronze must hold each Kafka message once, with its content."""
    return [
        _not_empty("bronze", bronze),
        _count_check(
            "bronze", "unique_kafka_offsets", _duplicates(bronze, "kafka_partition", "kafka_offset")
        ),
        _count_check(
            "bronze",
            "ingestion_time_present",
            bronze.filter(F.col("ingestion_ts").isNull()).count(),
        ),
    ]


def check_silver(
    silver: DataFrame,
    quarantine: DataFrame,
    bronze: DataFrame,
    max_invalid_ratio: float,
) -> list[CheckResult]:
    """Silver must hold clean, unique events, and no event may appear from nowhere."""
    missing_values = F.lit(False)
    for column in REQUIRED_SILVER_COLUMNS:
        missing_values = missing_values | F.col(column).isNull()
    is_transaction = F.col("event_type").isin(*TRANSACTION_TYPES)
    bad_transactions = is_transaction & (F.col("payment_method").isNull() | (F.col("amount") <= 0))

    valid, invalid, raw = silver.count(), quarantine.count(), bronze.count()
    invalid_ratio = invalid / (valid + invalid) if valid + invalid else 0.0
    return [
        _not_empty("silver", silver),
        _count_check("silver", "unique_event_ids", _duplicates(silver, "event_id")),
        _count_check("silver", "no_missing_value", silver.filter(missing_values).count()),
        _count_check("silver", "amount_not_negative", silver.filter(F.col("amount") < 0).count()),
        _count_check("silver", "quantity_positive", silver.filter(F.col("quantity") < 1).count()),
        _count_check("silver", "transactions_complete", silver.filter(bad_transactions).count()),
        CheckResult(
            "silver",
            "invalid_ratio",
            invalid_ratio <= max_invalid_ratio,
            f"{invalid_ratio:.4f}",
            f"at most {max_invalid_ratio}",
        ),
        CheckResult(
            "silver",
            "no_unknown_origin",
            valid + invalid <= raw,
            f"{valid + invalid} silver and quarantine rows",
            f"at most {raw} bronze rows",
        ),
    ]


def check_gold(
    sales: DataFrame,
    countries: DataFrame,
    now: datetime,
    max_staleness_minutes: int,
) -> list[CheckResult]:
    """Gold must be recent, and its figures consistent."""
    gap = F.abs(F.col("revenue") / F.col("transactions") - F.col("average_basket"))
    latest = single_row(sales.agg(F.max("window_end")))[0]
    limit = now - timedelta(minutes=max_staleness_minutes)
    return [
        _not_empty("gold", sales),
        CheckResult(
            "gold",
            "fresh_data",
            latest is not None and latest >= limit,
            str(latest),
            f"after {limit:%Y-%m-%d %H:%M:%S}",
        ),
        _count_check("gold", "unique_windows", _duplicates(sales, "window_start")),
        _count_check(
            "gold",
            "unique_country_windows",
            _duplicates(countries, "window_start", "country"),
        ),
        _count_check(
            "gold",
            "revenue_not_negative",
            sales.filter(F.col("revenue") < 0).count()
            + countries.filter(F.col("revenue") < 0).count(),
        ),
        _count_check(
            "gold",
            "average_basket_consistent",
            sales.filter(gap > ROUNDING_TOLERANCE).count(),
        ),
    ]


def _read(spark: SparkSession, config: AppConfig, layer: str, table: str) -> DataFrame:
    return spark.read.format("delta").load(config.storage.table_path(layer, table))


def run_checks(
    spark: SparkSession,
    config: AppConfig,
    layer: str,
    lookback_hours: int,
) -> list[CheckResult]:
    """Run the checks of a layer on its recent rows."""
    now = datetime.now(UTC).replace(tzinfo=None)
    start = now - timedelta(hours=lookback_hours)
    bronze = _read(spark, config, "bronze", BRONZE_TABLE).filter(F.col("ingestion_ts") >= start)
    if layer == "bronze":
        return check_bronze(bronze)
    if layer == "silver":
        return check_silver(
            _read(spark, config, "silver", SILVER_TABLE).filter(F.col("ingestion_ts") >= start),
            _read(spark, config, "quarantine", QUARANTINE_TABLE).filter(
                F.col("ingestion_ts") >= start
            ),
            bronze,
            config.quality.max_invalid_ratio,
        )
    return check_gold(
        _read(spark, config, "gold", SALES_TABLE),
        _read(spark, config, "gold", COUNTRY_TABLE),
        now,
        config.quality.max_gold_staleness_minutes,
    )


def store_results(
    spark: SparkSession, config: AppConfig, results: list[CheckResult], checked_at: datetime
) -> None:
    """Append the results, to keep the history of the controls."""
    rows = [
        (checked_at, item.layer, item.check, item.passed, item.observed, item.expected)
        for item in results
    ]
    schema = (
        "checked_at timestamp, layer string, check string, passed boolean, "
        "observed string, expected string"
    )
    (
        spark.createDataFrame(rows, schema)
        .write.format("delta")
        .mode("append")
        .save(config.storage.table_path("gold", RESULTS_TABLE))
    )


def main(argv: list[str] | None = None) -> int:
    """Command line entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(description="Control a layer of the lakehouse.")
    parser.add_argument("--layer", choices=LAYERS, required=True)
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    args = parser.parse_args(argv)

    config = load_config()
    configure_logging(config.log_level)
    spark = build_spark(f"quality-{args.layer}", config.storage)

    results = run_checks(spark, config, args.layer, args.lookback_hours)
    store_results(spark, config, results, datetime.now(UTC).replace(tzinfo=None))
    for result in results:
        level = logging.INFO if result.passed else logging.ERROR
        logger.log(level, "Quality check", extra=vars(result))
    spark.stop()

    failed = [result.check for result in results if not result.passed]
    if failed:
        logger.error("Quality checks failed", extra={"layer": args.layer, "failed": failed})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

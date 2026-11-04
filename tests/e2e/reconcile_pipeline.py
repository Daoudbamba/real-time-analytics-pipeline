"""Reconciles the Delta tables with the events a seeded producer run really sent.

The generator is reproducible: with the same seed and ratios, it draws the same sequence
of valid, invalid, duplicate and late events, whatever the time. Replaying it without
Kafka gives the exact content each layer must have. Run by scripts/verify_pipeline.sh
inside the Spark image, on a lakehouse that only holds that run.

Two parts: `streaming` reconciles the layers right after the seeded run, `batch` checks
what the Airflow DAGs wrote afterwards, while events flow again.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.bronze.ingest import BRONZE_TABLE
from jobs.gold.batch_aggregates import SALES_TREND_TABLE, TOP_PRODUCTS_TABLE
from jobs.gold.streaming import COUNTRY_TABLE, SALES_TABLE
from jobs.quality.checks import LAYERS as QUALITY_LAYERS
from jobs.quality.checks import RESULTS_TABLE
from jobs.session import build_spark, single_row
from jobs.silver.transform import QUARANTINE_TABLE, SILVER_TABLE
from producers.event_generator import EventGenerator
from rtap.config import AppConfig, load_config

# A purchase can only miss Gold if it arrived behind the 2-minute aggregation watermark.
# Measured lateness also includes up to one trigger interval, hence the lower bound.
LATE_FOR_GOLD_SECONDS = 60


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def expected_kinds(config: AppConfig, events: int, seed: int) -> Counter[str]:
    """Replay the generator of the producer run without sending anything."""
    generator = EventGenerator(
        invalid_ratio=config.producer.invalid_ratio,
        duplicate_ratio=config.producer.duplicate_ratio,
        late_ratio=config.producer.late_ratio,
        max_lateness_seconds=config.producer.max_lateness_seconds,
        seed=seed,
    )
    return Counter(generator.next_event().kind for _ in range(events))


def equal(name: str, observed: int, expected: int) -> Check:
    return Check(name, observed == expected, f"observed={observed} expected={expected}")


def reconcile_layers(
    bronze: DataFrame,
    silver: DataFrame,
    quarantine: DataFrame,
    kinds: Counter[str],
    kafka_offsets: dict[str, int],
) -> list[Check]:
    """Compare the streaming layers with the replayed run and the Kafka offsets."""
    events = sum(kinds.values())
    valid = kinds["valid"] + kinds["late"]
    bronze_rows = bronze.count()
    silver_rows = silver.count()
    quarantine_rows = quarantine.count()
    return [
        equal("kafka_received_every_event", kafka_offsets["raw"], events),
        equal("bronze_matches_kafka", bronze_rows, kafka_offsets["raw"]),
        equal(
            "bronze_exactly_once",
            bronze.select("kafka_partition", "kafka_offset").distinct().count(),
            bronze_rows,
        ),
        equal("silver_holds_valid_and_late_events", silver_rows, valid),
        equal("silver_event_ids_unique", silver.select("event_id").distinct().count(), silver_rows),
        equal("quarantine_holds_invalid_events", quarantine_rows, kinds["invalid"]),
        equal(
            "bronze_equals_silver_quarantine_and_duplicates",
            bronze_rows,
            silver_rows + quarantine_rows + kinds["duplicate"],
        ),
        # The derived topics are written at least once: a replayed micro-batch adds copies.
        Check(
            "valid_topic_has_every_silver_event",
            kafka_offsets["valid"] >= silver_rows,
            f"observed={kafka_offsets['valid']} expected>={silver_rows}",
        ),
        Check(
            "invalid_topic_has_every_rejected_event",
            kafka_offsets["invalid"] >= quarantine_rows,
            f"observed={kafka_offsets['invalid']} expected>={quarantine_rows}",
        ),
    ]


def reconcile_gold(silver: DataFrame, sales: DataFrame, countries: DataFrame) -> list[Check]:
    """Check the per-minute aggregates against Silver, for the windows already closed."""
    windows = sales.count()
    checks = [
        Check("gold_has_closed_windows", windows > 0, f"windows={windows}"),
        equal(
            "gold_sales_windows_unique",
            sales.select("window_start").distinct().count(),
            windows,
        ),
        equal(
            "gold_country_windows_unique",
            countries.select("window_start", "country").distinct().count(),
            countries.count(),
        ),
    ]
    if windows == 0:
        return checks

    last_end = single_row(sales.agg(F.max("window_end")))[0]
    purchases = silver.filter((F.col("event_type") == "purchase") & (F.col("event_ts") < last_end))
    silver_total = single_row(purchases.agg(F.count("*"), F.sum("amount")))
    gold_total = single_row(sales.agg(F.sum("transactions"), F.sum("revenue")))
    late_purchases = purchases.filter(
        F.unix_timestamp("kafka_timestamp") - F.unix_timestamp("event_ts") > LATE_FOR_GOLD_SECONDS
    ).count()
    missing = silver_total[0] - (gold_total[0] or 0)
    checks.append(
        Check(
            "gold_transactions_match_silver",
            0 <= missing <= late_purchases,
            f"silver={silver_total[0]} gold={gold_total[0]} missing={missing} "
            f"late_purchases={late_purchases}",
        )
    )
    checks.append(
        Check(
            "gold_revenue_within_silver",
            (gold_total[1] or 0) <= silver_total[1],
            f"silver={silver_total[1]} gold={gold_total[1]}",
        )
    )
    return checks


def reconcile_batch(spark: SparkSession, config: AppConfig) -> list[Check]:
    """Check what the Airflow DAGs wrote: batch Gold tables and quality results."""

    def read(table: str) -> DataFrame:
        return spark.read.format("delta").load(config.storage.table_path("gold", table))

    checks = [
        Check(f"batch_{table}_not_empty", (rows := read(table).count()) > 0, f"rows={rows}")
        for table in (TOP_PRODUCTS_TABLE, SALES_TREND_TABLE)
    ]
    # Each layer is controlled by its own task: take the latest control of every layer.
    results = read(RESULTS_TABLE)
    latest = results.groupBy("layer").agg(F.max("checked_at").alias("checked_at"))
    controlled = sorted(row.layer for row in latest.collect())
    failed = sorted(
        f"{row.layer}.{row.check}"
        for row in results.join(latest, ["layer", "checked_at"]).filter(~F.col("passed")).collect()
    )
    checks.append(
        Check(
            "latest_quality_controls_passed",
            controlled == sorted(QUALITY_LAYERS) and not failed,
            f"layers={controlled} failed={failed or 'none'}",
        )
    )
    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("part", choices=("streaming", "batch"))
    parser.add_argument("--events", type=int)
    parser.add_argument("--seed", type=int)
    for topic in ("raw", "valid", "invalid"):
        parser.add_argument(f"--{topic}-offsets", type=int)
    args = parser.parse_args(argv)
    offsets = {
        "raw": args.raw_offsets,
        "valid": args.valid_offsets,
        "invalid": args.invalid_offsets,
    }
    if args.part == "streaming" and None in (args.events, args.seed, *offsets.values()):
        parser.error("streaming needs --events, --seed and the three --*-offsets")

    config = load_config()
    spark = build_spark("e2e-reconciliation", config.storage)
    spark.sparkContext.setLogLevel("ERROR")

    def read(layer: str, table: str) -> DataFrame:
        return spark.read.format("delta").load(config.storage.table_path(layer, table))

    if args.part == "batch":
        checks = reconcile_batch(spark, config)
    else:
        silver = read("silver", SILVER_TABLE)
        checks = reconcile_layers(
            read("bronze", BRONZE_TABLE),
            silver,
            read("quarantine", QUARANTINE_TABLE),
            expected_kinds(config, args.events, args.seed),
            {topic: count for topic, count in offsets.items() if count is not None},
        )
        checks += reconcile_gold(silver, read("gold", SALES_TABLE), read("gold", COUNTRY_TABLE))
    spark.stop()

    for check in checks:
        print(f"CHECK {'PASS' if check.passed else 'FAIL'} {check.name}: {check.detail}")
    return 0 if all(check.passed for check in checks) else 1


if __name__ == "__main__":
    sys.exit(main())

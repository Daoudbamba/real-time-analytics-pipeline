"""Tests of the data quality checks and the Delta maintenance."""

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from jobs.bronze.ingest import BRONZE_SCHEMA
from jobs.maintenance.delta_maintenance import optimize_table, vacuum_table
from jobs.quality.checks import check_bronze, check_gold, check_silver

pytestmark = pytest.mark.spark

NOW = datetime(2026, 11, 1, 12, 0)
SILVER_SCHEMA = (
    "event_id string, user_id string, product_id string, category string, "
    "amount decimal(12,2), quantity int, event_type string, event_ts timestamp, "
    "payment_method string, country string"
)
SALES_SCHEMA = (
    "window_start timestamp, window_end timestamp, revenue decimal(22,2), "
    "transactions long, units_sold long, average_basket decimal(24,2)"
)
COUNTRY_SCHEMA = "window_start timestamp, country string, revenue decimal(22,2)"


def by_name(results: list[Any]) -> dict[str, Any]:
    return {result.check: result for result in results}


def bronze_rows(spark: Any, offsets: list[int]) -> Any:
    rows = [("k", "{}", "t", 0, offset, NOW, NOW, NOW.date()) for offset in offsets]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def silver_row(event_id: str = "e1", **changes: Any) -> tuple[Any, ...]:
    row = {
        "event_id": event_id,
        "user_id": "U1",
        "product_id": "P1",
        "category": "books",
        "amount": Decimal("10.00"),
        "quantity": 1,
        "event_type": "purchase",
        "event_ts": NOW,
        "payment_method": "card",
        "country": "FR",
        **changes,
    }
    return tuple(row.values())


def run_silver(spark: Any, rows: list[Any], invalid: int = 0, ratio: float = 0.2) -> dict[str, Any]:
    silver = spark.createDataFrame(rows, SILVER_SCHEMA)
    quarantine = spark.createDataFrame([(index,) for index in range(invalid)], "id int")
    bronze = bronze_rows(spark, list(range(len(rows) + invalid)))
    return by_name(check_silver(silver, quarantine, bronze, ratio))


def test_clean_bronze_passes(spark: Any) -> None:
    assert all(result.passed for result in check_bronze(bronze_rows(spark, [1, 2, 3])))


def test_bronze_detects_a_replayed_offset(spark: Any) -> None:
    results = by_name(check_bronze(bronze_rows(spark, [1, 1, 2])))

    assert not results["unique_kafka_offsets"].passed


def test_empty_table_fails(spark: Any) -> None:
    assert not by_name(check_bronze(bronze_rows(spark, [])))["not_empty"].passed


def test_clean_silver_passes(spark: Any) -> None:
    results = run_silver(spark, [silver_row("e1"), silver_row("e2")])

    assert all(result.passed for result in results.values())


@pytest.mark.parametrize(
    ("rows", "check"),
    [
        ([silver_row("e1"), silver_row("e1")], "unique_event_ids"),
        ([silver_row(amount=Decimal("-1.00"))], "amount_not_negative"),
        ([silver_row(quantity=0)], "quantity_positive"),
        ([silver_row(user_id=None)], "no_missing_value"),
        ([silver_row(payment_method=None)], "transactions_complete"),
        ([silver_row(amount=Decimal("0.00"))], "transactions_complete"),
    ],
)
def test_silver_detects_each_anomaly(spark: Any, rows: list[Any], check: str) -> None:
    assert not run_silver(spark, rows)[check].passed


def test_too_many_rejected_events_fail(spark: Any) -> None:
    assert not run_silver(spark, [silver_row()], invalid=1, ratio=0.2)["invalid_ratio"].passed


def test_rows_without_bronze_origin_fail(spark: Any) -> None:
    silver = spark.createDataFrame([silver_row("e1"), silver_row("e2")], SILVER_SCHEMA)
    quarantine = spark.createDataFrame([], "id int")

    results = by_name(check_silver(silver, quarantine, bronze_rows(spark, [1]), 0.2))

    assert not results["no_unknown_origin"].passed


def gold_results(spark: Any, sales: list[Any], countries: list[Any] | None = None) -> Any:
    return by_name(
        check_gold(
            spark.createDataFrame(sales, SALES_SCHEMA),
            spark.createDataFrame(countries or [], COUNTRY_SCHEMA),
            NOW,
            15,
        )
    )


def window(minutes_ago: int, revenue: str = "30.00", basket: str = "15.00") -> tuple[Any, ...]:
    end = NOW - timedelta(minutes=minutes_ago)
    return (end - timedelta(minutes=1), end, Decimal(revenue), 2, 2, Decimal(basket))


def test_clean_gold_passes(spark: Any) -> None:
    assert all(result.passed for result in gold_results(spark, [window(5)]).values())


def test_stale_gold_fails(spark: Any) -> None:
    assert not gold_results(spark, [window(30)])["fresh_data"].passed


def test_duplicate_window_fails(spark: Any) -> None:
    assert not gold_results(spark, [window(5), window(5)])["unique_windows"].passed


def test_negative_revenue_fails(spark: Any) -> None:
    results = gold_results(spark, [window(5, "-30.00", "-15.00")])

    assert not results["revenue_not_negative"].passed


def test_inconsistent_basket_fails(spark: Any) -> None:
    results = gold_results(spark, [window(5, basket="99.00")])

    assert not results["average_basket_consistent"].passed


def test_optimize_merges_small_files_without_changing_rows(spark: Any, tmp_path: Path) -> None:
    path = str(tmp_path / "table")
    for batch in range(4):
        spark.createDataFrame([(batch,)], "id int").write.format("delta").mode("append").save(path)

    result = optimize_table(spark, path)
    vacuum_table(spark, path, 168)

    assert result == {"files_added": 1, "files_removed": 4}
    assert spark.read.format("delta").load(path).count() == 4

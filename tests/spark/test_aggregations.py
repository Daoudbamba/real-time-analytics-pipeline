"""Tests of the Gold aggregates, with hand-computed expected values."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from jobs.gold.aggregations import (
    country_activity_per_window,
    sales_per_window,
    sales_trend,
    top_products,
)

pytestmark = pytest.mark.spark

SCHEMA = (
    "event_ts timestamp, event_date date, event_type string, amount decimal(12,2), "
    "quantity int, country string, user_id string, product_id string, category string"
)


def event(
    minute: int,
    event_type: str,
    amount: str,
    quantity: int = 1,
    country: str = "FR",
    user: str = "U1",
    product: str = "P1",
    hour: int = 10,
) -> tuple[Any, ...]:
    timestamp = datetime(2026, 11, 1, hour, minute, 30)
    return (
        timestamp,
        date(2026, 11, 1),
        event_type,
        Decimal(amount),
        quantity,
        country,
        user,
        product,
        "books",
    )


@pytest.fixture
def silver(spark: Any) -> Any:
    rows = [
        event(0, "purchase", "10.00", 1),
        event(0, "purchase", "20.00", 3, country="SN", user="U2"),
        event(0, "payment", "30.00"),
        event(0, "page_view", "0.00"),
        event(1, "purchase", "5.00", 2),
    ]
    return spark.createDataFrame(rows, SCHEMA)


def test_sales_per_minute(silver: Any) -> None:
    rows = sales_per_window(silver, "1 minute").orderBy("window_start").collect()

    assert len(rows) == 2
    first = rows[0]
    assert first["window_start"] == datetime(2026, 11, 1, 10, 0)
    assert first["window_end"] == datetime(2026, 11, 1, 10, 1)
    assert first["revenue"] == Decimal("30.00")
    assert first["transactions"] == 2
    assert first["units_sold"] == 4
    assert float(first["average_basket"]) == 15.0
    assert rows[1]["revenue"] == Decimal("5.00")


def test_payment_is_not_added_to_revenue(silver: Any) -> None:
    total = sum(row["revenue"] for row in sales_per_window(silver, "1 minute").collect())

    assert total == Decimal("35.00")


def test_country_activity(silver: Any) -> None:
    rows = country_activity_per_window(silver, "1 minute").collect()
    france = next(
        row
        for row in rows
        if row["country"] == "FR" and row["window_start"] == datetime(2026, 11, 1, 10, 0)
    )
    senegal = next(row for row in rows if row["country"] == "SN")

    assert (france["events"], france["transactions"]) == (3, 1)
    assert france["revenue"] == Decimal("10.00")
    assert france["active_users"] == 1
    assert senegal["revenue"] == Decimal("20.00")


def test_country_without_purchase_has_zero_revenue(spark: Any) -> None:
    silver = spark.createDataFrame([event(0, "page_view", "0.00", country="BE")], SCHEMA)

    row = country_activity_per_window(silver, "1 minute").first()

    assert (row["transactions"], row["revenue"]) == (0, Decimal("0.00"))


def test_top_products_are_ranked_by_revenue_per_day(spark: Any) -> None:
    rows = [
        event(0, "purchase", "10.00", product="P1"),
        event(1, "purchase", "30.00", product="P2"),
        event(2, "purchase", "20.00", product="P3"),
        event(3, "page_view", "0.00", product="P4"),
    ]

    result = top_products(spark.createDataFrame(rows, SCHEMA), limit=2).orderBy("rank").collect()

    assert [(row["product_id"], row["rank"]) for row in result] == [("P2", 1), ("P3", 2)]


def test_sales_trend_compares_consecutive_hours(spark: Any) -> None:
    rows = [
        event(0, "purchase", "100.00", hour=10),
        event(0, "purchase", "150.00", hour=11),
        event(0, "purchase", "75.00", hour=12),
    ]

    result = sales_trend(spark.createDataFrame(rows, SCHEMA)).collect()

    assert [row["revenue_change_pct"] for row in result][0] is None
    assert [float(row["revenue_change_pct"]) for row in result[1:]] == [50.0, -50.0]

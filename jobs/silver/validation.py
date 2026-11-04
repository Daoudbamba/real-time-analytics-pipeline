"""Validates raw events against schemas/event_schema.json.

The rules read their patterns and allowed values from the JSON Schema, so that the
producers and Spark share one contract. Fields are parsed as text first: a bad value
then gives a precise rejection reason instead of silently becoming null.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "schemas" / "event_schema.json"
CONTRACT: dict[str, Any] = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
REQUIRED_FIELDS: list[str] = CONTRACT["required"]
PROPERTIES: dict[str, Any] = CONTRACT["properties"]

CORRUPT_COLUMN = "_corrupt_record"
RAW_EVENT_SCHEMA = StructType(
    [StructField(name, StringType()) for name in REQUIRED_FIELDS]
    + [StructField(CORRUPT_COLUMN, StringType())]
)
TRANSACTION_TYPES = ("purchase", "payment")
MAX_CLOCK_SKEW = "INTERVAL 5 MINUTES"


def _amount() -> Column:
    return F.col("amount").try_cast("decimal(12,2)")


def _quantity() -> Column:
    return F.col("quantity").try_cast("int")


def _event_ts() -> Column:
    return F.col("timestamp").try_cast("timestamp")


def parse_events(bronze: DataFrame) -> DataFrame:
    """Add the JSON fields as trimmed text columns, next to the Bronze columns."""
    parsed = F.from_json(
        F.col("raw_value"),
        RAW_EVENT_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COLUMN},
    )
    return (
        bronze.withColumn("_parsed", parsed)
        .withColumn("_keys", F.json_object_keys("raw_value"))
        .select(
            *bronze.columns,
            "_keys",
            F.col(f"_parsed.{CORRUPT_COLUMN}").alias(CORRUPT_COLUMN),
            *[F.trim(F.col(f"_parsed.{name}")).alias(name) for name in REQUIRED_FIELDS],
        )
    )


def _is_quoted(field: str) -> Column:
    """Tell whether a numeric field was sent as a JSON string, such as "12.5"."""
    return F.col("raw_value").rlike(rf'"{field}"\s*:\s*"')


def _not_matching(field: str) -> Column:
    return ~F.col(field).rlike(PROPERTIES[field]["pattern"])


def _not_allowed(field: str) -> Column:
    allowed = [value for value in PROPERTIES[field]["enum"] if value is not None]
    return ~F.col(field).isin(allowed)


def _rules() -> list[tuple[str, Column]]:
    """Return each rejection reason with the condition that triggers it."""
    is_transaction = F.col("event_type").isin(*TRANSACTION_TYPES)
    payment_field_absent = ~F.array_contains(F.col("_keys"), "payment_method")
    missing = [
        (f"missing_{name}", F.col(name).isNull())
        for name in REQUIRED_FIELDS
        if name != "payment_method"
    ]
    return [
        *missing,
        ("missing_payment_method", payment_field_absent),
        ("missing_payment_method", is_transaction & F.col("payment_method").isNull()),
        ("invalid_event_id", _not_matching("event_id")),
        ("invalid_user_id", _not_matching("user_id")),
        ("invalid_product_id", _not_matching("product_id")),
        ("invalid_country", _not_matching("country")),
        ("invalid_category", _not_allowed("category")),
        ("invalid_event_type", _not_allowed("event_type")),
        ("invalid_payment_method", _not_allowed("payment_method")),
        (
            "invalid_amount",
            F.col("amount").isNotNull()
            & (_amount().isNull() | _is_quoted("amount") | (_amount() < 0)),
        ),
        ("invalid_amount", is_transaction & (_amount() == 0)),
        (
            "invalid_quantity",
            F.col("quantity").isNotNull()
            & (_quantity().isNull() | _is_quoted("quantity") | (_quantity() < 1)),
        ),
        (
            "invalid_timestamp",
            F.col("timestamp").isNotNull() & (_not_matching("timestamp") | _event_ts().isNull()),
        ),
        # A clock in the future would push the watermark and drop every normal event.
        ("timestamp_in_future", _event_ts() > F.current_timestamp() + F.expr(MAX_CLOCK_SKEW)),
    ]


def add_rejection_reasons(parsed: DataFrame) -> DataFrame:
    """Add `rejection_reasons`, which stays empty for valid events."""
    reasons = F.array_distinct(
        F.array_compact(
            F.array(*[F.when(condition, F.lit(reason)) for reason, condition in _rules()])
        )
    )
    not_json = F.col(CORRUPT_COLUMN).isNotNull() | F.col("raw_value").isNull()
    return parsed.withColumn(
        "rejection_reasons",
        F.when(not_json, F.array(F.lit("malformed_json"))).otherwise(reasons),
    )


def select_valid(validated: DataFrame) -> DataFrame:
    """Return valid events with their final types and their Bronze lineage."""
    return validated.filter(F.size("rejection_reasons") == 0).select(
        "event_id",
        "user_id",
        "product_id",
        "category",
        _amount().alias("amount"),
        _quantity().alias("quantity"),
        "event_type",
        _event_ts().alias("event_ts"),
        F.to_date(_event_ts()).alias("event_date"),
        "payment_method",
        "country",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingestion_ts",
    )


def select_invalid(validated: DataFrame) -> DataFrame:
    """Return rejected events with their reasons and everything needed to replay them."""
    return validated.filter(F.size("rejection_reasons") > 0).select(
        "raw_value",
        "rejection_reasons",
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingestion_ts",
        "ingestion_date",
    )


def deduplicate(valid_events: DataFrame, watermark: str) -> DataFrame:
    """Keep the first event of each event_id seen within the watermark delay."""
    return valid_events.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )

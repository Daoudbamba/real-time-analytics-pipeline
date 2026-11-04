"""Tests of the Silver validation, normalization and deduplication."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from pyspark.sql import functions as F

from jobs.silver.validation import (
    CONTRACT,
    add_rejection_reasons,
    deduplicate,
    parse_events,
    select_invalid,
    select_valid,
)
from producers.event_generator import EventGenerator

pytestmark = pytest.mark.spark

VALID_EVENT: dict[str, Any] = {
    "event_id": "0f8fad5b-d9cb-469f-a165-70867728950e",
    "user_id": "U000123",
    "product_id": "P00042",
    "category": "electronics",
    "amount": 149.9,
    "quantity": 2,
    "event_type": "purchase",
    "timestamp": "2026-11-01T10:15:30.500+00:00",
    "payment_method": "card",
    "country": "FR",
}


def validate(bronze_frame: Any, values: list[str | None]) -> Any:
    return add_rejection_reasons(parse_events(bronze_frame(values)))


def reasons_of(bronze_frame: Any, event: dict[str, Any]) -> list[str]:
    row = validate(bronze_frame, [json.dumps(event)]).first()
    return sorted(row["rejection_reasons"])


def test_valid_event_has_no_rejection_reason(bronze_frame: Any) -> None:
    assert reasons_of(bronze_frame, VALID_EVENT) == []


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"amount": -1}, "invalid_amount"),
        ({"amount": "12.5"}, "invalid_amount"),
        ({"amount": "twelve euros"}, "invalid_amount"),
        ({"amount": 0}, "invalid_amount"),
        ({"quantity": 0}, "invalid_quantity"),
        ({"quantity": 1.5}, "invalid_quantity"),
        ({"quantity": "2"}, "invalid_quantity"),
        ({"category": "unknown"}, "invalid_category"),
        ({"event_type": "teleport"}, "invalid_event_type"),
        ({"payment_method": "gold_coins"}, "invalid_payment_method"),
        ({"payment_method": None}, "missing_payment_method"),
        ({"country": "FRANCE"}, "invalid_country"),
        ({"user_id": "123"}, "invalid_user_id"),
        ({"product_id": "123"}, "invalid_product_id"),
        ({"event_id": "not-a-uuid"}, "invalid_event_id"),
        ({"timestamp": "not-a-timestamp"}, "invalid_timestamp"),
        ({"timestamp": "2026-13-45T10:00:00+00:00"}, "invalid_timestamp"),
    ],
)
def test_each_rule_gives_its_reason(
    bronze_frame: Any, changes: dict[str, Any], expected: str
) -> None:
    assert reasons_of(bronze_frame, {**VALID_EVENT, **changes}) == [expected]


@pytest.mark.parametrize("field", CONTRACT["required"])
def test_missing_field_is_named(bronze_frame: Any, field: str) -> None:
    event = {key: value for key, value in VALID_EVENT.items() if key != field}

    assert f"missing_{field}" in reasons_of(bronze_frame, event)


def test_page_view_needs_no_payment_method(bronze_frame: Any) -> None:
    event = {**VALID_EVENT, "event_type": "page_view", "payment_method": None, "amount": 0}

    assert reasons_of(bronze_frame, event) == []


def test_timestamp_far_in_the_future_is_rejected(bronze_frame: Any) -> None:
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

    assert reasons_of(bronze_frame, {**VALID_EVENT, "timestamp": future}) == ["timestamp_in_future"]


@pytest.mark.parametrize("value", ['{"event_id": "abc', "not json", None])
def test_unreadable_messages_are_malformed(bronze_frame: Any, value: str | None) -> None:
    row = validate(bronze_frame, [value]).first()

    assert row["rejection_reasons"] == ["malformed_json"]


def test_spark_agrees_with_the_json_schema(bronze_frame: Any) -> None:
    """Producers and Spark must reach the same verdict on the shared contract."""
    generator = EventGenerator(invalid_ratio=0.5, late_ratio=0.1, seed=11)
    events = [generator.next_event() for _ in range(3_000)]
    validator = Draft202012Validator(CONTRACT)

    expected = {}
    for offset, event in enumerate(events):
        is_valid = not isinstance(event.payload, str) and validator.is_valid(event.payload)
        expected[offset] = is_valid

    validated = validate(bronze_frame, [event.value.decode() for event in events])
    actual = {
        row["kafka_offset"]: len(row["rejection_reasons"]) == 0
        for row in validated.select("kafka_offset", "rejection_reasons").collect()
    }

    assert actual == expected
    assert 0 < sum(actual.values()) < len(actual)


def test_valid_events_get_their_final_types(bronze_frame: Any) -> None:
    row = select_valid(validate(bronze_frame, [json.dumps(VALID_EVENT)])).first()

    assert row["amount"] == Decimal("149.90")
    assert row["quantity"] == 2
    assert row["event_ts"] == datetime(2026, 11, 1, 10, 15, 30, 500000)
    assert str(row["event_date"]) == "2026-11-01"


def test_values_are_trimmed(bronze_frame: Any) -> None:
    event = {**VALID_EVENT, "country": " FR ", "category": "electronics "}

    row = select_valid(validate(bronze_frame, [json.dumps(event)])).first()

    assert (row["country"], row["category"]) == ("FR", "electronics")


def test_valid_and_invalid_outputs_split_every_event(bronze_frame: Any) -> None:
    generator = EventGenerator(invalid_ratio=0.3, seed=5)
    values = [generator.next_event().value.decode() for _ in range(500)]
    validated = validate(bronze_frame, values).cache()

    valid, invalid = select_valid(validated).count(), select_invalid(validated).count()

    assert valid + invalid == 500
    assert invalid > 0


def test_invalid_events_keep_what_is_needed_to_replay_them(bronze_frame: Any) -> None:
    row = select_invalid(validate(bronze_frame, ["not json"])).first()

    assert row["raw_value"] == "not json"
    assert row["kafka_offset"] == 0
    assert row["rejection_reasons"]


def test_duplicates_are_removed_across_micro_batches(
    spark: Any, bronze_frame: Any, tmp_path: Path
) -> None:
    source, sink = str(tmp_path / "source"), str(tmp_path / "sink")
    other = {**VALID_EVENT, "event_id": "1f8fad5b-d9cb-469f-a165-70867728950e"}

    def run_stream() -> None:
        stream = spark.readStream.format("delta").load(source)
        valid = select_valid(add_rejection_reasons(parse_events(stream)))
        (
            deduplicate(valid, "10 minutes")
            .writeStream.format("delta")
            .option("checkpointLocation", str(tmp_path / "checkpoint"))
            .trigger(availableNow=True)
            .start(sink)
            .awaitTermination()
        )

    first = [json.dumps(VALID_EVENT), json.dumps(VALID_EVENT)]
    bronze_frame(first).write.format("delta").save(source)
    run_stream()
    second = [json.dumps(VALID_EVENT), json.dumps(other)]
    bronze_frame(second).write.format("delta").mode("append").save(source)
    run_stream()

    result = spark.read.format("delta").load(sink)
    assert result.count() == 2
    assert result.select(F.countDistinct("event_id")).first()[0] == 2

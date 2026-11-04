"""Tests of the event generator."""

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from producers.event_generator import EventGenerator, GeneratedEvent

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "event_schema.json"
VALIDATOR = Draft202012Validator(
    json.loads(SCHEMA_PATH.read_text(encoding="utf-8")), format_checker=FormatChecker()
)
NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def generate(count: int, **options: float) -> list[GeneratedEvent]:
    generator = EventGenerator(seed=42, clock=lambda: NOW, **options)
    return [generator.next_event() for _ in range(count)]


def test_default_events_all_follow_the_contract() -> None:
    events = generate(2_000)

    assert all(event.kind == "valid" for event in events)
    assert all(VALIDATOR.is_valid(event.payload) for event in events)


def test_same_seed_gives_the_same_sequence() -> None:
    options = {"invalid_ratio": 0.2, "duplicate_ratio": 0.1, "late_ratio": 0.1}

    assert generate(200, **options) == generate(200, **options)


def test_invalid_events_all_break_the_contract() -> None:
    events = generate(2_000, invalid_ratio=1.0)

    for event in events:
        assert event.kind == "invalid"
        if isinstance(event.payload, str):
            with pytest.raises(json.JSONDecodeError):
                json.loads(event.payload)
        else:
            assert not VALIDATOR.is_valid(event.payload), event.payload


def test_invalid_events_cover_malformed_json_and_bad_fields() -> None:
    events = generate(500, invalid_ratio=1.0)

    assert any(isinstance(event.payload, str) for event in events)
    assert any(isinstance(event.payload, dict) for event in events)


def test_ratios_are_respected() -> None:
    kinds = Counter(
        event.kind
        for event in generate(20_000, invalid_ratio=0.1, duplicate_ratio=0.05, late_ratio=0.15)
    )

    assert kinds["invalid"] / 20_000 == pytest.approx(0.1, abs=0.01)
    assert kinds["duplicate"] / 20_000 == pytest.approx(0.05, abs=0.01)
    assert kinds["late"] / 20_000 == pytest.approx(0.15, abs=0.01)


def test_duplicates_repeat_an_event_already_produced() -> None:
    events = generate(5_000, duplicate_ratio=0.2)

    seen: set[str] = set()
    duplicates = 0
    for event in events:
        event_id = event.payload["event_id"]
        if event.kind == "duplicate":
            duplicates += 1
            assert event_id in seen
        else:
            assert event_id not in seen
            seen.add(event_id)
    assert duplicates > 0


def test_late_events_stay_within_the_maximum_delay() -> None:
    events = generate(500, late_ratio=1.0, max_lateness_seconds=60)

    for event in events:
        assert event.kind == "late"
        timestamp = datetime.fromisoformat(event.payload["timestamp"])
        assert NOW - timedelta(seconds=60) <= timestamp < NOW
        assert VALIDATOR.is_valid(event.payload)


def test_business_rules_of_valid_events() -> None:
    for event in (generated.payload for generated in generate(3_000)):
        is_transaction = event["event_type"] in ("purchase", "payment")
        assert (event["payment_method"] is not None) == is_transaction
        if event["event_type"] == "page_view":
            assert event["amount"] == 0
        else:
            assert event["amount"] > 0


def test_a_user_keeps_the_same_country() -> None:
    countries: dict[str, set[str]] = {}
    for generated in generate(20_000):
        event = generated.payload
        countries.setdefault(event["user_id"], set()).add(event["country"])

    assert all(len(values) == 1 for values in countries.values())


def test_a_few_products_receive_most_of_the_traffic() -> None:
    products = Counter(event.payload["product_id"] for event in generate(20_000))

    top_ten = sum(count for _, count in products.most_common(10))
    assert top_ten / 20_000 > 0.3


def test_serialized_value_and_key() -> None:
    event = generate(1)[0]

    assert json.loads(event.value) == event.payload
    assert event.key == event.payload["user_id"]


def test_malformed_payload_has_no_key() -> None:
    event = GeneratedEvent('{"event_id": "abc', "invalid")

    assert event.key is None
    assert event.value == b'{"event_id": "abc'


def test_ratios_above_one_are_rejected() -> None:
    with pytest.raises(ValueError, match="must not exceed 1"):
        EventGenerator(invalid_ratio=0.6, duplicate_ratio=0.6)

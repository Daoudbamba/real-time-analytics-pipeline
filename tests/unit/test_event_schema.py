"""Tests of the event contract."""

import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "event_schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)

VALID_EVENT: dict[str, Any] = {
    "event_id": "0f8fad5b-d9cb-469f-a165-70867728950e",
    "user_id": "U000123",
    "product_id": "P00042",
    "category": "electronics",
    "amount": 149.9,
    "quantity": 2,
    "event_type": "purchase",
    "timestamp": "2026-11-01T10:15:30+00:00",
    "payment_method": "card",
    "country": "FR",
}


def test_schema_is_a_valid_json_schema() -> None:
    Draft202012Validator.check_schema(SCHEMA)


def test_valid_event_is_accepted() -> None:
    assert VALIDATOR.is_valid(VALID_EVENT)


def test_page_view_may_have_no_payment_method() -> None:
    event = {**VALID_EVENT, "event_type": "page_view", "payment_method": None, "amount": 0}

    assert VALIDATOR.is_valid(event)


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": -1},
        {"amount": "12.5"},
        {"quantity": 0},
        {"category": "unknown"},
        {"event_type": "unknown"},
        {"country": "france"},
        {"timestamp": "not-a-timestamp"},
        {"user_id": ""},
        {"event_id": "not-a-uuid"},
        {"payment_method": None},
        {"amount": 0},
    ],
)
def test_invalid_values_are_rejected(changes: dict[str, Any]) -> None:
    assert not VALIDATOR.is_valid({**VALID_EVENT, **changes})


@pytest.mark.parametrize("field", SCHEMA["required"])
def test_missing_fields_are_rejected(field: str) -> None:
    event = {key: value for key, value in VALID_EVENT.items() if key != field}

    assert not VALIDATOR.is_valid(event)

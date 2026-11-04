"""Tests of the central configuration loader."""

from pathlib import Path

import pytest

from rtap.config import ConfigError, interpolate, load_config
from rtap.logging_utils import JsonFormatter

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "app.yaml"


def test_defaults_work_without_any_environment_variable() -> None:
    config = load_config(CONFIG_PATH, env={})

    assert config.kafka.bootstrap_servers == "kafka:9092"
    assert config.kafka.topics.raw == "ecommerce-events"
    assert config.storage.backend == "local"
    assert config.storage.table_path("gold", "sales") == "/lakehouse/gold/sales"
    assert config.storage.checkpoint_path("bronze") == "/lakehouse/_checkpoints/bronze"
    assert config.producer.events_per_second == 50


def test_environment_overrides_defaults() -> None:
    config = load_config(CONFIG_PATH, env={"PRODUCER_INVALID_RATIO": "0.5", "LOG_LEVEL": "DEBUG"})

    assert config.producer.invalid_ratio == 0.5
    assert config.log_level == "DEBUG"


@pytest.mark.parametrize(
    ("env", "expected_root"),
    [
        ({"STORAGE_BACKEND": "s3", "S3_BUCKET": "shop"}, "s3a://shop/lakehouse"),
        ({"STORAGE_BACKEND": "gcs", "GCS_BUCKET": "shop", "GCS_PREFIX": "dev"}, "gs://shop/dev"),
    ],
)
def test_cloud_backends_resolve_their_uri(env: dict[str, str], expected_root: str) -> None:
    assert load_config(CONFIG_PATH, env=env).storage.root == expected_root


@pytest.mark.parametrize(
    "env",
    [
        {"STORAGE_BACKEND": "s3"},
        {"STORAGE_BACKEND": "azure"},
        {"PRODUCER_INVALID_RATIO": "1.5"},
        {"PRODUCER_EVENTS_PER_SECOND": "0"},
    ],
)
def test_inconsistent_values_are_rejected(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        load_config(CONFIG_PATH, env=env)


def test_missing_required_variable_is_rejected() -> None:
    with pytest.raises(ConfigError, match="SECRET"):
        interpolate({"key": "${SECRET}"}, env={})


def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="Cannot read"):
        load_config(tmp_path / "missing.yaml", env={})


def test_unknown_layer_is_rejected() -> None:
    with pytest.raises(ConfigError, match="platinum"):
        load_config(CONFIG_PATH, env={}).storage.table_path("platinum", "sales")


def test_json_formatter_keeps_extra_fields() -> None:
    import json
    import logging

    record = logging.makeLogRecord({"msg": "sent", "levelname": "INFO", "topic": "raw"})

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "sent"
    assert payload["topic"] == "raw"

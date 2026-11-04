"""Typed access to config/app.yaml, with environment variable interpolation."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Z0-9_]+)(?::-(?P<default>[^}]*))?\}")
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "app.yaml"
_LAYERS = ("bronze", "silver", "quarantine", "gold")


class ConfigError(ValueError):
    """Raised when the configuration is missing or inconsistent."""


@dataclass(frozen=True)
class KafkaTopics:
    raw: str
    valid: str
    invalid: str
    transactions: str


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topics: KafkaTopics
    producer: Mapping[str, Any]
    consumer: Mapping[str, Any]


@dataclass(frozen=True)
class ProducerConfig:
    events_per_second: float
    invalid_ratio: float
    duplicate_ratio: float
    late_ratio: float
    max_lateness_seconds: int
    metrics_port: int


@dataclass(frozen=True)
class StorageConfig:
    """Resolves lakehouse locations for the selected storage backend."""

    backend: str
    root: str

    def table_path(self, layer: str, table: str) -> str:
        """Return the location of a Delta table, such as gold/revenue_per_minute."""
        if layer not in _LAYERS:
            raise ConfigError(f"Unknown layer '{layer}', expected one of {_LAYERS}")
        return f"{self.root}/{layer}/{table}"

    def checkpoint_path(self, query_name: str) -> str:
        """Return the checkpoint location of a streaming query."""
        return f"{self.root}/_checkpoints/{query_name}"


@dataclass(frozen=True)
class StreamingConfig:
    trigger_interval: str
    deduplication_watermark: str
    aggregation_watermark: str
    aggregation_window: str
    metrics_port: int


@dataclass(frozen=True)
class QualityConfig:
    max_invalid_ratio: float
    max_gold_staleness_minutes: int


@dataclass(frozen=True)
class AppConfig:
    name: str
    environment: str
    log_level: str
    kafka: KafkaConfig
    producer: ProducerConfig
    storage: StorageConfig
    streaming: StreamingConfig
    quality: QualityConfig
    vacuum_retention_hours: int


def interpolate(value: Any, env: Mapping[str, str]) -> Any:
    """Replace ${VAR} and ${VAR:-default} placeholders in a parsed YAML tree."""
    if isinstance(value, dict):
        return {key: interpolate(item, env) for key, item in value.items()}
    if isinstance(value, list):
        return [interpolate(item, env) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name, default = match.group("name"), match.group("default")
        resolved = env.get(name) or default
        if resolved is None:
            raise ConfigError(f"Environment variable {name} is required")
        return resolved

    return _ENV_PATTERN.sub(replace, value)


def _resolve_storage(section: Mapping[str, Any]) -> StorageConfig:
    backend = section["backend"]
    if backend == "local":
        return StorageConfig(backend, str(section["local_root"]).rstrip("/"))
    if backend not in ("s3", "gcs"):
        raise ConfigError(f"Unknown storage backend '{backend}', expected local, s3 or gcs")

    bucket, prefix = section[backend]["bucket"], section[backend]["prefix"]
    if not bucket:
        raise ConfigError(f"Storage backend '{backend}' requires a bucket name")
    scheme = "s3a" if backend == "s3" else "gs"
    return StorageConfig(backend, f"{scheme}://{bucket}/{prefix}".rstrip("/"))


def _parse_ratio(name: str, value: Any) -> float:
    ratio = float(value)
    if not 0 <= ratio <= 1:
        raise ConfigError(f"{name} must be between 0 and 1, got {ratio}")
    return ratio


def _parse_producer(section: Mapping[str, Any]) -> ProducerConfig:
    events_per_second = float(section["events_per_second"])
    if events_per_second <= 0:
        raise ConfigError("producer.events_per_second must be positive")
    return ProducerConfig(
        events_per_second=events_per_second,
        invalid_ratio=_parse_ratio("producer.invalid_ratio", section["invalid_ratio"]),
        duplicate_ratio=_parse_ratio("producer.duplicate_ratio", section["duplicate_ratio"]),
        late_ratio=_parse_ratio("producer.late_ratio", section["late_ratio"]),
        max_lateness_seconds=int(section["max_lateness_seconds"]),
        metrics_port=int(section["metrics_port"]),
    )


def load_config(
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> AppConfig:
    """Load and validate the configuration.

    Args:
        path: YAML file to read. Defaults to $RTAP_CONFIG, then config/app.yaml.
        env: Variables used for interpolation. Defaults to os.environ.

    Raises:
        ConfigError: If the file is missing, incomplete or inconsistent.
    """
    env = os.environ if env is None else env
    config_path = Path(path or env.get("RTAP_CONFIG") or _DEFAULT_CONFIG_PATH)
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"Cannot read configuration file {config_path}: {error}") from error

    data = interpolate(raw, env)
    try:
        kafka = data["kafka"]
        streaming = data["streaming"]
        quality = data["quality"]
        return AppConfig(
            name=data["app"]["name"],
            environment=data["app"]["environment"],
            log_level=data["app"]["log_level"],
            kafka=KafkaConfig(
                bootstrap_servers=kafka["bootstrap_servers"],
                topics=KafkaTopics(**kafka["topics"]),
                producer=kafka["producer"],
                consumer=kafka["consumer"],
            ),
            producer=_parse_producer(data["producer"]),
            storage=_resolve_storage(data["storage"]),
            streaming=StreamingConfig(
                trigger_interval=streaming["trigger_interval"],
                deduplication_watermark=streaming["deduplication_watermark"],
                aggregation_watermark=streaming["aggregation_watermark"],
                aggregation_window=streaming["aggregation_window"],
                metrics_port=int(streaming["metrics_port"]),
            ),
            quality=QualityConfig(
                max_invalid_ratio=_parse_ratio(
                    "quality.max_invalid_ratio", quality["max_invalid_ratio"]
                ),
                max_gold_staleness_minutes=int(quality["max_gold_staleness_minutes"]),
            ),
            vacuum_retention_hours=int(data["maintenance"]["vacuum_retention_hours"]),
        )
    except (KeyError, TypeError) as error:
        raise ConfigError(f"Invalid configuration in {config_path}: {error!r}") from error

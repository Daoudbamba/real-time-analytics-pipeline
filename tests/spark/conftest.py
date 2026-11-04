"""Shared fixtures of the Spark tests. They run inside the Spark image."""

import importlib.util
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

collect_ignore_glob = [] if importlib.util.find_spec("pyspark") else ["test_*.py"]


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_frame(spark: Any) -> Any:
    """Build a Bronze DataFrame from raw message values."""
    from jobs.bronze.ingest import BRONZE_SCHEMA

    def build(values: list[str | None]) -> Any:
        now = datetime.now(UTC).replace(tzinfo=None)
        rows = [
            ("key", value, "ecommerce-events", 0, offset, now, now, now.date())
            for offset, value in enumerate(values)
        ]
        return spark.createDataFrame(rows, BRONZE_SCHEMA)

    return build

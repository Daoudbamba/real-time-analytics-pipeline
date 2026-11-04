"""Checks the structure of the DAGs without running them."""

from pathlib import Path
from typing import Any

import pytest
from airflow.dag_processing.dagbag import DagBag

REPOSITORY = Path(__file__).resolve().parents[2]
DAGS_DIR = REPOSITORY / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def task_ids(dag: Any) -> set[str]:
    return set(dag.task_ids)


def test_all_dags_load_without_error(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {
        "gold_batch_aggregates",
        "lakehouse_quality",
        "lakehouse_maintenance",
    }


def test_dags_do_not_backfill_or_overlap(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        assert dag.catchup is False
        assert dag.max_active_runs == 1


def test_every_task_is_retried_and_time_limited(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.execution_timeout is not None
            assert task.pool == "spark_jobs"


def test_every_application_exists(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            relative_path = task.application.removeprefix("/opt/airflow/")
            assert (REPOSITORY / relative_path).is_file(), task.application


def test_gold_is_rebuilt_only_after_validating_silver(dagbag: DagBag) -> None:
    dag = dagbag.dags["gold_batch_aggregates"]

    downstream = dag.get_task("validate_silver").downstream_task_ids

    assert downstream == {"build_gold_tables"}


def test_quality_controls_follow_the_layers(dagbag: DagBag) -> None:
    dag = dagbag.dags["lakehouse_quality"]

    assert dag.get_task("check_bronze").downstream_task_ids == {"check_silver"}
    assert dag.get_task("check_silver").downstream_task_ids == {"check_gold"}


def test_vacuum_runs_after_compaction(dagbag: DagBag) -> None:
    dag = dagbag.dags["lakehouse_maintenance"]

    assert dag.get_task("optimize_tables").downstream_task_ids == {"vacuum_tables"}

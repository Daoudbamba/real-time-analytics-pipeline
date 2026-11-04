"""Building blocks shared by the DAGs."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger("rtap.airflow")

SPARK_CONNECTION = "spark_cluster"
SPARK_POOL = "spark_jobs"
JOBS_DIR = "/opt/airflow/jobs"
# Cores left free by the streaming application on the Spark worker.
BATCH_SPARK_CONF = {
    "spark.cores.max": "2",
    "spark.driver.host": "airflow-scheduler",
    "spark.ui.enabled": "false",
}


def log_failure(context: dict[str, Any]) -> None:
    """Record failed tasks. This is the place to plug an alerting channel."""
    instance = context["task_instance"]
    logger.error(
        "Task failed: dag=%s task=%s try=%s error=%s",
        instance.dag_id,
        instance.task_id,
        instance.try_number,
        context.get("exception"),
    )


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": log_failure,
}


def spark_job(
    task_id: str,
    application: str,
    arguments: Sequence[str] = (),
    **kwargs: Any,
) -> SparkSubmitOperator:
    """Submit a job of the jobs/ folder to the Spark cluster, in client mode.

    All jobs share one pool slot: running several Spark drivers at once would not
    fit on a development machine.
    """
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONNECTION,
        application=f"{JOBS_DIR}/{application}",
        application_args=list(arguments),
        name=f"airflow-{task_id}",
        conf=BATCH_SPARK_CONF,
        driver_memory="768m",
        executor_memory="768m",
        pool=SPARK_POOL,
        **kwargs,
    )

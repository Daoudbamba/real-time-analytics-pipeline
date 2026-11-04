"""Hourly rebuild of the Gold tables that do not fit a streaming window."""

from datetime import datetime

from airflow.sdk import DAG
from rtap_airflow import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="gold_batch_aggregates",
    description="Validates Silver, then rebuilds top products and the sales trend.",
    doc_md=__doc__,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "batch"],
) as dag:
    validate_silver = spark_job(
        "validate_silver", "quality/checks.py", ["--layer", "silver"], retries=0
    )
    build_gold = spark_job("build_gold_tables", "gold/batch_aggregates.py")

    validate_silver >> build_gold

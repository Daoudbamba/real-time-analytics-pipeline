"""Controls the lakehouse layer by layer.

A failed layer stops the controls of the next ones: their result would mean nothing.
Results are stored in gold/data_quality_results.
"""

from datetime import datetime

from airflow.sdk import DAG
from rtap_airflow import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="lakehouse_quality",
    description="Controls the Bronze, Silver and Gold tables.",
    doc_md=__doc__,
    schedule="30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["quality"],
) as dag:
    check_bronze = spark_job("check_bronze", "quality/checks.py", ["--layer", "bronze"])
    check_silver = spark_job("check_silver", "quality/checks.py", ["--layer", "silver"])
    check_gold = spark_job("check_gold", "quality/checks.py", ["--layer", "gold"])

    check_bronze >> check_silver >> check_gold

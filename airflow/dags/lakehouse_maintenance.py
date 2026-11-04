"""Nightly maintenance of the Delta tables.

Compaction runs first, so that VACUUM can later clean the files it replaced. Bronze
is controlled afterwards to confirm the tables remain readable.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from rtap_airflow import DEFAULT_ARGS, spark_job

with DAG(
    dag_id="lakehouse_maintenance",
    description="Compacts the Delta tables and removes obsolete files.",
    doc_md=__doc__,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    optimize = spark_job(
        "optimize_tables", "maintenance/delta_maintenance.py", ["--action", "optimize"]
    )
    # VACUUM lists every file of every table, which takes minutes per table on a laptop.
    vacuum = spark_job(
        "vacuum_tables",
        "maintenance/delta_maintenance.py",
        ["--action", "vacuum"],
        execution_timeout=timedelta(hours=1),
    )
    verify = spark_job("verify_tables", "quality/checks.py", ["--layer", "bronze"])

    optimize >> vacuum >> verify

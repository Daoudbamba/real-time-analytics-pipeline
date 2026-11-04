"""Runs the whole Medallion stream in one Spark application.

Sharing one application keeps the memory footprint small on a laptop. If a query
fails, the application stops and exits with an error: on restart, every query
resumes from its checkpoint.

Run it with `python -m jobs.streaming_pipeline`, not spark-submit: Python must be the
main process to receive SIGTERM and stop the queries cleanly. PYSPARK_SUBMIT_ARGS
carries the Spark master and memory settings.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from prometheus_client import start_http_server

from jobs.bronze.ingest import start_bronze_query
from jobs.gold.streaming import start_gold_queries
from jobs.metrics import MetricsListener, PipelineMetrics
from jobs.session import build_spark
from jobs.silver.transform import start_quarantine_query, start_silver_query
from rtap.config import load_config
from rtap.logging_utils import configure_logging

logger = logging.getLogger("rtap.streaming")

STOP_TIMEOUT_MILLISECONDS = 60_000
POLL_INTERVAL_SECONDS = 2


def main() -> int:
    """Start every streaming query and wait. Returns the process exit code."""
    config = load_config()
    configure_logging(config.log_level)

    spark = build_spark("medallion-streaming", config.storage)
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.streaming.stopTimeout", STOP_TIMEOUT_MILLISECONDS)

    metrics = PipelineMetrics()
    spark.streams.addListener(MetricsListener(metrics))
    start_http_server(config.streaming.metrics_port)

    stop = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        logger.info("Stop requested", extra={"signal": signal.Signals(signum).name})
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    # Order matters: each layer creates the table that the next one reads.
    start_bronze_query(spark, config)
    start_silver_query(spark, config, metrics)
    start_quarantine_query(spark, config, metrics)
    start_gold_queries(spark, config)
    logger.info("Streaming pipeline started", extra={"storage": config.storage.root})

    exit_code = 0
    try:
        while not stop.is_set():
            if spark.streams.awaitAnyTermination(POLL_INTERVAL_SECONDS):
                logger.error("A streaming query stopped unexpectedly")
                exit_code = 1
                break
    except Exception:
        logger.exception("Streaming pipeline failed")
        exit_code = 1
    for query in spark.streams.active:
        query.stop()
    spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

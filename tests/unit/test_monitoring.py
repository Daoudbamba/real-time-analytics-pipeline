"""Checks that the monitoring files stay consistent with the metrics the code exposes."""

import json
import re
from pathlib import Path
from typing import Any

import yaml

REPOSITORY = Path(__file__).resolve().parents[2]
GRAFANA = REPOSITORY / "dashboards" / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards").glob("*.json"))
DATASOURCES = GRAFANA / "provisioning" / "datasources" / "prometheus.yml"
PROMETHEUS = REPOSITORY / "monitoring" / "prometheus" / "prometheus.yml"
ALERTS = REPOSITORY / "monitoring" / "prometheus" / "alerts.yml"
METRIC_SOURCES = (
    REPOSITORY / "producers" / "kafka_producer.py",
    REPOSITORY / "jobs" / "metrics.py",
)
DECLARATION = re.compile(r"(Counter|Gauge|Histogram)\(\s*\"(rtap_\w+)\"")

# prometheus_client adds these suffixes to the series it exposes.
SUFFIXES = {
    "Counter": ("_total", "_created"),
    "Gauge": ("",),
    "Histogram": ("_bucket", "_sum", "_count", "_created"),
}


def exposed_metrics() -> set[str]:
    """Return every series name the producer and the Spark driver can expose."""
    names = set()
    for source in METRIC_SOURCES:
        for kind, name in DECLARATION.findall(source.read_text()):
            names.update(name + suffix for suffix in SUFFIXES[kind])
    return names


def dashboards() -> list[dict[str, Any]]:
    return [json.loads(path.read_text(encoding="utf-8")) for path in DASHBOARDS]


def dashboard_queries() -> list[str]:
    return [
        target["expr"]
        for dashboard in dashboards()
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    ]


def alert_rules() -> list[dict[str, Any]]:
    groups = yaml.safe_load(ALERTS.read_text())["groups"]
    return [rule for group in groups for rule in group["rules"]]


def test_both_metric_sources_are_found() -> None:
    names = exposed_metrics()

    assert "rtap_producer_events_delivered_total" in names
    assert "rtap_pipeline_latency_seconds" in names


def test_dashboard_and_alerts_only_use_exposed_metrics() -> None:
    expressions = dashboard_queries() + [rule["expr"] for rule in alert_rules()]
    used = {name for expr in expressions for name in re.findall(r"\brtap_\w+", expr)}

    assert used
    assert used - exposed_metrics() == set()


def test_dashboards_have_unique_uids() -> None:
    uids = [dashboard["uid"] for dashboard in dashboards()]

    assert len(uids) == 2
    assert len(uids) == len(set(uids))


def test_dashboard_panels_have_unique_ids_and_the_provisioned_datasource() -> None:
    datasource_uid = yaml.safe_load(DATASOURCES.read_text())["datasources"][0]["uid"]
    for dashboard in dashboards():
        ids = [panel["id"] for panel in dashboard["panels"]]

        assert len(ids) == len(set(ids)), dashboard["uid"]
        for panel in dashboard["panels"]:
            if panel["type"] != "row":
                assert panel["datasource"]["uid"] == datasource_uid, panel["title"]


def test_every_alert_has_a_severity_and_a_summary() -> None:
    for rule in alert_rules():
        assert rule["labels"]["severity"] in {"warning", "critical"}, rule["alert"]
        assert rule["annotations"]["summary"], rule["alert"]


def test_alerts_only_reference_scraped_jobs() -> None:
    jobs = {job["job_name"] for job in yaml.safe_load(PROMETHEUS.read_text())["scrape_configs"]}
    referenced = {
        job
        for rule in alert_rules()
        for selector in re.findall(r'job[!=]~?"([^"]+)"', rule["expr"])
        for job in selector.split("|")
    }

    assert referenced <= jobs

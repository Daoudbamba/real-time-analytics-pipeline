#!/usr/bin/env bash
# Runs the test suites. Spark and Airflow tests run inside the project images, so the
# host only needs Python for the unit tests.
#   bash scripts/run_tests.sh unit          unit tests on the host (no Docker needed)
#   bash scripts/run_tests.sh integration   producer against a running Kafka
#   bash scripts/run_tests.sh spark         Spark and Delta tests in the rtap-spark image
#   bash scripts/run_tests.sh dags          DAG structure tests in the rtap-airflow image
#   bash scripts/run_tests.sh typecheck     mypy on the host and in the rtap-spark image
#   bash scripts/run_tests.sh all           unit, spark, dags and typecheck
# Unit and Spark tests print their coverage.
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

PYTEST_VERSION=9.2.0
JSONSCHEMA_VERSION=4.26.0
PYTEST_COV_VERSION=7.1.0
MYPY_VERSION=2.4.1
TYPES_PYYAML_VERSION=6.0.12.20261030

host_python() {
    local candidate
    for candidate in .venv/bin/python .venv/Scripts/python python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            echo "$candidate"
            return
        fi
    done
    die "Python is not installed: create the virtual environment described in the README."
}

run_unit() {
    log "Unit tests"
    "$(host_python)" -m pytest -q -m "not integration" tests/unit \
        --cov=rtap --cov=producers --cov-report=term
}

run_integration() {
    log "Integration tests (Kafka must be running: bash scripts/create_topics.sh)"
    "$(host_python)" -m pytest -q -m integration tests/integration
}

# The images are rebuilt first (cached when nothing changed). The repository is mounted
# read-only and copied, so the tests never write into it.
run_spark() {
    log "Spark tests"
    docker build --quiet -f docker/spark/Dockerfile -t rtap-spark:local . >/dev/null
    docker run --rm -v "$HOST_ROOT_DIR:/repo:ro" rtap-spark:local bash -c "
        set -e
        pip install --quiet --target /tmp/dev pytest==$PYTEST_VERSION jsonschema==$JSONSCHEMA_VERSION \
            pytest-cov==$PYTEST_COV_VERSION
        cd /opt/app && cp -r /repo/tests /repo/pyproject.toml /repo/producers .
        PYTHONPATH=/tmp/dev:/opt/app/src:/opt/app python -m pytest tests/spark -q -p no:cacheprovider \
            --cov=jobs --cov-report=term"
}

run_dags() {
    log "Airflow DAG tests"
    docker build --quiet -f docker/airflow/Dockerfile -t rtap-airflow:local . >/dev/null
    # Airflow is installed in /home/airflow/.local for UID 1000.
    docker run --rm --user 1000:0 --entrypoint bash -v "$HOST_ROOT_DIR:/repo:ro" rtap-airflow:local -c "
        set -e
        python=/home/airflow/.local/bin/python
        mkdir -p /tmp/r /tmp/ah
        cp -r /repo/tests /repo/airflow /repo/jobs /repo/pyproject.toml /tmp/r/
        \$python -m pip install --quiet --target /tmp/dev pytest==$PYTEST_VERSION
        cd /tmp/r
        AIRFLOW_HOME=/tmp/ah PYTHONPATH=/tmp/dev:/tmp/r/airflow/plugins \
            \$python -m pytest tests/dags -q -p no:cacheprovider -o pythonpath="
}

# mypy needs the libraries it checks: confluent-kafka on the host, PySpark in the image.
run_typecheck() {
    log "Type checks"
    "$(host_python)" -m mypy src producers
    docker build --quiet -f docker/spark/Dockerfile -t rtap-spark:local . >/dev/null
    docker run --rm -v "$HOST_ROOT_DIR:/repo:ro" rtap-spark:local bash -c "
        set -e
        pip install --quiet --target /tmp/dev mypy==$MYPY_VERSION types-PyYAML==$TYPES_PYYAML_VERSION
        cd /tmp && cp -r /repo/src /repo/producers /repo/jobs /repo/tests /repo/pyproject.toml .
        PYTHONPATH=/tmp/dev python -m mypy src jobs tests/e2e"
}

case "${1:-}" in
    unit) run_unit ;;
    integration) run_integration ;;
    spark) run_spark ;;
    dags) run_dags ;;
    typecheck) run_typecheck ;;
    all) run_unit && run_spark && run_dags && run_typecheck ;;
    *) sed -n '2,10p' "$0"; exit 1 ;;
esac

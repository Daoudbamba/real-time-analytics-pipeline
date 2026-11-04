#!/usr/bin/env bash
# End-to-end verification on a running, empty stack:
#   1. sends a seeded batch of events,
#   2. waits until the streaming layers have processed them,
#   3. reconciles the streaming tables with the replayed generator and the Kafka offsets,
#   4. runs the Gold batch and quality DAGs through Airflow while new events flow,
#   5. checks what the DAGs wrote,
#   6. checks that Prometheus scrapes every service and no critical alert fires.
# Usage: bash scripts/verify_pipeline.sh [--events 24000] [--rate 100] [--seed 42] [--skip-airflow]
# Gold only closes a window two minutes after newer events: keep the run several minutes long.
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

events=24000
rate=100
seed=42
skip_airflow=false
while (($#)); do
    case "$1" in
        --events) events="${2:-}"; shift ;;
        --rate) rate="${2:-}"; shift ;;
        --seed) seed="${2:-}"; shift ;;
        --skip-airflow) skip_airflow=true ;;
        -h | --help) sed -n '2,10p' "$0"; exit 0 ;;
        *) die "unknown option $1 (see --help)" ;;
    esac
    shift
done
[[ "$events" =~ ^[1-9][0-9]*$ && "$seed" =~ ^[0-9]+$ ]] || die "--events and --seed must be integers"

PROCESSING_TIMEOUT_SECONDS=900
DAG_TIMEOUT_SECONDS=1800
LIVE_RATE=20
failures=0

pass() { log "PASS $*"; }
fail() { log "FAIL $*"; failures=$((failures + 1)); }

prometheus_value() {
    # Sum of an instant query, 0 when it returns nothing.
    curl --silent --fail --max-time 10 --get 'http://localhost:9090/api/v1/query' \
        --data-urlencode "query=sum($1) or vector(0)" \
        | grep -o '"value":\[[^]]*\]' | sed 's/.*,"\([^"]*\)"\]/\1/' | cut -d. -f1
}

topic_offsets() {
    compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
        --topic "$1" | awk -F: '{total += $3} END {print total + 0}'
}

wait_for_rows() {
    local query="$1" target="$2" rows deadline=$((SECONDS + PROCESSING_TIMEOUT_SECONDS))
    while :; do
        rows="$(prometheus_value "rtap_streaming_input_rows_total{query=\"$query\"}")"
        ((${rows:-0} >= target)) && { log "$query read $rows rows."; return 0; }
        ((SECONDS >= deadline)) && { fail "$query read ${rows:-0}/$target rows"; return 1; }
        sleep 15
    done
}

airflow() { compose exec -T airflow-scheduler airflow "$@"; }

# Reading the metadata database is much faster than the Airflow CLI (about 30 s a call).
airflow_sql() {
    # shellcheck disable=SC2016 # expanded by the shell of the postgres container
    compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"' sh "$1"
}

wait_for_airflow_idle() {
    # After a start, Airflow runs the latest interval of every DAG: let these runs end
    # first, so that they neither share the memory nor the Spark pool with the test.
    local active deadline=$((SECONDS + DAG_TIMEOUT_SECONDS))
    while :; do
        active="$(airflow_sql "select count(*) from dag_run where state in ('queued', 'running')")"
        ((active == 0)) && return 0
        ((SECONDS >= deadline)) && die "Airflow still has $active active runs"
        log "Airflow has $active active runs, waiting..."
        sleep 30
    done
}

run_dag() {
    local dag="$1" run_id state deadline=$((SECONDS + DAG_TIMEOUT_SECONDS))
    run_id="verify__$(date -u +%Y%m%dT%H%M%S)"
    airflow dags trigger "$dag" --run-id "$run_id" >/dev/null
    log "Triggered $dag ($run_id), waiting..."
    while :; do
        state="$(airflow_sql "select state from dag_run where dag_id = '$dag' and run_id = '$run_id'")"
        case "$state" in
            success) pass "DAG $dag"; return 0 ;;
            failed) fail "DAG $dag (see the Airflow UI, run $run_id)"; return 1 ;;
        esac
        ((SECONDS >= deadline)) && { fail "DAG $dag still ${state:-unknown}"; return 1; }
        sleep 30
    done
}

require_docker
compose ps --status running --services | grep -qx streaming \
    || die "the stack is not running: bash scripts/start_pipeline.sh"
already="$(topic_offsets ecommerce-events)"
((already == 0)) || die "Kafka already holds $already events. The reconciliation needs an empty stack:
  bash scripts/stop_pipeline.sh --volumes --yes && bash scripts/start_pipeline.sh"

$skip_airflow || wait_for_airflow_idle
log "1/6 Sending $events events (seed $seed)..."
bash scripts/generate_data.sh --events "$events" --rate "$rate" --seed "$seed" | grep -v 'Producer progress'

log "2/6 Waiting for the streaming queries..."
for query in bronze_events silver_events quarantine_events; do
    wait_for_rows "$query" "$events" || true
done
# The Gold queries read Silver: give them two micro-batches more.
sleep 70

# Same image, network and volume as the streaming service, but a local Spark session:
# the reconciliation does not take cores from the cluster.
reconcile() {
    local output status=0 checks
    output="$(compose run --rm --no-deps -T         -e PRODUCER_INVALID_RATIO="$(env_value PRODUCER_INVALID_RATIO)"         -e PRODUCER_DUPLICATE_RATIO="$(env_value PRODUCER_DUPLICATE_RATIO)"         -e PRODUCER_LATE_RATIO="$(env_value PRODUCER_LATE_RATIO)"         -e PYSPARK_SUBMIT_ARGS="--master local[2] --driver-memory 768m --conf spark.ui.showConsoleProgress=false pyspark-shell"         -v "$HOST_ROOT_DIR/tests/e2e:/opt/app/e2e:ro" -v "$HOST_ROOT_DIR/producers:/opt/app/producers:ro"         streaming python -m e2e.reconcile_pipeline "$@" 2>&1)" || status=$?
    checks="$(grep '^CHECK' <<<"$output" || true)"
    if [[ -z "$checks" ]]; then
        printf '%s
' "$output" | tail -n 20
        fail "reconciliation $1 did not run (exit $status)"
    else
        printf '%s
' "$checks" | sed 's/^CHECK /  /'
        if ((status == 0)); then pass "reconciliation $1"; else fail "reconciliation $1"; fi
    fi
}

log "3/6 Reconciling the streaming layers with the seeded run..."
reconcile streaming --events "$events" --seed "$seed"     --raw-offsets "$(topic_offsets ecommerce-events)"     --valid-offsets "$(topic_offsets valid-events)"     --invalid-offsets "$(topic_offsets invalid-events)"

if $skip_airflow; then
    log "4/6 Airflow skipped."
    log "5/6 Airflow skipped."
else
    # The DAGs control a live pipeline: Gold must stay fresh, so events flow again.
    log "4/6 Running the Airflow DAGs while the producer runs..."
    bash scripts/generate_data.sh --continuous --rate "$LIVE_RATE" >/dev/null
    trap 'bash scripts/generate_data.sh --stop >/dev/null 2>&1' EXIT
    run_dag gold_batch_aggregates || true
    run_dag lakehouse_quality || true
    bash scripts/generate_data.sh --stop >/dev/null
    log "5/6 Checking the DAG outputs..."
    reconcile batch
fi

log "6/6 Checking the monitoring..."
# The producer has been stopped: its target is expected to be down now.
down="$(prometheus_value 'count(up{job!="producer"} == 0)')"
total="$(prometheus_value 'count(up{job!="producer"})')"
if ((down == 0 && total > 0)); then
    pass "Prometheus scrapes $total targets, all up"
else
    fail "$down of $total Prometheus targets down"
fi
critical="$(prometheus_value 'count(ALERTS{alertstate="firing",severity="critical"})')"
if ((critical == 0)); then
    pass "no critical alert firing"
else
    fail "$critical critical alert(s) firing"
fi

if ((failures == 0)); then
    log "Verification passed."
else
    die "verification failed: $failures problem(s)."
fi

#!/usr/bin/env bash
# Builds the images and starts the whole stack, then waits until every service is healthy.
# The event producer stays off unless --with-producer is given: see scripts/generate_data.sh.
# Usage: bash scripts/start_pipeline.sh [--with-producer] [--no-build]
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

profiles=()
build=true
for argument in "$@"; do
    case "$argument" in
        --with-producer) profiles=(--profile producer) ;;
        --no-build) build=false ;;
        -h | --help) sed -n '2,4p' "$0"; exit 0 ;;
        *) die "unknown option $argument (see --help)" ;;
    esac
done

require_docker
require_env
warn_if_low_memory

if $build; then
    log "Building the images (several minutes the first time)..."
    compose "${profiles[@]}" build
fi

# Airflow, Spark and Grafana need a few minutes on a laptop before they report healthy.
log "Starting the services and waiting until they are healthy..."
if ! compose "${profiles[@]}" up -d --wait --wait-timeout 600; then
    compose "${profiles[@]}" ps --format 'table {{.Service}}\t{{.Status}}'
    die "some services did not become healthy: inspect them with 'docker compose logs <service>'."
fi

compose "${profiles[@]}" ps --format 'table {{.Service}}\t{{.Status}}'
cat <<EOF

The pipeline is running.
  Airflow     http://localhost:8080
  Grafana     $(grafana_url)   (user: $(env_value GRAFANA_ADMIN_USER | sed 's/^$/admin/'), password in .env)
  Prometheus  http://localhost:9090
  Spark       http://localhost:8081 (cluster)  http://localhost:4040 (streaming application)

Next: bash scripts/generate_data.sh --events 5000
EOF

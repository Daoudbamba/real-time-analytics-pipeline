#!/usr/bin/env bash
# Stops every container of the project. Data is kept unless --volumes is given.
# Usage: bash scripts/stop_pipeline.sh [--volumes [--yes]]
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

remove_volumes=false
confirmed=false
for argument in "$@"; do
    case "$argument" in
        --volumes) remove_volumes=true ;;
        --yes) confirmed=true ;;
        -h | --help) sed -n '2,3p' "$0"; exit 0 ;;
        *) die "unknown option $argument (see --help)" ;;
    esac
done

require_docker

if ! $remove_volumes; then
    compose --profile producer down
    log "Stopped. Kafka, the Delta tables, Airflow and Grafana keep their data."
    exit 0
fi

if ! $confirmed; then
    read -r -p "Delete Kafka topics, Delta tables, Airflow history and metrics? Type 'yes': " answer
    [[ "$answer" == "yes" ]] || die "cancelled, nothing was deleted."
fi
compose --profile producer down --volumes
log "Stopped and every volume of the project was deleted."

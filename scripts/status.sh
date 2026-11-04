#!/usr/bin/env bash
# Shows the state of every service and the Prometheus alerts currently firing.
# Usage: bash scripts/status.sh
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

require_docker
compose --profile producer ps --all --format 'table {{.Service}}\t{{.Status}}'

echo
if ! alerts="$(curl --silent --fail --max-time 5 --get 'http://localhost:9090/api/v1/query' \
    --data-urlencode 'query=ALERTS{alertstate="firing"}')"; then
    log "Prometheus is not reachable: alerts unknown."
    exit 0
fi
# Plain text parsing: the host may not have jq or Python.
firing="$(printf '%s' "$alerts" | grep -o '"alertname":"[^"]*"' \
    | sed 's/"alertname":"\([^"]*\)"/\1/' | sort | uniq -c || true)"
if [[ -z "$firing" ]]; then
    log "No alert firing."
else
    log "Alerts firing:"
    printf '%s\n' "$firing"
fi

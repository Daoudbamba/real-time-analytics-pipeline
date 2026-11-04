#!/usr/bin/env bash
# Sends generated e-commerce events to Kafka.
#   bash scripts/generate_data.sh --events 5000 [--rate 100] [--seed 42]   a fixed number, then stops
#   bash scripts/generate_data.sh --continuous [--rate 50]                 in the background, until --stop
#   bash scripts/generate_data.sh --stop                                   stops the background producer
# The invalid, duplicate and late ratios come from .env (PRODUCER_*_RATIO).
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

mode=""
events=""
rate=""
seed=""
while (($#)); do
    case "$1" in
        --events) mode=bounded; events="${2:-}"; shift ;;
        --continuous) mode=continuous ;;
        --stop) mode=stop ;;
        --rate) rate="${2:-}"; shift ;;
        --seed) seed="${2:-}"; shift ;;
        -h | --help) sed -n '2,6p' "$0"; exit 0 ;;
        *) die "unknown option $1 (see --help)" ;;
    esac
    shift
done

is_positive_integer() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
[[ -n "$mode" ]] || die "choose --events N, --continuous or --stop (see --help)"
[[ -z "$rate" || "$rate" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--rate must be a positive number"
[[ -z "$seed" ]] || is_positive_integer "$seed" || die "--seed must be a positive integer"

require_docker

case "$mode" in
    bounded)
        is_positive_integer "$events" || die "--events must be a positive integer"
        arguments=(--max-events "$events")
        [[ -z "$rate" ]] || arguments+=(--events-per-second "$rate")
        [[ -z "$seed" ]] || arguments+=(--seed "$seed")
        log "Sending $events events..."
        # --use-aliases keeps the name 'producer', so Prometheus scrapes this run too.
        compose --profile producer run --rm --use-aliases producer \
            python -m producers.kafka_producer "${arguments[@]}"
        log "Done. Bronze shows them after the next micro-batch (STREAMING_TRIGGER_INTERVAL)."
        ;;
    continuous)
        [[ -z "$seed" ]] || die "--seed only applies with --events"
        log "Starting the producer in the background..."
        PRODUCER_EVENTS_PER_SECOND="${rate:-$(env_value PRODUCER_EVENTS_PER_SECOND | sed 's/^$/50/')}" \
            compose --profile producer up -d --wait producer
        log "Running. Follow it with 'docker compose logs -f producer', stop it with --stop."
        ;;
    stop)
        compose --profile producer stop producer
        log "Producer stopped."
        ;;
esac

#!/usr/bin/env bash
# Starts Kafka if needed, creates the topics of config/kafka/topics.yaml, then lists them.
# Safe to run repeatedly: existing topics are left as they are.
# Usage: bash scripts/create_topics.sh
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

require_docker
log "Starting Kafka..."
compose up -d --wait kafka
log "Creating the topics..."
compose run --rm kafka-init
log "Topics on the broker:"
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe \
    | grep -E '^Topic:' | grep -v '__consumer_offsets' | awk '{printf "  %-20s partitions=%s replication=%s\n", $2, $6, $8}'

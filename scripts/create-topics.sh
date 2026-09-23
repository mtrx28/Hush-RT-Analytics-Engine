#!/usr/bin/env bash
# Creates the single Kafka topic Hush's whole pipeline reads and writes.
# Run automatically by the `kafka-init` compose service, or manually:
#   KAFKA_BOOTSTRAP=localhost:9092 scripts/create-topics.sh
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

echo "waiting for kafka at ${BOOTSTRAP}..."
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

"${KAFKA_BIN}/kafka-topics.sh" --create --if-not-exists \
  --bootstrap-server "${BOOTSTRAP}" \
  --topic events \
  --partitions 12 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config min.insync.replicas=1

echo "topic 'events' ready:"
"${KAFKA_BIN}/kafka-topics.sh" --describe --bootstrap-server "${BOOTSTRAP}" --topic events

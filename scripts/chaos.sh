#!/usr/bin/env bash
# Runs loadgen against a live stack while randomly killing aggregator
# containers, then reconciles Postgres against loadgen's ground truth.
# Proves the exactly-once design survives crashes and rebalances.
#
# Usage: scripts/chaos.sh [duration_secs] [events_per_sec]
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${1:-180}"
RATE="${2:-300}"
COMPOSE="docker compose -f deploy/docker-compose.yml"

echo "== scaling aggregator to 3 replicas =="
$COMPOSE up -d --scale aggregator=3

echo "== starting loadgen for ${DURATION}s at ${RATE}/s =="
docker run --rm --network hush_default \
  -v "$(pwd):/work" -w /work \
  -e GATEWAY_URL=http://hush-ingest-gateway:8080/v1/events \
  hush-dev \
  cargo run --release -p loadgen -- \
    --duration-secs "${DURATION}" --events-per-sec "${RATE}" \
    --truth-file /work/truth.json &
LOADGEN_PID=$!

echo "== chaos: killing a random aggregator every 20-60s =="
END=$((SECONDS + DURATION))
while [ $SECONDS -lt $END ]; do
  sleep $((RANDOM % 40 + 20))
  VICTIM=$($COMPOSE ps -q aggregator | shuf -n 1 || true)
  if [ -n "${VICTIM:-}" ]; then
    echo "killing aggregator container ${VICTIM}"
    docker kill "${VICTIM}" || true
    # compose restart policy (or a fresh `up -d`) brings a replacement back;
    # this forces the consumer group to rebalance.
    $COMPOSE up -d --scale aggregator=3
  fi
done

wait "${LOADGEN_PID}"

echo "== waiting for aggregators to catch up and flush =="
sleep 15

echo "== reconciling =="
export DATABASE_URL="postgres://hush:hush@localhost:5432/hush"
docker run --rm --network hush_default \
  -v "$(pwd):/work" -w /work \
  -e DATABASE_URL="postgres://hush:hush@hush-postgres:5432/hush" \
  hush-dev \
  cargo run --release -p reconcile -- --truth-file /work/truth.json

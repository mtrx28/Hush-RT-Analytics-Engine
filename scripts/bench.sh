#!/usr/bin/env bash
# Runs loadgen against a live ingest-gateway and prints the p50/p95/p99/max
# latency summary loadgen logs at the end. Used to produce the before/after
# numbers in docs/benchmarks.md when tuning linger.ms / batch.size / JSON
# vs a binary wire format.
#
# Usage: scripts/bench.sh [events_per_sec] [duration_secs]
set -euo pipefail
cd "$(dirname "$0")/.."

RATE="${1:-500}"
DURATION="${2:-60}"

scripts/dev-cargo.sh run --release -p loadgen -- \
  --gateway-url "${GATEWAY_URL:-http://localhost:8080/v1/events}" \
  --events-per-sec "${RATE}" \
  --duration-secs "${DURATION}" \
  --truth-file /tmp/bench-truth.json

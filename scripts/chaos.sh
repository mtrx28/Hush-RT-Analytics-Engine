#!/usr/bin/env bash
# Runs loadgen against a live stack while randomly killing aggregator
# containers, then reconciles Postgres against loadgen's ground truth.
# Proves the exactly-once design survives crashes and rebalances.
#
# Usage: scripts/chaos.sh [duration_secs] [events_per_sec]
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
# On Windows/git-bash, `pwd` returns a POSIX-style path that Docker Desktop
# doesn't always translate correctly for -v; `pwd -W` (MSYS only) gives the
# native Windows path instead. Falls back to plain `pwd` elsewhere.
WORKDIR="$(pwd -W 2>/dev/null || pwd)"

DURATION="${1:-180}"
RATE="${2:-300}"
COMPOSE="docker compose -f deploy/docker-compose.yml"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-hush}"

# Marks an annotation at the current instant on the "Hush Consumer View"
# dashboard (any panel querying the "chaos-kill" tag picks it up — see
# deploy/grafana/dashboards/hush-consumer-view.json). Best-effort: a
# Grafana hiccup shouldn't fail the chaos run itself.
annotate_kill() {
  local text="$1"
  local now_ms
  now_ms=$(( $(date +%s%N) / 1000000 ))
  curl -s -u "admin:${GRAFANA_ADMIN_PASSWORD}" -X POST "${GRAFANA_URL}/api/annotations" \
    -H "Content-Type: application/json" \
    -d "{\"tags\":[\"chaos-kill\"],\"text\":\"${text}\",\"time\":${now_ms}}" \
    >/dev/null || echo "warning: failed to post Grafana annotation (continuing)"
}

DEVRUN=(docker run --rm --network hush_default
  -v "${WORKDIR}:/work"
  -v hush-cargo-registry:/usr/local/cargo/registry
  -v hush-cargo-target:/work/target
  -w /work)

# Explicitly scoped to the services the controlled test actually needs.
# `up -d` with no service names starts *everything* in the compose file,
# including wiki-source — which, now that its User-Agent bug is fixed,
# successfully ingests real Wikipedia traffic. That traffic lands in the
# same cells loadgen's synthetic events do, breaking reconcile's exact
# comparison against loadgen's own ground truth (observed directly: a run
# with wiki-source left running produced 75 mismatches / 860 unexpected
# cells, all real Wikipedia wikis/namespaces loadgen never generates, not
# a real correctness regression). wiki-source is validated separately.
SERVICES="kafka postgres ingest-gateway aggregator query-api prometheus grafana"

echo "== scaling aggregator to 3 replicas =="
$COMPOSE up -d --scale aggregator=3 ${SERVICES}

echo "== starting loadgen for ${DURATION}s at ${RATE}/s =="
"${DEVRUN[@]}" \
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
    VICTIM_NAME=$(docker inspect --format='{{.Name}}' "${VICTIM}" | sed 's#^/##')
    echo "killing aggregator container ${VICTIM_NAME}"
    docker kill "${VICTIM}" || true
    annotate_kill "killed ${VICTIM_NAME}"
    # compose restart policy (or a fresh `up -d`) brings a replacement back;
    # this forces the consumer group to rebalance.
    $COMPOSE up -d --scale aggregator=3 ${SERVICES}
  fi
done

wait "${LOADGEN_PID}"

echo "== waiting for aggregators to catch up and flush =="
# The tail window can take up to WINDOW_SIZE (60s) + ALLOWED_LATENESS (30s)
# + the idle-check interval (~1s) to close after the last event, plus one
# flush cycle — 100s covers that with margin. Reconciling any earlier will
# report the still-open tail window's cells as "missing", which is a
# harmless timing artifact, not real drift (verified manually: re-running
# reconcile after the wait shows zero drift).
sleep 100

echo "== reconciling =="
"${DEVRUN[@]}" \
  -e DATABASE_URL="postgres://hush:hush@hush-postgres:5432/hush" \
  hush-dev \
  cargo run --release -p reconcile -- --truth-file /work/truth.json

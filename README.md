# Hush

A privacy-first, real-time analytics engine in Rust, built over Wikimedia's
live global edit stream. Hush aggregates a high-volume event stream in real
time and never releases a number that describes fewer than `k` distinct
people — raw per-user events are kept for 24 hours (Kafka retention) and
never stored permanently; only aggregates land in Postgres.

See [docs/design.md](docs/design.md) for the architecture, and
[docs/decisions.md](docs/decisions.md) for the reasoning behind the major
choices (why Kafka, why offsets live in Postgres, why 12 partitions, etc.).

## Quickstart

Rust/cargo commands run inside a Linux dev container
(`deploy/Dockerfile.dev`), so `rdkafka`'s C dependency links against a real
`librdkafka` without needing MSVC + vcpkg on Windows — and it matches the
Linux target the services actually ship on.

```bash
# build the dev image once
docker build -t hush-dev -f deploy/Dockerfile.dev .

# build & test the whole workspace
scripts/dev-cargo.sh check --workspace
scripts/dev-cargo.sh test --workspace
```

Bring up the stack:

```bash
export PSEUDONYM_SALT=change-me-to-something-secret
docker compose -f deploy/docker-compose.yml up -d kafka postgres
docker compose -f deploy/docker-compose.yml up kafka-init      # creates the `events` topic
docker compose -f deploy/docker-compose.yml up -d ingest-gateway aggregator query-api prometheus grafana
```

Feed it real Wikipedia traffic:

```bash
docker compose -f deploy/docker-compose.yml up -d wiki-source
```

...or synthetic traffic instead:

```bash
scripts/dev-cargo.sh run --release -p loadgen -- --events-per-sec 200 --duration-secs 120
```

Query it once a window has flushed:

```bash
curl 'http://localhost:8081/v1/windows/latest'
curl 'http://localhost:8081/v1/stats?window=2026-09-23T10:15:00Z&level=1'
```

Grafana is at `http://localhost:3000` (anonymous viewer access enabled),
Prometheus at `http://localhost:9090`.

Scale the aggregator to watch a Kafka consumer-group rebalance happen:

```bash
docker compose -f deploy/docker-compose.yml up -d --scale aggregator=3
```

Run the chaos test (kills aggregator containers under load, then reconciles
Postgres against loadgen's ground truth — the pass criterion is zero drift):

```bash
scripts/chaos.sh 180 300   # 180s, 300 events/sec
```

## Privacy: known limitation

`query-api`'s secondary suppression (the differencing-attack defense) sums
hidden children's user counts directly to decide whether the "Other" bucket
clears the `k` threshold. A user who appears in more than one hidden child
is counted once per child there, so that check is an overestimate of the
true distinct count — approximate, not exact. Calibrated noise
(differential privacy) is the principled fix and is tracked as a stretch
goal; see `crates/query-api/src/privacy.rs` for the full explanation and
`docs/decisions.md` for why thresholds shipped first.

## Repo layout

```
hush/
├── Cargo.toml                 (workspace)
├── crates/
│   ├── common/                (Event, hierarchy, config, pseudonymization, window/watermark)
│   ├── wiki-source/
│   ├── ingest-gateway/
│   ├── aggregator/            (window.rs, db.rs, rebalance.rs, lag.rs, flush.rs)
│   ├── query-api/             (privacy.rs)
│   ├── loadgen/
│   └── reconcile/
├── migrations/                (sqlx migrations)
├── deploy/                    (docker-compose.yml, Dockerfiles, Prometheus, Grafana)
├── scripts/                   (create-topics.sh, chaos.sh, bench.sh, dev-cargo.sh)
└── docs/                      (design.md, decisions.md, db-optimization.md, benchmarks.md)
```

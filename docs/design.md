# Hush: design overview

Hush is a privacy-first, real-time analytics engine over Wikimedia's live
edit stream. It ingests a high-volume event stream, aggregates it in real
time, and never releases a number that describes fewer than `k` distinct
people. Raw per-user events are never stored beyond Kafka's 24h retention;
Postgres only ever holds aggregates.

## Architecture

```
 Wikimedia live stream ──► wiki-source ─┐
                                        ├──► ingest-gateway ──► Kafka (KRaft)
 loadgen (synthetic, Zipf) ─────────────┘     (HTTP, validate,     topic: events
                                               pseudonymize,       12 partitions,
                                               backpressure)       key = hashed user
                                                                        │
                                                                        ▼
                                                              aggregator (×N instances,
                                                              one consumer group)
                                                              windows + dedup in memory
                                                                        │ one transaction:
                                                                        │ aggregates + offsets
                                                                        ▼
                                                                   PostgreSQL
                                                                        │
                                                                        ▼
                                                   query-api ──► privacy layer ──► JSON
                                                   (k-threshold, suppression, cache)
```

Five binaries in one Cargo workspace (`crates/`), sharing a `common` crate
for the `Event` type, the 4-level dimension hierarchy, HMAC pseudonymization,
and window/watermark constants:

- **wiki-source** — consumes Wikimedia's `recentchange` Server-Sent Events
  stream, maps each message to `common::Event`, batches, and POSTs to the
  gateway. Reconnects with exponential backoff, resuming via `Last-Event-ID`.
- **ingest-gateway** — validates and pseudonymizes events (HMAC-SHA256 under
  a secret salt — never a plain hash, which would be dictionary-attackable),
  then produces them to Kafka keyed by the pseudonym. Backpressures with 503
  once its internal channel to the Kafka producer is full.
- **aggregator** — the core. Consumes the `events` topic in a consumer
  group, maintains 1-minute tumbling windows with a per-window dedup set,
  advances a watermark per partition, and flushes closed windows plus its
  own offset checkpoint to Postgres in one transaction. See
  `docs/decisions.md` for why offsets live in Postgres instead of Kafka.
- **query-api** — serves `GET /v1/stats` and `GET /v1/windows/latest`,
  applying k-anonymity suppression (`crates/query-api/src/privacy.rs`)
  before any number leaves the process, with a cache for already-flushed
  (and therefore immutable) windows.
- **loadgen** — generates a deterministic, Zipf-skewed synthetic event
  stream with injected duplicates and (bounded) late events, records
  latency with an open-loop schedule and an HdrHistogram, and writes
  `truth.json` — the exact expected aggregate counts — for `reconcile` to
  check the live pipeline against.
- **reconcile** — compares Postgres's actual aggregates against
  `truth.json` and reports drift; the pass criterion is zero drift.

## The privacy model

Every event contributes to 4 cells, one per level of a fixed hierarchy:

```
Level 0: global
Level 1: family                 (wikipedia)
Level 2: family / wiki          (wikipedia / knwiki)
Level 3: family / wiki / ns     (wikipedia / knwiki / 0)
```

Because Kafka messages are keyed by the pseudonymized user, a user's events
always land in the same partition — which means the `users` sets different
partitions accumulate for the same cell never overlap, so summing them
across partitions at query time gives an **exact** distinct-user count with
no HyperLogLog or cross-partition merge needed.

`query-api` never returns a cell whose distinct-user count is below `k`
(default 10). It also defends against differencing attacks: if hiding one
or more children would leave a released "Other" bucket smaller than `k`
itself, more children are folded in until it isn't. See
`crates/query-api/src/privacy.rs` for the exact algorithm, its tests
(including a differencing-attack case), and the documented approximation in
its residual accounting.

## Exactly-once aggregation

Kafka transactions only give exactly-once when both the input and the
output are Kafka topics; Hush's output is Postgres. Instead, `aggregator`
writes every closed window's aggregates **and** its own progress checkpoint
(`committed_offset`, `flushed_watermark`) in a single Postgres transaction
(`crates/aggregator/src/db.rs::flush_windows`). On crash or rebalance, the
consumer seeks back to the last committed offset and replays — deterministic
because Kafka preserves per-partition order and messages are keyed so a
user's duplicates and retries land on the same partition as the original.
See `docs/decisions.md` for the full reasoning and the idle-partition edge
case (a partition with no traffic has its watermark advanced by wall-clock
time instead, so its windows still close).

## Running it

```bash
docker compose -f deploy/docker-compose.yml up -d kafka postgres
docker compose -f deploy/docker-compose.yml up kafka-init   # creates the events topic
docker compose -f deploy/docker-compose.yml up -d ingest-gateway aggregator query-api prometheus grafana
docker compose -f deploy/docker-compose.yml up -d wiki-source   # or run loadgen instead, see below
```

To use synthetic traffic instead of the live Wikimedia stream:

```bash
scripts/dev-cargo.sh run --release -p loadgen -- --events-per-sec 200 --duration-secs 120
```

Query it once a window has flushed (`GET /v1/windows/latest` tells you the
newest one):

```bash
curl 'http://localhost:8081/v1/windows/latest'
curl 'http://localhost:8081/v1/stats?window=2026-09-23T10:15:00Z&level=1'
```

Rust/cargo runs against a Linux dev container (`deploy/Dockerfile.dev`, via
`scripts/dev-cargo.sh`) rather than the host toolchain directly, so
`rdkafka`'s C dependency links against a real `librdkafka` without fighting
MSVC — this also matches the Linux target the services actually ship on.

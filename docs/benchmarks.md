# Benchmarks

Latency numbers below come from `loadgen`'s open-loop HdrHistogram summary
(`scripts/bench.sh`), measuring the round trip from `loadgen` to
`ingest-gateway`'s `POST /v1/events`. Open-loop scheduling means a slow
period is measured honestly instead of being smoothed away by the client
waiting for each response before sending the next (coordinated omission).

Run a benchmark:

```bash
docker compose -f deploy/docker-compose.yml up -d kafka postgres ingest-gateway
scripts/bench.sh 500 60     # 500 events/sec for 60s
```

## Tuning `linger.ms` / `batch.size`

`ingest-gateway`'s Kafka producer settings
(`crates/ingest-gateway/src/producer.rs`) trade a little latency for a lot
more throughput by batching:

| linger.ms | batch.size | p50 | p99 | notes |
|-----------|-----------|-----|-----|-------|
| (fill in) | (fill in) | (fill in) | (fill in) | baseline |
| 5 | 262144 | (fill in) | (fill in) | current default |

Fill this table in by running `scripts/bench.sh` against each configuration
(override via `KAFKA_BROKERS`-style env vars on `ingest-gateway`, or edit
`producer.rs` locally and rebuild) at a fixed load level, high enough to
actually fill batches (a few hundred events/sec or more — at very low rates
`linger.ms` dominates and batch size barely matters).

## JSON vs. a compact binary wire format

Events are currently JSON on the wire and on the Kafka topic — easy to
inspect while debugging, at the cost of encode/decode overhead and message
size. Switching to a compact binary format (Protobuf or `bincode`) is a
measurable optimization:

| format | encode+decode time | wire bytes/event | notes |
|--------|--------------------|--------------------|-------|
| JSON (current) | (fill in) | (fill in) | baseline |
| bincode | (fill in) | (fill in) | stretch goal |

## Query-api latency with and without the cache

`query-api` caches answers for already-flushed windows (moka,
`crates/query-api/src/main.rs`), since a flushed window's answer never
changes.

| scenario | p50 | p99 |
|----------|-----|-----|
| cold cache | (fill in) | (fill in) |
| warm cache | (fill in) | (fill in) |

Measure with `curl -w '%{time_total}\n'` in a loop against the same
`window`/`level`/`family` query, or with a small script using `hyperfine`.

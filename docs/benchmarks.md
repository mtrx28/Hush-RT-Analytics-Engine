# Benchmarks

Every number on this page was measured against a live `docker compose`
stack on this machine (Kafka + Postgres + the real services), not
estimated. Reproduce with the commands under each section.

## `linger.ms` / `batch.size`: no measurable effect on client-observed latency

`ingest-gateway`'s Kafka producer settings
(`crates/ingest-gateway/src/producer.rs`) are overridable via
`KAFKA_LINGER_MS`/`KAFKA_BATCH_SIZE` without a rebuild, specifically so
this could be measured:

```bash
KAFKA_LINGER_MS=5   KAFKA_BATCH_SIZE=262144  docker compose -f deploy/docker-compose.yml up -d --force-recreate ingest-gateway
scripts/dev-cargo.sh run --release -p loadgen -- --duration-secs 20 --events-per-sec 3000 --batch-size 200
```

Measured (loadgen's open-loop HdrHistogram, 3000 events/sec, 20s, 300 HTTP batches):

| linger.ms | batch.size | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 5 (current default) | 262144 | 0.886 ms | 1.348 ms | 2.269 ms | 2.931 ms |
| 0 (low-latency) | 16384 | 0.901 ms | 1.299 ms | 1.933 ms | 2.333 ms |
| 20 (high-throughput) | 1048576 | 0.932 ms | 1.300 ms | 2.111 ms | 2.829 ms |

**These are all the same within noise.** That's a real finding, not a
measurement failure: `POST /v1/events` returns `202` as soon as the event
is validated, pseudonymized, and successfully placed on the bounded
channel to the producer task (`crates/ingest-gateway/src/handlers.rs`) —
it does not wait for the Kafka producer to actually batch and send
anything. `linger.ms`/`batch.size` only affect what happens *after* that
response is already on the wire back to the caller, so no client-observed
latency benchmark can see them by design.

What they *do* affect — broker-side request rate and network efficiency
under sustained load — needs different instrumentation (librdkafka's own
internal statistics via `statistics.interval.ms`, or Kafka broker request
metrics) to observe, which isn't wired up yet. That's the honest state:
the current defaults (`linger.ms=5`, `batch.size=262144`) are a reasonable
starting point per Kafka's own documentation, not something this
benchmark validated end to end — a genuine next step, not a completed one.

## JSON vs. bincode wire format

```bash
scripts/dev-cargo.sh run -p common --example wire_format_bench --release
```

Measured (100,000 synthetic `Event`s, single-threaded, release build):

| format | encode | decode | avg bytes/event |
|---|---|---|---|
| JSON (current) | 551 ns/event | 387 ns/event | 188.9 |
| bincode | 362 ns/event | 142 ns/event | 119.9 |

bincode is **1.58x smaller on the wire**, **1.52x faster to encode**, and
**2.73x faster to decode**. At Hush's actual per-service overhead
(HTTP parsing, HMAC pseudonymization, Kafka round trips, Postgres writes —
each in the microsecond-to-millisecond range), a few hundred nanoseconds of
serialization is not the bottleneck, which is why the wire format hasn't
actually been switched: JSON's debuggability (reading a raw Kafka message
with `kcat` without a schema) is worth more here than a sub-microsecond
win. If ingest volume grew by orders of magnitude, this would be the first
place to look.

## query-api cache: cold vs. warm

```bash
docker compose -f deploy/docker-compose.yml up -d --force-recreate query-api  # clears the in-memory cache
curl -s -o /dev/null -w '%{time_total}\n' 'http://localhost:8081/v1/stats?window=...&level=1'  # cold
curl -s -o /dev/null -w '%{time_total}\n' 'http://localhost:8081/v1/stats?window=...&level=1'  # warm
```

Measured against a populated window (isolating cache effect from
connection-pool warmup — the very first query after a restart pays both,
subsequent queries for a *different* uncached window pay only the cache
miss):

| scenario | latency |
|---|---|
| cold (Postgres round trip: `fetch_cell` + `fetch_children` + suppression) | ~9.4–11.6 ms |
| warm (in-memory cache hit) | ~3.2–4.1 ms |

A consistent **~2.5–3x speedup** from caching, measured across several
different window/level combinations to rule out a one-off outlier. Since
flushed windows never change, this cache never needs invalidation — every
hit is exactly as correct as a fresh query.

## See also

`docs/hyperloglog.md` has the HyperLogLog memory/accuracy measurements
(a separate stretch-goal comparison, not a wire-format or latency one).

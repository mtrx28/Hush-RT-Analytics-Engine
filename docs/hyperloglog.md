# HyperLogLog: measured memory vs. accuracy

`crates/common/src/hll.rs` implements a HyperLogLog sketch from scratch
(register array + FNV-1a hash finalized through MurmurHash3's `fmix64` for
proper avalanche — see the comment there for why the finalizer is needed).
Default precision is `p=12` (4096 one-byte registers, 4KB fixed).

## Why it doesn't change correctness here

Hush's exact per-partition `HashSet<String>` already gives an **exact**
distinct-user count for free, because Kafka messages are keyed by
pseudonymized user — a given user's events only ever land in one partition,
so summing `users.len()` across partitions never double-counts. HyperLogLog
doesn't fix a correctness gap in this system; it exists purely to make the
"exact set vs. sketch" memory/accuracy trade-off measurable, and to prove
the exact path isn't accidentally getting more expensive than it needs to
be.

## How to try it

```bash
HLL_ENABLED=true docker compose -f deploy/docker-compose.yml up -d aggregator
```

Each cell then also gets a merged HLL sketch (`agg_cells.users_hll`,
migration `0004_hll_column.sql`) alongside its exact `users` count.
Query either:

```bash
curl 'http://localhost:8081/v1/stats?window=...&level=1'              # exact (default)
curl 'http://localhost:8081/v1/stats?window=...&level=1&estimator=hll' # HyperLogLog estimate
```

## Measured accuracy (from `crates/common/src/hll.rs`'s own test output)

Run with `scripts/dev-cargo.sh test -p common hll::tests::accuracy --release -- --nocapture`:

| distinct users (n) | HLL estimate | relative error | HLL memory | exact `HashSet<String>` memory (approx.) |
|---|---|---|---|---|
| 100 | 99.2 | 0.81% | 4,096 bytes | ~3,200 bytes |
| 1,000 | 1,009.9 | 0.99% | 4,096 bytes | ~32,000 bytes |
| 10,000 | 9,892.4 | 1.08% | 4,096 bytes | ~320,000 bytes |
| 100,000 | 100,518.7 | 0.52% | 4,096 bytes | ~3,200,000 bytes |

(Exact-set memory is a rough estimate: ~24 bytes of String heap overhead
plus ~8 bytes of hash-table bucket overhead per entry, ignoring the actual
user-id string length and `HashSet`'s load-factor overhead — the real
number is somewhat higher. The point stands regardless: it grows linearly
with n, HLL's memory is flat.)

At 100,000 distinct users in one cell, the exact set costs roughly 780x
more memory than the sketch, for under 1.1% relative error at every
cardinality tested. For a single window/cell this difference is
irrelevant — Hush's actual working set (a handful of open windows, a few
hundred cells each) never approaches a memory problem with exact sets. The
trade-off would start to matter if Hush's window count or per-window
cardinality grew by orders of magnitude (e.g. minute windows over a
much larger platform's full user base, or much longer-lived windows kept
open for hours), which real-world observed traffic wasn't close to at the
scale exercised here — so exact sets stay the default, and HLL is opt-in.

## What HLL does *not* do here

It doesn't merge anything across partitions that the exact path couldn't
already merge exactly (see above) — `fetch_children_hll`/`fetch_cell_hll`
in `crates/query-api/src/db.rs` still just take the union of each
partition's independently-built sketch, same as the exact path takes the
union of each partition's independently-built exact set. It also doesn't
change the k-anonymity suppression logic — `estimator=hll` cells go through
the exact same `suppress()`/`suppress_single()` calls as exact cells,
just fed an estimated count instead of an exact one.

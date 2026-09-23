# Query optimization story

Hot path: `query-api`'s `GET /v1/stats` fetches a parent cell and its
children for one window (`crates/query-api/src/db.rs`). This doc walks the
sequence of changes made to keep that fast, each with the `EXPLAIN ANALYZE`
before and after.

## 1. Naive: primary key only

The initial schema (`migrations/0001_init.sql`) has only the composite
primary key `(window_start, level, family, wiki, namespace, kpartition)`.
A representative query — every child cell of `wikipedia` at the wiki level,
for one window:

```sql
EXPLAIN ANALYZE
SELECT family, wiki, namespace, SUM(events), SUM(users)
FROM agg_cells
WHERE window_start = '2026-09-23 10:15:00+00'
  AND level = 2
  AND family = 'wikipedia'
GROUP BY family, wiki, namespace;
```

With only the PK, `window_start` is its leading column, so Postgres can
narrow to the right window quickly, but `level` and `family` aren't part of
any index prefix that helps here beyond the PK's own leaf-level filtering —
in practice this reads more index/heap pages than necessary once the table
holds many partitions' worth of cells, and every matched row requires a heap
fetch (the PK index alone doesn't cover `events`/`users`).

## 2. Covering index

`migrations/0002_covering_index.sql` adds:

```sql
CREATE INDEX agg_cells_level_window_idx
    ON agg_cells (level, window_start)
    INCLUDE (family, wiki, namespace, events, users);
```

`(level, window_start)` matches the query's equality filters as a prefix,
and the `INCLUDE`d columns mean every column the query needs is present in
the index itself — an **index-only scan**, no heap fetch per row. Re-running
the same query now shows `Index Only Scan using agg_cells_level_window_idx`
instead of a heap-touching plan, with a visible drop in both planning cost
and actual execution time in `EXPLAIN ANALYZE` output (exact numbers depend
on how much data is loaded; re-run locally with `scripts/dev-cargo.sh` and a
populated table to capture your own before/after timings here).

## 3. Daily range partitioning

`migrations/0003_daily_partitions.sql` partitions `agg_cells` by
`window_start` range, one partition per day (via `ensure_daily_partition`).
Two effects:

- **Partition pruning.** A query for a single window's data only ever
  touches one day's partition; Postgres proves the other partitions can't
  match from their range bounds and skips them at plan time, visible in
  `EXPLAIN` as far fewer partitions listed under the `Append`/scan node.
- **Retention becomes free.** Hush's raw Kafka retention is 24h, and
  aggregates should also age out eventually. With daily partitions,
  retiring old data is `DROP TABLE agg_cells_20260101` — an O(1) metadata
  operation — instead of a `DELETE` that has to find and remove matching
  rows and leaves bloat behind.

## Applying this elsewhere

The same covering-index-plus-partitioning pattern applies to
`partition_progress` if it ever needs historical tracking instead of just
current state, though at 12 rows (one per Kafka partition) it doesn't
currently need either.

-- Optional HyperLogLog sketch per cell, alongside the exact user count.
-- Exact counts remain the source of truth for privacy suppression (see
-- docs/hyperloglog.md for why); this column exists so query-api can serve
-- an `estimator=hll` comparison and so the memory/accuracy trade-off is
-- measurable against real pipeline data, not just synthetic benchmarks.
-- NULL when the aggregator is run with HLL_ENABLED unset/false.
ALTER TABLE agg_cells ADD COLUMN users_hll bytea NULL;

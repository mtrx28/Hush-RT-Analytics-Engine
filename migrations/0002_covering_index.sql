-- Index-only scan support for query-api's hot path: "give me events/users
-- for this level as of this window". See docs/db-optimization.md for the
-- EXPLAIN ANALYZE before/after this was added.
CREATE INDEX agg_cells_level_window_idx
    ON agg_cells (level, window_start)
    INCLUDE (family, wiki, namespace, events, users);

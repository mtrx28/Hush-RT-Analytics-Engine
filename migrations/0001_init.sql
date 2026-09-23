CREATE TABLE agg_cells (
    window_start  timestamptz NOT NULL,
    level         smallint    NOT NULL,       -- 0..3
    family        text        NOT NULL,       -- '*' when rolled up
    wiki          text        NOT NULL,       -- '*' when rolled up
    namespace     int         NOT NULL,       -- -1 when rolled up
    kpartition    int         NOT NULL,       -- Kafka partition that produced the row
    events        bigint      NOT NULL,
    users         bigint      NOT NULL,       -- distinct users within this partition
    PRIMARY KEY (window_start, level, family, wiki, namespace, kpartition)
) PARTITION BY RANGE (window_start);

-- A generous initial set of daily partitions; scripts/create-topics.sh or an
-- ops cron is expected to roll these forward. See docs/db-optimization.md.
CREATE TABLE agg_cells_default PARTITION OF agg_cells DEFAULT;

CREATE TABLE partition_progress (
    kpartition         int PRIMARY KEY,
    committed_offset   bigint      NOT NULL,
    flushed_watermark  timestamptz NOT NULL
);

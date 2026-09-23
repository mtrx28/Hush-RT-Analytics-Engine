-- Daily range partitions let a query for one window prune to a single
-- partition, and let retention become DROP TABLE instead of a slow DELETE.
-- The DEFAULT partition from 0001 stays as a catch-all for any window that
-- doesn't yet have a dedicated daily partition.
CREATE OR REPLACE FUNCTION ensure_daily_partition(for_day date) RETURNS void AS $$
DECLARE
    partition_name text := 'agg_cells_' || to_char(for_day, 'YYYYMMDD');
    range_start timestamptz := for_day::timestamptz;
    range_end timestamptz := (for_day + 1)::timestamptz;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF agg_cells FOR VALUES FROM (%L) TO (%L)',
            partition_name, range_start, range_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT ensure_daily_partition(CURRENT_DATE - 1);
SELECT ensure_daily_partition(CURRENT_DATE);
SELECT ensure_daily_partition(CURRENT_DATE + 1);

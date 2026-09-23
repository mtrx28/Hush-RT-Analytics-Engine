use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::hierarchy::CellKey;
use common::HyperLogLog;
use sqlx::PgPool;

use crate::privacy::RawCell;

pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    Ok(PgPool::connect(database_url).await?)
}

/// The earliest point up to which *every* partition has flushed. A window
/// is only safe to serve once it ends at or before this instant — serving
/// earlier would let one slow partition make the whole window look lower
/// than it really is.
pub async fn min_flushed_watermark(pool: &PgPool) -> anyhow::Result<Option<DateTime<Utc>>> {
    let row: Option<(Option<DateTime<Utc>>,)> =
        sqlx::query_as("SELECT min(flushed_watermark) FROM partition_progress")
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(w,)| w))
}

/// Fetches one cell's totals, summed across Kafka partitions. Because a
/// user's events only ever land in one partition, summing `users` across
/// partitions is an exact distinct count, not an approximation.
pub async fn fetch_cell(
    pool: &PgPool,
    window_start: DateTime<Utc>,
    key: &CellKey,
) -> anyhow::Result<Option<RawCell>> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        r#"
        SELECT COALESCE(SUM(events), 0)::bigint, COALESCE(SUM(users), 0)::bigint
        FROM agg_cells
        WHERE window_start = $1 AND level = $2 AND family = $3 AND wiki = $4 AND namespace = $5
        "#,
    )
    .bind(window_start)
    .bind(key.level)
    .bind(&key.family)
    .bind(&key.wiki)
    .bind(key.namespace)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(events, users)| RawCell {
        family: key.family.clone(),
        wiki: key.wiki.clone(),
        namespace: key.namespace,
        events: events as u64,
        users: users as u64,
    }))
}

/// Fetches every direct child of `parent` at `parent.level + 1`, summed
/// across partitions and grouped by the dimension that varies at the
/// child level (family for level 1, wiki for level 2, namespace for level 3).
pub async fn fetch_children(
    pool: &PgPool,
    window_start: DateTime<Utc>,
    parent: &CellKey,
) -> anyhow::Result<Vec<RawCell>> {
    let child_level = parent.level + 1;
    let rows: Vec<(String, String, i32, i64, i64)> = match parent.level {
        0 => {
            sqlx::query_as(
                r#"
                SELECT family, '*' as wiki, -1 as namespace,
                       SUM(events)::bigint, SUM(users)::bigint
                FROM agg_cells
                WHERE window_start = $1 AND level = $2
                GROUP BY family
                "#,
            )
            .bind(window_start)
            .bind(child_level)
            .fetch_all(pool)
            .await?
        }
        1 => {
            sqlx::query_as(
                r#"
                SELECT family, wiki, -1 as namespace,
                       SUM(events)::bigint, SUM(users)::bigint
                FROM agg_cells
                WHERE window_start = $1 AND level = $2 AND family = $3
                GROUP BY family, wiki
                "#,
            )
            .bind(window_start)
            .bind(child_level)
            .bind(&parent.family)
            .fetch_all(pool)
            .await?
        }
        2 => {
            sqlx::query_as(
                r#"
                SELECT family, wiki, namespace,
                       SUM(events)::bigint, SUM(users)::bigint
                FROM agg_cells
                WHERE window_start = $1 AND level = $2 AND family = $3 AND wiki = $4
                GROUP BY family, wiki, namespace
                "#,
            )
            .bind(window_start)
            .bind(child_level)
            .bind(&parent.family)
            .bind(&parent.wiki)
            .fetch_all(pool)
            .await?
        }
        _ => Vec::new(),
    };

    Ok(rows
        .into_iter()
        .map(|(family, wiki, namespace, events, users)| RawCell {
            family,
            wiki,
            namespace,
            events: events as u64,
            users: users as u64,
        })
        .collect())
}

fn merge_hll_bytes(acc: &mut Option<HyperLogLog>, bytes: Option<Vec<u8>>) {
    let Some(bytes) = bytes else { return };
    let Some(sketch) = HyperLogLog::from_bytes(&bytes) else { return };
    match acc {
        Some(existing) => existing.merge(&sketch),
        None => *acc = Some(sketch),
    }
}

/// HyperLogLog counterpart to `fetch_cell`: sums `events` exactly as usual,
/// but estimates `users` by merging each partition's HLL sketch (`bytea`
/// stored in `users_hll`) instead of summing the exact per-partition
/// counts. Returns cells with `users = 0` wherever no sketch was ever
/// stored (i.e. the aggregator that wrote that row didn't have
/// `HLL_ENABLED=true`) — see `docs/hyperloglog.md`.
pub async fn fetch_cell_hll(
    pool: &PgPool,
    window_start: DateTime<Utc>,
    key: &CellKey,
) -> anyhow::Result<Option<RawCell>> {
    let rows: Vec<(i64, Option<Vec<u8>>)> = sqlx::query_as(
        r#"
        SELECT events, users_hll
        FROM agg_cells
        WHERE window_start = $1 AND level = $2 AND family = $3 AND wiki = $4 AND namespace = $5
        "#,
    )
    .bind(window_start)
    .bind(key.level)
    .bind(&key.family)
    .bind(&key.wiki)
    .bind(key.namespace)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let mut events_total: i64 = 0;
    let mut merged: Option<HyperLogLog> = None;
    for (events, hll_bytes) in rows {
        events_total += events;
        merge_hll_bytes(&mut merged, hll_bytes);
    }

    Ok(Some(RawCell {
        family: key.family.clone(),
        wiki: key.wiki.clone(),
        namespace: key.namespace,
        events: events_total as u64,
        users: merged.map(|h| h.estimate().round() as u64).unwrap_or(0),
    }))
}

/// HyperLogLog counterpart to `fetch_children`.
pub async fn fetch_children_hll(
    pool: &PgPool,
    window_start: DateTime<Utc>,
    parent: &CellKey,
) -> anyhow::Result<Vec<RawCell>> {
    let child_level = parent.level + 1;
    let rows: Vec<(String, String, i32, i64, Option<Vec<u8>>)> = match parent.level {
        0 => {
            sqlx::query_as(
                "SELECT family, wiki, namespace, events, users_hll FROM agg_cells WHERE window_start = $1 AND level = $2",
            )
            .bind(window_start)
            .bind(child_level)
            .fetch_all(pool)
            .await?
        }
        1 => {
            sqlx::query_as(
                "SELECT family, wiki, namespace, events, users_hll FROM agg_cells WHERE window_start = $1 AND level = $2 AND family = $3",
            )
            .bind(window_start)
            .bind(child_level)
            .bind(&parent.family)
            .fetch_all(pool)
            .await?
        }
        2 => {
            sqlx::query_as(
                "SELECT family, wiki, namespace, events, users_hll FROM agg_cells WHERE window_start = $1 AND level = $2 AND family = $3 AND wiki = $4",
            )
            .bind(window_start)
            .bind(child_level)
            .bind(&parent.family)
            .bind(&parent.wiki)
            .fetch_all(pool)
            .await?
        }
        _ => Vec::new(),
    };

    // Rows come back one per Kafka partition; group by the cell's own key
    // (dropping kpartition, unlike `fetch_children`'s SQL-side SUM) since
    // HLL sketches must be merged in application code, not summed in SQL.
    let mut grouped: HashMap<(String, String, i32), (i64, Option<HyperLogLog>)> = HashMap::new();
    for (family, wiki, namespace, events, hll_bytes) in rows {
        let entry = grouped.entry((family, wiki, namespace)).or_insert((0, None));
        entry.0 += events;
        merge_hll_bytes(&mut entry.1, hll_bytes);
    }

    Ok(grouped
        .into_iter()
        .map(|((family, wiki, namespace), (events, hll))| RawCell {
            family,
            wiki,
            namespace,
            events: events as u64,
            users: hll.map(|h| h.estimate().round() as u64).unwrap_or(0),
        })
        .collect())
}

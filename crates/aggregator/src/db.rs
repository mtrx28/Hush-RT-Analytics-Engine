use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};

use crate::window::Window;

pub async fn connect(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPool::connect(database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    Ok(pool)
}

/// Progress checkpoint for one partition, stored in Postgres instead of
/// Kafka's own consumer-offsets topic so it can be updated in the same
/// transaction as the aggregates it corresponds to.
pub struct PartitionProgress {
    pub committed_offset: i64,
    pub flushed_watermark: DateTime<Utc>,
}

pub async fn load_progress(pool: &PgPool, partition: i32) -> anyhow::Result<Option<PartitionProgress>> {
    let row = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
        "SELECT committed_offset, flushed_watermark FROM partition_progress WHERE kpartition = $1",
    )
    .bind(partition)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(committed_offset, flushed_watermark)| PartitionProgress {
        committed_offset,
        flushed_watermark,
    }))
}

/// Writes every closed window's cells, plus the new partition progress, in
/// one transaction. This is the source of Hush's exactly-once guarantee:
/// the "how far did we get" marker and the aggregates it describes can
/// never disagree, because they commit together or not at all.
pub async fn flush_windows(
    pool: &PgPool,
    partition: i32,
    closed: &[(DateTime<Utc>, Window)],
    committed_offset: i64,
    flushed_watermark: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    for (window_start, window) in closed {
        for (cell_key, agg) in &window.cells {
            sqlx::query(
                r#"
                INSERT INTO agg_cells
                    (window_start, level, family, wiki, namespace, kpartition, events, users)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (window_start, level, family, wiki, namespace, kpartition)
                DO UPDATE SET events = EXCLUDED.events, users = EXCLUDED.users
                "#,
            )
            .bind(window_start)
            .bind(cell_key.level)
            .bind(&cell_key.family)
            .bind(&cell_key.wiki)
            .bind(cell_key.namespace)
            .bind(partition)
            .bind(agg.events as i64)
            .bind(agg.users.len() as i64)
            .execute(&mut *tx)
            .await?;
        }
    }

    sqlx::query(
        r#"
        INSERT INTO partition_progress (kpartition, committed_offset, flushed_watermark)
        VALUES ($1, $2, $3)
        ON CONFLICT (kpartition)
        DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                       flushed_watermark = EXCLUDED.flushed_watermark
        "#,
    )
    .bind(partition)
    .bind(committed_offset)
    .bind(flushed_watermark)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

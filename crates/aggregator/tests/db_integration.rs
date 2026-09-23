//! Integration tests against a real Postgres, launched via testcontainers.
//! These exercise `db.rs`'s actual SQL, which the unit tests in `window.rs`
//! deliberately don't touch (those test the in-memory windowing logic in
//! isolation). Requires a reachable Docker daemon.

use aggregator::db;
use aggregator::window::{CellAgg, Window};
use chrono::{TimeZone, Utc};
use common::hierarchy::CellKey;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;

async fn connect_test_db() -> (sqlx::PgPool, ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.expect("start postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container.get_host_port_ipv4(5432).await.expect("mapped port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = db::connect(&url).await.expect("connect and migrate");
    (pool, container)
}

fn window_with_cells(cells: Vec<(CellKey, u64, Vec<&str>)>) -> Window {
    let mut window = Window {
        min_offset: 0,
        seen_ids: Default::default(),
        cells: Default::default(),
    };
    for (key, events, users) in cells {
        let agg = CellAgg {
            events,
            users: users.into_iter().map(String::from).collect(),
        };
        window.cells.insert(key, agg);
    }
    window
}

#[tokio::test]
async fn flush_then_load_round_trips_through_postgres() {
    let (pool, _container) = connect_test_db().await;

    let window_start = Utc.with_ymd_and_hms(2026, 9, 23, 10, 15, 0).unwrap();
    let window = window_with_cells(vec![(CellKey::global(), 3, vec!["u1", "u2", "u3"])]);
    let closed = vec![(window_start, window)];

    let flushed_watermark = Utc.with_ymd_and_hms(2026, 9, 23, 10, 16, 0).unwrap();
    db::flush_windows(&pool, 5, &closed, 42, flushed_watermark)
        .await
        .expect("flush");

    let progress = db::load_progress(&pool, 5)
        .await
        .expect("load progress")
        .expect("progress row exists");
    assert_eq!(progress.committed_offset, 42);
    assert_eq!(progress.flushed_watermark, flushed_watermark);

    let row: (i64, i64) = sqlx::query_as(
        "SELECT events, users FROM agg_cells WHERE window_start = $1 AND level = 0 AND kpartition = 5",
    )
    .bind(window_start)
    .fetch_one(&pool)
    .await
    .expect("row exists");
    assert_eq!(row, (3, 3));
}

#[tokio::test]
async fn flush_is_idempotent_under_reflush() {
    // Simulates a crash-and-replay: the same window gets flushed twice
    // (e.g. because the process died right after committing but before
    // the outer loop advanced). The upsert must leave the same final
    // state, not double-count.
    let (pool, _container) = connect_test_db().await;

    let window_start = Utc.with_ymd_and_hms(2026, 9, 23, 11, 0, 0).unwrap();
    let window = window_with_cells(vec![(CellKey::global(), 5, vec!["a", "b"])]);
    let closed = vec![(window_start, window)];
    let watermark = Utc.with_ymd_and_hms(2026, 9, 23, 11, 1, 0).unwrap();

    db::flush_windows(&pool, 0, &closed, 10, watermark).await.unwrap();
    db::flush_windows(&pool, 0, &closed, 10, watermark).await.unwrap();

    let row: (i64, i64) = sqlx::query_as(
        "SELECT events, users FROM agg_cells WHERE window_start = $1 AND level = 0 AND kpartition = 0",
    )
    .bind(window_start)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, (5, 2), "reflushing the same window must not double-count");

    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM agg_cells")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count.0, 1, "reflushing must not create a duplicate row");
}

#[tokio::test]
async fn load_progress_returns_none_for_unknown_partition() {
    let (pool, _container) = connect_test_db().await;
    let progress = db::load_progress(&pool, 999).await.unwrap();
    assert!(progress.is_none());
}


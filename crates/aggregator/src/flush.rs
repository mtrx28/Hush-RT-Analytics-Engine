use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{error, info};

use crate::db;
use crate::rebalance::SharedStates;
use crate::window::Window;

/// Periodically drains every partition's closed windows and writes them to
/// Postgres. Runs independently of message consumption so a burst of
/// traffic doesn't delay flushing already-closed windows.
pub async fn run_flush_loop(pool: PgPool, states: SharedStates, interval_ms: u64) {
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    // Tracks this instance's own high-water mark so the gauge only ever
    // moves forward, even though partitions flush independently and out of
    // order relative to each other within a tick.
    let mut last_flushed_window: Option<DateTime<Utc>> = None;
    loop {
        ticker.tick().await;

        let batches: Vec<(i32, Vec<(DateTime<Utc>, Window)>, i64, DateTime<Utc>)> = {
            let mut states = states.lock().unwrap();
            states
                .iter_mut()
                .filter_map(|(partition, state)| {
                    let closed = state.take_closed_windows();
                    if closed.is_empty() {
                        None
                    } else {
                        Some((*partition, closed, state.committed_offset(), state.flushed_watermark))
                    }
                })
                .collect()
        };

        for (partition, closed, committed_offset, watermark) in batches {
            let n_windows = closed.len();
            let max_window_start = closed.iter().map(|(window_start, _)| *window_start).max();
            match db::flush_windows(&pool, partition, &closed, committed_offset, watermark).await {
                Ok(()) => {
                    metrics::counter!("aggregator_windows_flushed_total").increment(n_windows as u64);
                    if let Some(window_start) = max_window_start {
                        if last_flushed_window.is_none_or(|prev| window_start > prev) {
                            last_flushed_window = Some(window_start);
                            metrics::gauge!("aggregator_last_flushed_window_timestamp")
                                .set(window_start.timestamp() as f64);
                        }
                    }
                    info!(partition, n_windows, committed_offset, %watermark, "flushed closed windows");
                }
                Err(e) => {
                    metrics::counter!("aggregator_flush_errors_total").increment(1);
                    error!(partition, error = %e, "failed to flush windows to postgres");
                }
            }
        }
    }
}

/// Advances idle partitions' watermarks by wall-clock time so a partition
/// that's stopped receiving events still closes its open windows.
pub async fn run_idle_loop(states: SharedStates, interval_ms: u64) {
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        let now_wall = std::time::Instant::now();
        let now_utc = Utc::now();
        let mut states = states.lock().unwrap();
        for state in states.values_mut() {
            state.tick_idle(now_wall, now_utc);
        }
    }
}

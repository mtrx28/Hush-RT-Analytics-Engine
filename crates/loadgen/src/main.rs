mod config;
mod generator;

use clap::Parser;
use common::EventBatch;
use config::LoadgenConfig;
use generator::Generator;
use hdrhistogram::Histogram;
use reqwest::Client;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cfg = LoadgenConfig::parse();

    let total_events = cfg.events_per_sec * cfg.duration_secs;
    let batches = (total_events as f64 / cfg.batch_size as f64).ceil() as u64;
    let batch_period = Duration::from_secs_f64(cfg.batch_size as f64 / cfg.events_per_sec as f64);

    info!(
        events_per_sec = cfg.events_per_sec,
        duration_secs = cfg.duration_secs,
        batches,
        batch_period_ms = batch_period.as_millis(),
        "starting loadgen (open-loop)"
    );

    let mut generator = Generator::new(cfg.clone());
    let http = Client::new();
    // HdrHistogram tracked in microseconds, up to 60s, 3 significant digits.
    let histogram = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?));

    let mut ticker = tokio::time::interval(batch_period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..batches {
        ticker.tick().await;
        let events = generator.next_batch(cfg.batch_size);
        let http = http.clone();
        let url = cfg.gateway_url.clone();
        let histogram = histogram.clone();
        // Open-loop: the next tick fires on schedule regardless of whether
        // this send has completed, so a slow period is measured honestly
        // instead of being smoothed away (avoiding coordinated omission).
        join_set.spawn(async move {
            let start = Instant::now();
            let body = EventBatch { events };
            let result = http.post(&url).json(&body).send().await;
            let elapsed_us = start.elapsed().as_micros() as u64;
            if let Ok(mut h) = histogram.lock() {
                let _ = h.record(elapsed_us);
            }
            if let Err(e) = result {
                tracing::warn!(error = %e, "batch send failed");
            }
        });
    }

    while join_set.join_next().await.is_some() {}

    let truth = generator.into_truth();
    let json = serde_json::to_string_pretty(&truth)?;
    std::fs::write(&cfg.truth_file, json)?;
    info!(file = %cfg.truth_file, cells = truth.cells.len(), "wrote ground truth");

    let h = histogram.lock().unwrap();
    info!(
        p50_ms = h.value_at_quantile(0.50) as f64 / 1000.0,
        p95_ms = h.value_at_quantile(0.95) as f64 / 1000.0,
        p99_ms = h.value_at_quantile(0.99) as f64 / 1000.0,
        max_ms = h.max() as f64 / 1000.0,
        count = h.len(),
        "latency summary"
    );

    Ok(())
}

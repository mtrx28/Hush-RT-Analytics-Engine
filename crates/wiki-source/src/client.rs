use std::time::Duration;

use common::{Event, EventBatch};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::Client;
use tracing::{error, info, warn};

use crate::config::SourceConfig;
use crate::mapping;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Runs forever: connects to the Wikimedia recentchange SSE stream, maps
/// each message to an `Event`, batches them, and POSTs batches to the
/// ingest gateway. Reconnects with exponential backoff on any stream
/// error, resuming from the last seen SSE id via `Last-Event-ID` so no
/// edits are missed across a reconnect.
pub async fn run(cfg: SourceConfig) -> anyhow::Result<()> {
    let http = Client::new();
    let mut last_event_id: Option<String> = None;
    let mut backoff = BACKOFF_INITIAL;

    loop {
        match connect_and_consume(&http, &cfg, &mut last_event_id).await {
            Ok(()) => {
                // Stream ended cleanly (server closed it); reconnect promptly.
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                warn!(error = %e, backoff_secs = backoff.as_secs(), "stream disconnected, backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

async fn connect_and_consume(
    http: &Client,
    cfg: &SourceConfig,
    last_event_id: &mut Option<String>,
) -> anyhow::Result<()> {
    // Wikimedia's EventStreams service rejects requests with no
    // identifying User-Agent (403 Forbidden) — see
    // https://meta.wikimedia.org/wiki/User-Agent_policy.
    let mut req = http
        .get(&cfg.stream_url)
        .header("User-Agent", "hush-wiki-source/0.1 (https://github.com/hush-analytics; contact@example.com)");
    if let Some(id) = last_event_id {
        req = req.header("Last-Event-ID", id.clone());
    }
    let response = req.send().await?.error_for_status()?;
    info!(url = %cfg.stream_url, "connected to wikimedia recentchange stream");

    let mut stream = response.bytes_stream().eventsource();
    let mut batch: Vec<Event> = Vec::with_capacity(cfg.batch_max_events);
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.batch_max_interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe_event = stream.next() => {
                let Some(event) = maybe_event else {
                    flush(http, cfg, &mut batch).await;
                    return Ok(());
                };
                let event = event?;
                if !event.id.is_empty() {
                    *last_event_id = Some(event.id.clone());
                }
                if event.data.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<mapping::RecentChange>(&event.data) {
                    Ok(rc) => match mapping::to_event(rc) {
                        Ok(e) => {
                            batch.push(e);
                            if batch.len() >= cfg.batch_max_events {
                                flush(http, cfg, &mut batch).await;
                            }
                        }
                        Err(e) => {
                            metrics_skip("map_error");
                            warn!(error = %e, "skipping unmappable recentchange message");
                        }
                    },
                    Err(e) => {
                        metrics_skip("parse_error");
                        warn!(error = %e, "skipping unparseable SSE payload");
                    }
                }
            }
            _ = ticker.tick() => {
                flush(http, cfg, &mut batch).await;
            }
        }
    }
}

fn metrics_skip(reason: &'static str) {
    metrics::counter!("wiki_source_skipped_total", "reason" => reason).increment(1);
}

async fn flush(http: &Client, cfg: &SourceConfig, batch: &mut Vec<Event>) {
    if batch.is_empty() {
        return;
    }
    let events = std::mem::take(batch);
    let n = events.len();
    let body = EventBatch { events };
    match http.post(&cfg.gateway_url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            metrics::counter!("wiki_source_events_sent_total").increment(n as u64);
        }
        Ok(resp) => {
            metrics::counter!("wiki_source_send_errors_total").increment(1);
            error!(status = %resp.status(), n, "gateway rejected batch");
        }
        Err(e) => {
            metrics::counter!("wiki_source_send_errors_total").increment(1);
            error!(error = %e, n, "failed to send batch to gateway");
        }
    }
}

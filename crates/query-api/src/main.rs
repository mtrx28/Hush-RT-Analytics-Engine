mod config;
mod db;
mod dp;
mod handlers;
mod observability;
mod privacy;

use axum::routing::get;
use axum::Router;
use config::QueryConfig;
use handlers::StatsResponse;
use metrics_exporter_prometheus::PrometheusBuilder;
use moka::future::Cache;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// Flushed windows never change, so caching their (already-suppressed,
    /// already-noised) answers is always safe and needs no invalidation.
    /// With DP noise enabled this also has a nice side effect: repeating
    /// the exact same query always returns the same cached noisy answer
    /// rather than fresh noise per request, which avoids leaking more
    /// signal than intended by letting someone average away the noise
    /// through repetition.
    pub cache: Cache<String, Arc<StatsResponse>>,
    pub k: u64,
    pub dp_enabled: bool,
    pub dp_epsilon: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cfg = QueryConfig::from_env();

    let metrics_addr: SocketAddr = cfg.metrics_addr.parse()?;
    PrometheusBuilder::new()
        // Without explicit buckets, `metrics::histogram!` renders as a
        // Prometheus *summary* (pre-computed per-instance quantiles, no
        // `_bucket` series) instead of a real histogram. Summaries can't be
        // aggregated across replicas with `histogram_quantile()` — exactly
        // what the dashboard's p50/p95/p99 panel needs if query-api is ever
        // scaled beyond one instance, so buckets are set explicitly here
        // rather than relying on the library's default.
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full("query_api_request_duration_seconds".to_string()),
            &[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )?
        .with_http_listener(metrics_addr)
        .install()?;

    let pool = db::connect(&cfg.database_url).await?;
    let cache = Cache::new(cfg.cache_capacity);
    let state = AppState {
        pool,
        cache,
        k: cfg.privacy_k,
        dp_enabled: cfg.dp_enabled,
        dp_epsilon: cfg.dp_epsilon,
    };

    let app = Router::new()
        .route("/v1/stats", get(handlers::get_stats))
        .route("/v1/windows/latest", get(handlers::get_latest_window))
        .route("/healthz", get(handlers::healthz))
        .route_layer(axum::middleware::from_fn(observability::track_metrics))
        .with_state(state);

    let addr: SocketAddr = cfg.bind_addr.parse()?;
    info!(%addr, k = cfg.privacy_k, dp_enabled = cfg.dp_enabled, "query-api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

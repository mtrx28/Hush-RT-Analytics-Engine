mod config;
mod db;
mod handlers;
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
    /// Flushed windows never change, so caching their (already-suppressed)
    /// answers is always safe and needs no invalidation.
    pub cache: Cache<String, Arc<StatsResponse>>,
    pub k: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cfg = QueryConfig::from_env();

    let metrics_addr: SocketAddr = cfg.metrics_addr.parse()?;
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()?;

    let pool = db::connect(&cfg.database_url).await?;
    let cache = Cache::new(cfg.cache_capacity);
    let state = AppState {
        pool,
        cache,
        k: cfg.privacy_k,
    };

    let app = Router::new()
        .route("/v1/stats", get(handlers::get_stats))
        .route("/v1/windows/latest", get(handlers::get_latest_window))
        .route("/healthz", get(handlers::healthz))
        .with_state(state);

    let addr: SocketAddr = cfg.bind_addr.parse()?;
    info!(%addr, k = cfg.privacy_k, "query-api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

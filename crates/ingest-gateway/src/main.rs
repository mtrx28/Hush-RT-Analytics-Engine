mod config;
mod handlers;
mod producer;

use axum::routing::{get, post};
use axum::Router;
use common::Pseudonymizer;
use config::AppConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub tx: mpsc::Sender<producer::QueuedMessage>,
    pub pseudonymizer: Pseudonymizer,
    pub max_batch_events: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let cfg = AppConfig::from_env();

    let metrics_builder = PrometheusBuilder::new();
    let metrics_addr: SocketAddr = cfg.metrics_addr.parse()?;
    metrics_builder
        .with_http_listener(metrics_addr)
        .install()?;

    let kafka_producer = producer::build_producer(&cfg)?;
    let (tx, rx) = mpsc::channel(cfg.channel_capacity);
    tokio::spawn(producer::run_producer_loop(
        rx,
        kafka_producer,
        cfg.topic.clone(),
    ));

    let state = AppState {
        tx,
        pseudonymizer: Pseudonymizer::new(cfg.pseudonym_salt.clone()),
        max_batch_events: cfg.max_batch_events,
    };

    let app = Router::new()
        .route("/v1/events", post(handlers::post_events))
        .route("/healthz", get(handlers::healthz))
        .with_state(state);

    let addr: SocketAddr = cfg.bind_addr.parse()?;
    info!(%addr, metrics_addr = %cfg.metrics_addr, "ingest-gateway listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

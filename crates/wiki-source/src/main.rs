mod client;
mod config;
mod mapping;

use common::config::env_or;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let metrics_addr: SocketAddr = env_or("WIKI_SOURCE_METRICS_ADDR", "0.0.0.0:9101").parse()?;
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()?;

    let cfg = config::SourceConfig::from_env();
    info!(stream_url = %cfg.stream_url, gateway_url = %cfg.gateway_url, "starting wiki-source");
    client::run(cfg).await
}

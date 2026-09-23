use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aggregator::config::AggregatorConfig;
use aggregator::rebalance::{self, RebalanceContext};
use aggregator::window::{self, PartitionState};
use aggregator::{db, flush, lag};
use common::Event;
use futures_util::StreamExt;
use metrics_exporter_prometheus::PrometheusBuilder;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cfg = AggregatorConfig::from_env();

    let metrics_addr: std::net::SocketAddr = cfg.metrics_addr.parse()?;
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()?;

    let pool = db::connect(&cfg.database_url).await?;
    let states: rebalance::SharedStates = Arc::new(Mutex::new(HashMap::new()));

    let context = RebalanceContext::new(pool.clone(), states.clone(), tokio::runtime::Handle::current(), cfg.hll_enabled);
    let consumer: StreamConsumer<RebalanceContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(context)?;
    let consumer = Arc::new(consumer);
    consumer.context().bind_consumer(&consumer);
    consumer.subscribe(&[&cfg.topic])?;

    info!(
        brokers = %cfg.kafka_brokers,
        topic = %cfg.topic,
        group_id = %cfg.group_id,
        "aggregator starting"
    );

    tokio::spawn(flush::run_flush_loop(pool.clone(), states.clone(), cfg.flush_interval_ms));
    tokio::spawn(flush::run_idle_loop(states.clone(), cfg.idle_check_interval_ms));
    tokio::spawn(lag::run_lag_loop(consumer.clone(), states.clone(), cfg.topic.clone()));

    let mut message_stream = consumer.stream();
    while let Some(result) = message_stream.next().await {
        match result {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    warn!("received message with no payload, skipping");
                    continue;
                };
                let event: Event = match serde_json::from_slice(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        metrics::counter!("aggregator_decode_errors_total").increment(1);
                        error!(error = %e, "failed to decode event payload, skipping");
                        continue;
                    }
                };

                let partition = msg.partition();
                let offset = msg.offset();
                let mut states = states.lock().unwrap();
                let state = states.entry(partition).or_insert_with(|| {
                    PartitionState::resume_from(partition, offset, event.ts - chrono::Duration::days(1))
                        .with_hll_enabled(cfg.hll_enabled)
                });

                match state.process_event(&event, offset) {
                    window::ProcessOutcome::Accepted => {
                        metrics::counter!("aggregator_events_processed_total").increment(1);
                    }
                    window::ProcessOutcome::Late => {
                        metrics::counter!("aggregator_late_events_total").increment(1);
                    }
                    window::ProcessOutcome::Duplicate => {
                        metrics::counter!("aggregator_duplicate_events_total").increment(1);
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "kafka consumer error");
            }
        }
    }

    Ok(())
}

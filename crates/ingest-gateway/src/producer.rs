use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tracing::{error, warn};

use crate::config::AppConfig;

/// One message queued for Kafka: the partition key (pseudonymized user) and
/// the JSON-serialized event payload.
pub struct QueuedMessage {
    pub key: String,
    pub payload: Vec<u8>,
}

pub fn build_producer(cfg: &AppConfig) -> anyhow::Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        // The broker discards duplicates from the producer's own internal
        // retries (e.g. after a transient network blip).
        .set("enable.idempotence", "true")
        // A write only counts once every in-sync replica has it.
        .set("acks", "all")
        // Wait briefly to fill larger batches: a little latency for a lot
        // more throughput. Overridable at runtime (not just recompiled)
        // specifically so docs/benchmarks.md's tuning numbers could be
        // measured across configurations without a rebuild per config.
        .set("linger.ms", &cfg.kafka_linger_ms)
        .set("batch.size", &cfg.kafka_batch_size)
        .set("compression.type", "lz4")
        .create()?;
    Ok(producer)
}

/// Drains the bounded channel and hands each message to librdkafka.
/// Each send is spawned as its own task rather than awaited in sequence:
/// awaiting delivery confirmation per message here would cap throughput at
/// one round trip at a time, defeating librdkafka's own internal batching.
/// The upstream bounded channel (and the gateway's 503 backpressure) keeps
/// the number of in-flight spawned tasks bounded instead.
pub async fn run_producer_loop(
    mut rx: Receiver<QueuedMessage>,
    producer: FutureProducer,
    topic: String,
) {
    while let Some(msg) = rx.recv().await {
        let producer = producer.clone();
        let topic = topic.clone();
        tokio::spawn(async move {
            let record = FutureRecord::to(&topic)
                .key(&msg.key)
                .payload(&msg.payload);
            match producer.send(record, Timeout::After(Duration::from_secs(10))).await {
                Ok(_) => {
                    metrics::counter!("ingest_kafka_produced_total").increment(1);
                }
                Err((e, _)) => {
                    metrics::counter!("ingest_kafka_produce_errors_total").increment(1);
                    error!(error = %e, "failed to produce event to kafka");
                }
            }
        });
    }
    warn!("producer channel closed, producer loop exiting");
}

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use tracing::warn;

use crate::rebalance::{RebalanceContext, SharedStates};

/// Because offsets live in Postgres rather than Kafka's own
/// `__consumer_offsets` topic, Kafka's built-in lag tooling can't see our
/// progress. We compute lag ourselves: ask the broker for each assigned
/// partition's high watermark and subtract the offset we've actually
/// processed up to.
pub async fn run_lag_loop(
    consumer: Arc<StreamConsumer<RebalanceContext>>,
    states: SharedStates,
    topic: String,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        ticker.tick().await;
        let partitions: Vec<(i32, i64)> = {
            let states = states.lock().unwrap();
            states.iter().map(|(p, s)| (*p, s.next_offset)).collect()
        };

        for (partition, processed_offset) in partitions {
            let consumer = consumer.clone();
            let topic = topic.clone();
            let watermarks = tokio::task::spawn_blocking(move || {
                consumer.fetch_watermarks(&topic, partition, Duration::from_secs(5))
            })
            .await;

            match watermarks {
                Ok(Ok((_low, high))) => {
                    let lag = (high - processed_offset).max(0);
                    metrics::gauge!("aggregator_consumer_lag", "partition" => partition.to_string())
                        .set(lag as f64);
                }
                Ok(Err(e)) => warn!(partition, error = %e, "failed to fetch watermarks"),
                Err(e) => warn!(partition, error = %e, "lag fetch task panicked"),
            }
        }
    }
}

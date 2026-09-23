use common::config::{env_or, env_or_parse, require_env};

#[derive(Clone)]
pub struct AppConfig {
    pub bind_addr: String,
    pub kafka_brokers: String,
    pub topic: String,
    /// HMAC salt used to pseudonymize raw usernames. Never logged, never
    /// leaves this process.
    pub pseudonym_salt: String,
    /// Capacity of the in-memory channel between HTTP handlers and the
    /// Kafka producer task. Bounds memory under overload; once full, the
    /// gateway starts returning 503 instead of buffering unboundedly.
    pub channel_capacity: usize,
    pub max_batch_events: usize,
    pub metrics_addr: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env_or("INGEST_BIND_ADDR", "0.0.0.0:8080"),
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            topic: env_or("EVENTS_TOPIC", common::config::EVENTS_TOPIC),
            pseudonym_salt: require_env("PSEUDONYM_SALT"),
            channel_capacity: env_or_parse("INGEST_CHANNEL_CAPACITY", 10_000),
            max_batch_events: env_or_parse("INGEST_MAX_BATCH_EVENTS", 5_000),
            metrics_addr: env_or("INGEST_METRICS_ADDR", "0.0.0.0:9100"),
        }
    }
}

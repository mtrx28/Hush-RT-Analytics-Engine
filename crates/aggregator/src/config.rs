use common::config::{env_or, require_env};

pub struct AggregatorConfig {
    pub kafka_brokers: String,
    pub topic: String,
    pub group_id: String,
    pub database_url: String,
    pub metrics_addr: String,
    pub flush_interval_ms: u64,
    pub idle_check_interval_ms: u64,
    /// When true, also maintains a HyperLogLog sketch per cell alongside
    /// the exact distinct-user set, purely to make the memory/accuracy
    /// trade-off measurable against real data. Off by default: the exact
    /// path is always used for privacy suppression regardless of this flag.
    pub hll_enabled: bool,
}

impl AggregatorConfig {
    pub fn from_env() -> Self {
        Self {
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            topic: env_or("EVENTS_TOPIC", common::config::EVENTS_TOPIC),
            group_id: env_or("AGGREGATOR_GROUP_ID", "hush-aggregator"),
            database_url: require_env("DATABASE_URL"),
            metrics_addr: env_or("AGGREGATOR_METRICS_ADDR", "0.0.0.0:9102"),
            flush_interval_ms: common::config::env_or_parse("FLUSH_INTERVAL_MS", 1_000),
            idle_check_interval_ms: common::config::env_or_parse("IDLE_CHECK_INTERVAL_MS", 1_000),
            hll_enabled: common::config::env_or_parse("HLL_ENABLED", false),
        }
    }
}

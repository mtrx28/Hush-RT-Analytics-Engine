use common::config::{env_or, env_or_parse};

#[derive(Clone)]
pub struct SourceConfig {
    pub stream_url: String,
    pub gateway_url: String,
    pub batch_max_events: usize,
    pub batch_max_interval_ms: u64,
}

impl SourceConfig {
    pub fn from_env() -> Self {
        Self {
            stream_url: env_or(
                "WIKI_STREAM_URL",
                "https://stream.wikimedia.org/v2/stream/recentchange",
            ),
            gateway_url: env_or("GATEWAY_URL", "http://localhost:8080/v1/events"),
            batch_max_events: env_or_parse("BATCH_MAX_EVENTS", 500),
            batch_max_interval_ms: env_or_parse("BATCH_MAX_INTERVAL_MS", 200),
        }
    }
}

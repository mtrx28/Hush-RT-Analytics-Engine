use common::config::{env_or, env_or_parse, require_env};

pub struct QueryConfig {
    pub bind_addr: String,
    pub database_url: String,
    pub metrics_addr: String,
    pub privacy_k: u64,
    pub cache_capacity: u64,
    /// Opt-in calibrated (Laplace-mechanism) noise layered on top of
    /// k-anonymity suppression — see `dp.rs` for what this does and does
    /// not guarantee. Off by default: behavior is unchanged unless set.
    pub dp_enabled: bool,
    pub dp_epsilon: f64,
}

impl QueryConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env_or("QUERY_BIND_ADDR", "0.0.0.0:8081"),
            database_url: require_env("DATABASE_URL"),
            metrics_addr: env_or("QUERY_METRICS_ADDR", "0.0.0.0:9103"),
            privacy_k: env_or_parse("PRIVACY_K", crate::privacy::DEFAULT_K),
            cache_capacity: env_or_parse("QUERY_CACHE_CAPACITY", 10_000),
            dp_enabled: env_or_parse("DP_ENABLED", false),
            dp_epsilon: env_or_parse("DP_EPSILON", 1.0),
        }
    }
}

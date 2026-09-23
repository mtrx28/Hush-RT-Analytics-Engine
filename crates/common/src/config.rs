use std::env;

/// Name of the single Kafka topic the whole pipeline reads and writes.
pub const EVENTS_TOPIC: &str = "events";
/// Fixed partition count for the events topic (see docs/decisions.md for why 12).
pub const EVENTS_TOPIC_PARTITIONS: i32 = 12;

/// Reads a required environment variable, or panics with a clear message.
/// Used at service startup only, never in request-handling paths.
pub fn require_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("missing required environment variable: {key}"))
}

pub fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

pub fn env_or_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

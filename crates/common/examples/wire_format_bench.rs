//! Not a `criterion` benchmark (kept dependency-light) — a small, honest
//! timing comparison of JSON vs. bincode for `Event`, run with:
//!   scripts/dev-cargo.sh run -p common --example wire_format_bench --release
//!
//! Numbers feed docs/benchmarks.md's JSON-vs-binary section. This does NOT
//! switch Hush's actual wire format — see that doc for why not.

use chrono::Utc;
use common::{Event, EventKind};
use std::time::Instant;
use uuid::Uuid;

fn sample_event(i: u64) -> Event {
    Event {
        event_id: Uuid::new_v4(),
        user: format!("user-{i}"),
        ts: Utc::now(),
        family: "wikipedia".to_string(),
        wiki: "enwiki".to_string(),
        namespace: 0,
        kind: EventKind::Edit,
        is_bot: false,
    }
}

fn main() {
    const N: usize = 100_000;
    let events: Vec<Event> = (0..N as u64).map(sample_event).collect();

    // JSON encode
    let start = Instant::now();
    let json_bytes: Vec<Vec<u8>> = events.iter().map(|e| serde_json::to_vec(e).unwrap()).collect();
    let json_encode = start.elapsed();

    // JSON decode
    let start = Instant::now();
    for b in &json_bytes {
        let _: Event = serde_json::from_slice(b).unwrap();
    }
    let json_decode = start.elapsed();

    let json_total_bytes: usize = json_bytes.iter().map(|b| b.len()).sum();

    // bincode encode
    let start = Instant::now();
    let bincode_bytes: Vec<Vec<u8>> = events.iter().map(|e| bincode::serialize(e).unwrap()).collect();
    let bincode_encode = start.elapsed();

    // bincode decode
    let start = Instant::now();
    for b in &bincode_bytes {
        let _: Event = bincode::deserialize(b).unwrap();
    }
    let bincode_decode = start.elapsed();

    let bincode_total_bytes: usize = bincode_bytes.iter().map(|b| b.len()).sum();

    eprintln!("N = {N} events");
    eprintln!(
        "JSON:    encode={:?} ({:.0} ns/event), decode={:?} ({:.0} ns/event), avg_bytes/event={:.1}",
        json_encode,
        json_encode.as_nanos() as f64 / N as f64,
        json_decode,
        json_decode.as_nanos() as f64 / N as f64,
        json_total_bytes as f64 / N as f64,
    );
    eprintln!(
        "bincode: encode={:?} ({:.0} ns/event), decode={:?} ({:.0} ns/event), avg_bytes/event={:.1}",
        bincode_encode,
        bincode_encode.as_nanos() as f64 / N as f64,
        bincode_decode,
        bincode_decode.as_nanos() as f64 / N as f64,
        bincode_total_bytes as f64 / N as f64,
    );
    eprintln!(
        "bincode is {:.2}x smaller, {:.2}x faster to encode, {:.2}x faster to decode",
        json_total_bytes as f64 / bincode_total_bytes as f64,
        json_encode.as_nanos() as f64 / bincode_encode.as_nanos() as f64,
        json_decode.as_nanos() as f64 / bincode_decode.as_nanos() as f64,
    );
}

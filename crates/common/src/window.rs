use chrono::{DateTime, DurationRound, TimeDelta, Utc};

/// Tumbling window size used throughout the pipeline.
pub const WINDOW_SIZE: TimeDelta = TimeDelta::seconds(60);

/// How long after an event's own timestamp we keep its window open, to
/// absorb reordering and network delay before declaring the window closed.
pub const ALLOWED_LATENESS: TimeDelta = TimeDelta::seconds(30);

/// If a partition has produced no events for this long, we advance its
/// watermark using wall-clock time instead, so an idle partition doesn't
/// leave windows open forever. Expressed as `std::time::Duration` because
/// it's compared against `Instant`, not against event timestamps.
pub const IDLE_PARTITION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Floors a timestamp to the start of its containing tumbling window.
pub fn window_start(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(WINDOW_SIZE)
        .expect("WINDOW_SIZE is a fixed, non-zero duration")
}

pub fn window_end(start: DateTime<Utc>) -> DateTime<Utc> {
    start + WINDOW_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn floors_to_minute_boundary() {
        let ts = Utc.with_ymd_and_hms(2026, 9, 23, 10, 15, 47).unwrap();
        let start = window_start(ts);
        assert_eq!(start, Utc.with_ymd_and_hms(2026, 9, 23, 10, 15, 0).unwrap());
    }

    #[test]
    fn already_on_boundary_is_unchanged() {
        let ts = Utc.with_ymd_and_hms(2026, 9, 23, 10, 15, 0).unwrap();
        assert_eq!(window_start(ts), ts);
    }

    #[test]
    fn window_end_is_one_window_later() {
        let start = Utc.with_ymd_and_hms(2026, 9, 23, 10, 15, 0).unwrap();
        assert_eq!(
            window_end(start),
            Utc.with_ymd_and_hms(2026, 9, 23, 10, 16, 0).unwrap()
        );
    }
}

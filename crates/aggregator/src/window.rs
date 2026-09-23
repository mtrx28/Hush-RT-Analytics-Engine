use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use common::hierarchy::CellKey;
use common::window::{window_end, window_start, ALLOWED_LATENESS, IDLE_PARTITION_TIMEOUT};
use common::Event;
use uuid::Uuid;

/// Per-cell aggregate accumulated in memory for one open window.
#[derive(Debug, Default, Clone)]
pub struct CellAgg {
    pub events: u64,
    /// Exact set of pseudonymized users seen in this cell. Because a
    /// user's events always land in the same Kafka partition (partitioned
    /// by pseudonym), the sets kept by different partitions never overlap,
    /// so summing `users.len()` across partitions at query time is exact —
    /// no HyperLogLog or cross-partition merge needed.
    pub users: HashSet<String>,
}

/// State for one still-open tumbling window on one partition.
#[derive(Debug)]
pub struct Window {
    /// Lowest Kafka offset of any event folded into this window. Used to
    /// compute the safe-to-resume-from offset when the window is flushed.
    pub min_offset: i64,
    /// Dedups events within this window by id. Freed when the window flushes.
    pub seen_ids: HashSet<Uuid>,
    pub cells: HashMap<CellKey, CellAgg>,
}

impl Window {
    fn new(offset: i64) -> Self {
        Window {
            min_offset: offset,
            seen_ids: HashSet::new(),
            cells: HashMap::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessOutcome {
    Accepted,
    /// The event's window already closed and flushed; dropped.
    Late,
    /// Same `event_id` already seen in this window; dropped.
    Duplicate,
}

/// All in-memory state for one assigned Kafka partition.
pub struct PartitionState {
    #[allow(dead_code)]
    pub partition: i32,
    pub open_windows: BTreeMap<DateTime<Utc>, Window>,
    /// The latest event timestamp seen so far on this partition. The
    /// watermark is derived from this: `max_event_ts - ALLOWED_LATENESS`.
    pub max_event_ts: DateTime<Utc>,
    /// Everything with `window_end <= flushed_watermark` has already been
    /// written to Postgres.
    pub flushed_watermark: DateTime<Utc>,
    /// Offset of the next message to be processed (i.e. one past the last
    /// processed offset). Used as the committed offset when no windows are
    /// currently open.
    pub next_offset: i64,
    /// Wall-clock time this partition last received an event. Drives idle
    /// detection so a quiet partition doesn't leave windows open forever.
    pub last_event_wall: std::time::Instant,
}

impl PartitionState {
    /// Constructs fresh state for a partition resuming from `committed_offset`
    /// with everything before `flushed_watermark` already durable.
    pub fn resume_from(partition: i32, committed_offset: i64, flushed_watermark: DateTime<Utc>) -> Self {
        PartitionState {
            partition,
            open_windows: BTreeMap::new(),
            max_event_ts: flushed_watermark,
            flushed_watermark,
            next_offset: committed_offset,
            last_event_wall: std::time::Instant::now(),
        }
    }

    pub fn watermark(&self) -> DateTime<Utc> {
        self.max_event_ts - ALLOWED_LATENESS
    }

    /// Folds one event (at Kafka `offset`) into its window, applying the
    /// late-drop and dedup rules. Advances `max_event_ts` and `next_offset`
    /// regardless of outcome, since even a dropped event has been consumed.
    pub fn process_event(&mut self, event: &Event, offset: i64) -> ProcessOutcome {
        self.next_offset = offset + 1;
        self.last_event_wall = std::time::Instant::now();
        if event.ts > self.max_event_ts {
            self.max_event_ts = event.ts;
        }

        let w_start = window_start(event.ts);
        let w_end = window_end(w_start);
        if w_end <= self.flushed_watermark {
            return ProcessOutcome::Late;
        }

        let window = self
            .open_windows
            .entry(w_start)
            .or_insert_with(|| Window::new(offset));
        if !window.seen_ids.insert(event.event_id) {
            return ProcessOutcome::Duplicate;
        }
        window.min_offset = window.min_offset.min(offset);
        for cell_key in CellKey::cells_for_event(event) {
            let agg = window.cells.entry(cell_key).or_default();
            agg.events += 1;
            agg.users.insert(event.user.clone());
        }
        ProcessOutcome::Accepted
    }

    /// If this partition has been idle past `IDLE_PARTITION_TIMEOUT`,
    /// advances its effective watermark using wall-clock time instead of
    /// event time, so its windows still close. Call periodically from the
    /// main loop (not just on message receipt).
    pub fn tick_idle(&mut self, now_wall: std::time::Instant, now_utc: DateTime<Utc>) {
        if now_wall.duration_since(self.last_event_wall) > IDLE_PARTITION_TIMEOUT
            && now_utc > self.max_event_ts
        {
            self.max_event_ts = now_utc;
        }
    }

    /// Removes and returns every window that is now closed under the
    /// current watermark, along with the window_start each came from.
    /// Also advances `flushed_watermark` to the current watermark.
    pub fn take_closed_windows(&mut self) -> Vec<(DateTime<Utc>, Window)> {
        let watermark = self.watermark();
        let closed_starts: Vec<DateTime<Utc>> = self
            .open_windows
            .keys()
            .filter(|&&start| window_end(start) <= watermark)
            .copied()
            .collect();

        let mut closed = Vec::with_capacity(closed_starts.len());
        for start in closed_starts {
            if let Some(w) = self.open_windows.remove(&start) {
                closed.push((start, w));
            }
        }
        if watermark > self.flushed_watermark {
            self.flushed_watermark = watermark;
        }
        closed
    }

    /// The offset that is safe to record as "committed" right now: the
    /// lowest `min_offset` among still-open windows, or `next_offset` if
    /// nothing is open. This is what gets written to `partition_progress`
    /// in the same transaction as the flushed aggregates, replacing Kafka's
    /// own offset-commit mechanism.
    pub fn committed_offset(&self) -> i64 {
        self.open_windows
            .values()
            .map(|w| w.min_offset)
            .min()
            .unwrap_or(self.next_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use common::EventKind;

    fn evt(ts: DateTime<Utc>, id: Uuid, user: &str) -> Event {
        Event {
            event_id: id,
            user: user.to_string(),
            ts,
            family: "wikipedia".to_string(),
            wiki: "enwiki".to_string(),
            namespace: 0,
            kind: EventKind::Edit,
            is_bot: false,
        }
    }

    fn t(sec: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 23, 10, 0, 0).unwrap() + chrono::Duration::seconds(sec as i64)
    }

    #[test]
    fn accepts_and_dedups_within_window() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        let id = Uuid::new_v4();
        let e = evt(t(5), id, "u1");
        assert_eq!(ps.process_event(&e, 0), ProcessOutcome::Accepted);
        assert_eq!(ps.process_event(&e, 1), ProcessOutcome::Duplicate);

        let window = ps.open_windows.values().next().unwrap();
        let global = window.cells.get(&CellKey::global()).unwrap();
        assert_eq!(global.events, 1);
        assert_eq!(global.users.len(), 1);
    }

    #[test]
    fn distinct_users_counted_exactly() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        ps.process_event(&evt(t(1), Uuid::new_v4(), "u1"), 0);
        ps.process_event(&evt(t(2), Uuid::new_v4(), "u1"), 1);
        ps.process_event(&evt(t(3), Uuid::new_v4(), "u2"), 2);

        let window = ps.open_windows.values().next().unwrap();
        let global = window.cells.get(&CellKey::global()).unwrap();
        assert_eq!(global.events, 3);
        assert_eq!(global.users.len(), 2);
    }

    #[test]
    fn late_event_dropped_after_flush() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        // Advance watermark far enough to close window at t(0).
        ps.process_event(&evt(t(200), Uuid::new_v4(), "u1"), 0);
        ps.take_closed_windows();

        // Now an event for the already-flushed window arrives late.
        let outcome = ps.process_event(&evt(t(5), Uuid::new_v4(), "u2"), 1);
        assert_eq!(outcome, ProcessOutcome::Late);
    }

    #[test]
    fn window_closes_once_watermark_passes_its_end() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        ps.process_event(&evt(t(5), Uuid::new_v4(), "u1"), 0);
        // watermark = max_event_ts - 30s = t(5) - 30s, window [t0,t60) still open.
        assert!(ps.take_closed_windows().is_empty());

        ps.process_event(&evt(t(95), Uuid::new_v4(), "u2"), 1);
        // watermark = t(95) - 30s = t(65) >= window_end t(60): closes.
        let closed = ps.take_closed_windows();
        assert_eq!(closed.len(), 1);
    }

    #[test]
    fn committed_offset_tracks_open_windows_min() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        ps.process_event(&evt(t(5), Uuid::new_v4(), "u1"), 10);
        ps.process_event(&evt(t(65), Uuid::new_v4(), "u2"), 11);
        // Two open windows now: min_offset 10 and 11. Lowest is 10.
        assert_eq!(ps.committed_offset(), 10);
    }

    #[test]
    fn committed_offset_falls_back_to_next_offset_when_all_closed() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        ps.process_event(&evt(t(5), Uuid::new_v4(), "u1"), 10);
        ps.process_event(&evt(t(200), Uuid::new_v4(), "u2"), 11);
        // Force the watermark past every remaining open window, the way an
        // idle-partition tick would, so nothing stays open.
        let far_future_wall = ps.last_event_wall + common::window::IDLE_PARTITION_TIMEOUT + std::time::Duration::from_secs(1);
        ps.tick_idle(far_future_wall, t(200) + chrono::Duration::hours(1));
        ps.take_closed_windows();
        assert_eq!(ps.committed_offset(), ps.next_offset);
    }

    #[test]
    fn idle_partition_advances_watermark_by_wall_clock() {
        let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(1));
        ps.process_event(&evt(t(5), Uuid::new_v4(), "u1"), 0);
        let far_future_wall = ps.last_event_wall + common::window::IDLE_PARTITION_TIMEOUT + std::time::Duration::from_secs(1);
        let far_future_utc = t(5) + chrono::Duration::hours(1);
        ps.tick_idle(far_future_wall, far_future_utc);
        let closed = ps.take_closed_windows();
        assert_eq!(closed.len(), 1, "idle watermark advance should close the window");
    }

    proptest::proptest! {
        /// For any stream of events with random duplicates and arbitrary
        /// arrival order (reordering within what `ALLOWED_LATENESS` can
        /// absorb), the flushed totals equal the true totals: every unique
        /// event_id counted exactly once, against the window its own
        /// timestamp maps to.
        #[test]
        fn flushed_totals_match_true_totals_under_dup_and_reorder(
            // Timestamps kept within a span well under ALLOWED_LATENESS, so
            // shuffling arrival order never causes a legitimate late-drop
            // (that behavior is covered separately by
            // `late_event_dropped_after_flush`): this test isolates dedup +
            // distinct-user counting under reordering.
            specs in proptest::collection::vec((0u32..20, 0u32..5, 0u32..3), 1..60),
            seed in proptest::prelude::any::<u64>(),
        ) {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            // Build the unique events plus their duplicate copies, then
            // shuffle deterministically (seeded) to simulate reordering.
            let mut wire: Vec<Event> = Vec::new();
            let mut true_totals: HashMap<DateTime<Utc>, (u64, HashSet<String>)> = HashMap::new();
            for (ts_offset, user_idx, dup_count) in &specs {
                let ts = t(*ts_offset);
                let user = format!("u{user_idx}");
                let event = evt(ts, Uuid::new_v4(), &user);
                let entry = true_totals.entry(window_start(ts)).or_default();
                entry.0 += 1;
                entry.1.insert(user.clone());

                wire.push(event.clone());
                for _ in 0..*dup_count {
                    wire.push(event.clone());
                }
            }

            // Deterministic shuffle from the proptest-provided seed, so
            // failures reproduce without relying on external randomness.
            let mut keyed: Vec<(u64, Event)> = wire
                .into_iter()
                .enumerate()
                .map(|(i, e)| {
                    let mut hasher = DefaultHasher::new();
                    (seed, i).hash(&mut hasher);
                    (hasher.finish(), e)
                })
                .collect();
            keyed.sort_by_key(|(k, _)| *k);

            let mut ps = PartitionState::resume_from(0, 0, t(0) - chrono::Duration::minutes(5));
            for (offset, (_, event)) in keyed.into_iter().enumerate() {
                ps.process_event(&event, offset as i64);
            }

            // Force every window closed, the way idle detection eventually would.
            let far_wall = ps.last_event_wall + common::window::IDLE_PARTITION_TIMEOUT + std::time::Duration::from_secs(1);
            let far_utc = t(0) + chrono::Duration::hours(2);
            ps.tick_idle(far_wall, far_utc);
            let closed = ps.take_closed_windows();

            let mut actual_totals: HashMap<DateTime<Utc>, (u64, usize)> = HashMap::new();
            for (window_start, window) in &closed {
                if let Some(global) = window.cells.get(&CellKey::global()) {
                    actual_totals.insert(*window_start, (global.events, global.users.len()));
                }
            }

            for (window_start, (true_events, true_users)) in &true_totals {
                let (actual_events, actual_users) = actual_totals
                    .get(window_start)
                    .copied()
                    .unwrap_or((0, 0));
                proptest::prop_assert_eq!(actual_events, *true_events, "event count drift in window {:?}", window_start);
                proptest::prop_assert_eq!(actual_users, true_users.len(), "user count drift in window {:?}", window_start);
            }
        }
    }
}

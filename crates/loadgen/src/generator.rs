use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::hierarchy::CellKey;
use common::window::ALLOWED_LATENESS;
use common::{Event, EventKind};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde::Serialize;
use uuid::Uuid;

use crate::config::LoadgenConfig;

const WIKIS: &[(&str, &str)] = &[
    ("wikipedia", "enwiki"),
    ("wikipedia", "knwiki"),
    ("wikipedia", "dewiki"),
    ("wikipedia", "frwiki"),
    ("wikipedia", "jawiki"),
    ("wiktionary", "enwiktionary"),
    ("wikidata", "wikidatawiki"),
    ("commons", "commonswiki"),
];

/// One truthfully-expected aggregate cell, written to `truth.json` so
/// `reconcile` can compare it byte-for-byte against what ended up in
/// Postgres after the whole pipeline (including any chaos) settles.
#[derive(Debug, Serialize, Clone)]
pub struct TruthCell {
    pub window_start: DateTime<Utc>,
    pub level: i16,
    pub family: String,
    pub wiki: String,
    pub namespace: i32,
    pub events: u64,
    pub users: u64,
}

#[derive(Debug, Serialize)]
pub struct Truth {
    pub cells: Vec<TruthCell>,
}

/// Deterministically generates a Zipf-skewed synthetic event stream and
/// tracks ground truth as it goes.
///
/// Design choice: late events are backdated by a random amount strictly
/// less than `ALLOWED_LATENESS` and sent immediately, so in a local
/// docker-compose setup (sub-second network transit) they reliably land
/// inside the aggregator's lateness window rather than being dropped.
/// That keeps ground truth deterministic: every unique event this
/// generator emits is expected to be counted exactly once, so
/// `reconcile`'s pass criterion can be "zero drift" rather than needing to
/// model exactly which late events survive.
pub struct Generator {
    rng: StdRng,
    user_zipf: Zipf<f64>,
    wiki_zipf: Zipf<f64>,
    cfg: LoadgenConfig,
    truth: HashMap<(DateTime<Utc>, i16, String, String, i32), (u64, std::collections::HashSet<String>)>,
}

impl Generator {
    pub fn new(cfg: LoadgenConfig) -> Self {
        Generator {
            rng: StdRng::seed_from_u64(cfg.seed),
            user_zipf: Zipf::new(cfg.num_users, cfg.zipf_exponent).expect("valid zipf params"),
            wiki_zipf: Zipf::new(WIKIS.len() as u64, 1.0).expect("valid zipf params"),
            cfg,
            truth: HashMap::new(),
        }
    }

    /// Generates one logical event and returns the wire payload(s) to send:
    /// normally just the event itself, but with probability
    /// `duplicate_rate` a second copy with the same `event_id` follows,
    /// which must not double-count in ground truth.
    pub fn next_batch(&mut self, n: usize) -> Vec<Event> {
        let mut out = Vec::with_capacity(n + n / 10);
        for _ in 0..n {
            let event = self.gen_one();
            self.record_truth(&event);
            let is_dup = self.rng.gen_bool(self.cfg.duplicate_rate);
            out.push(event.clone());
            if is_dup {
                out.push(event);
            }
        }
        out
    }

    fn gen_one(&mut self) -> Event {
        let user_rank = self.user_zipf.sample(&mut self.rng) as u64;
        let wiki_idx = (self.wiki_zipf.sample(&mut self.rng) as usize - 1).min(WIKIS.len() - 1);
        let (family, wiki) = WIKIS[wiki_idx];

        let is_late = self.rng.gen_bool(self.cfg.late_rate);
        let ts = if is_late {
            let backdate_secs = self
                .rng
                .gen_range(1..(ALLOWED_LATENESS.num_seconds() as u64).saturating_sub(2).max(2));
            Utc::now() - chrono::Duration::seconds(backdate_secs as i64)
        } else {
            Utc::now()
        };

        let namespace = match self.rng.gen_range(0..10) {
            0..=6 => 0,
            7..=8 => 1,
            _ => 2,
        };
        let kind = match self.rng.gen_range(0..10) {
            0..=6 => EventKind::Edit,
            7 => EventKind::New,
            8 => EventKind::Log,
            _ => EventKind::Categorize,
        };

        Event {
            event_id: Uuid::new_v4(),
            user: format!("loadgen-user-{user_rank}"),
            ts,
            family: family.to_string(),
            wiki: wiki.to_string(),
            namespace,
            kind,
            is_bot: false,
        }
    }

    fn record_truth(&mut self, event: &Event) {
        let window_start = common::window::window_start(event.ts);
        for cell_key in CellKey::cells_for_event(event) {
            let entry = self
                .truth
                .entry((window_start, cell_key.level, cell_key.family, cell_key.wiki, cell_key.namespace))
                .or_insert_with(|| (0, std::collections::HashSet::new()));
            entry.0 += 1;
            entry.1.insert(event.user.clone());
        }
    }

    pub fn into_truth(self) -> Truth {
        let cells = self
            .truth
            .into_iter()
            .map(|((window_start, level, family, wiki, namespace), (events, users))| TruthCell {
                window_start,
                level,
                family,
                wiki,
                namespace,
                events,
                users: users.len() as u64,
            })
            .collect();
        Truth { cells }
    }
}

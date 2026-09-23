use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "reconcile")]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, default_value = "truth.json")]
    truth_file: String,
}

#[derive(Debug, Deserialize)]
struct TruthCell {
    window_start: DateTime<Utc>,
    level: i16,
    family: String,
    wiki: String,
    namespace: i32,
    events: u64,
    users: u64,
}

#[derive(Debug, Deserialize)]
struct Truth {
    cells: Vec<TruthCell>,
}

type Key = (DateTime<Utc>, i16, String, String, i32);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let args = Args::parse();

    let raw = std::fs::read_to_string(&args.truth_file)?;
    let truth: Truth = serde_json::from_str(&raw)?;
    let expected: HashMap<Key, (u64, u64)> = truth
        .cells
        .into_iter()
        .map(|c| ((c.window_start, c.level, c.family, c.wiki, c.namespace), (c.events, c.users)))
        .collect();

    let pool = PgPool::connect(&args.database_url).await?;
    let rows: Vec<(DateTime<Utc>, i16, String, String, i32, i64, i64)> = sqlx::query_as(
        r#"
        SELECT window_start, level, family, wiki, namespace,
               SUM(events)::bigint, SUM(users)::bigint
        FROM agg_cells
        GROUP BY window_start, level, family, wiki, namespace
        "#,
    )
    .fetch_all(&pool)
    .await?;

    let actual: HashMap<Key, (u64, u64)> = rows
        .into_iter()
        .map(|(ws, level, family, wiki, ns, events, users)| {
            ((ws, level, family, wiki, ns), (events as u64, users as u64))
        })
        .collect();

    let mut mismatches = 0u64;
    let mut missing = 0u64;
    let mut unexpected = 0u64;

    for (key, (exp_events, exp_users)) in &expected {
        match actual.get(key) {
            Some((act_events, act_users)) => {
                if act_events != exp_events || act_users != exp_users {
                    mismatches += 1;
                    error!(
                        window_start = %key.0, level = key.1, family = %key.2, wiki = %key.3, namespace = key.4,
                        expected_events = exp_events, actual_events = act_events,
                        expected_users = exp_users, actual_users = act_users,
                        "drift detected"
                    );
                }
            }
            None => {
                missing += 1;
                error!(window_start = %key.0, level = key.1, family = %key.2, wiki = %key.3, namespace = key.4, "expected cell missing from postgres");
            }
        }
    }

    for key in actual.keys() {
        if !expected.contains_key(key) {
            unexpected += 1;
            error!(window_start = %key.0, level = key.1, family = %key.2, wiki = %key.3, namespace = key.4, "unexpected cell present in postgres");
        }
    }

    info!(
        expected_cells = expected.len(),
        actual_cells = actual.len(),
        mismatches,
        missing,
        unexpected,
        "reconciliation complete"
    );

    if mismatches == 0 && missing == 0 && unexpected == 0 {
        info!("PASS: zero drift");
        Ok(())
    } else {
        anyhow::bail!("FAIL: drift detected ({mismatches} mismatches, {missing} missing, {unexpected} unexpected)");
    }
}

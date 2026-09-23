use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use common::hierarchy::{CellKey, LEVEL_FAMILY, LEVEL_GLOBAL, LEVEL_NAMESPACE, LEVEL_WIKI, ROLLUP_NAMESPACE, ROLLUP_STR};
use common::window::window_end;
use serde::{Deserialize, Serialize};

use crate::privacy::{suppress, suppress_single, ResultCell};
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct StatsQuery {
    pub window: DateTime<Utc>,
    pub level: i16,
    pub family: Option<String>,
    pub wiki: Option<String>,
    /// `exact` (default) uses the exact per-partition distinct-user sets.
    /// `hll` uses the merged HyperLogLog estimate instead, purely to make
    /// the memory/accuracy trade-off comparable against the same live
    /// data — see docs/hyperloglog.md. Reads as 0 users everywhere if the
    /// aggregator wasn't run with HLL_ENABLED=true.
    #[serde(default)]
    pub estimator: Estimator,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Estimator {
    #[default]
    Exact,
    Hll,
}

#[derive(Debug, Serialize, Clone)]
pub struct StatsResponse {
    pub window_start: DateTime<Utc>,
    pub level: i16,
    pub cells: Vec<ResultCell>,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: String,
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg.into() }))
}

/// `GET /v1/stats?window=...&level=0..3[&family=...][&wiki=...]`
///
/// Returns the suppressed breakdown at `level` for the given window. Only
/// fixed, complete windows and the fixed 4-level hierarchy can be queried —
/// no free-form filters, no custom time ranges — which limits how far
/// someone could combine overlapping queries to work around suppression.
pub async fn get_stats(
    State(state): State<AppState>,
    Query(q): Query<StatsQuery>,
) -> Result<Json<StatsResponse>, (StatusCode, Json<ErrorBody>)> {
    if !(LEVEL_GLOBAL..=LEVEL_NAMESPACE).contains(&q.level) {
        return Err(bad_request("level must be between 0 and 3"));
    }
    if q.level >= LEVEL_WIKI && q.family.is_none() {
        return Err(bad_request("family is required for level >= 2"));
    }
    if q.level >= LEVEL_NAMESPACE && q.wiki.is_none() {
        return Err(bad_request("wiki is required for level >= 3"));
    }

    let min_watermark = crate::db::min_flushed_watermark(&state.pool)
        .await
        .map_err(internal_error)?;
    let complete = min_watermark
        .map(|wm| window_end(q.window) <= wm)
        .unwrap_or(false);
    if !complete {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "window is not yet fully flushed by every partition".to_string(),
            }),
        ));
    }

    let cache_key = format!(
        "{}:{}:{}:{}:{:?}",
        q.window.to_rfc3339(),
        q.level,
        q.family.clone().unwrap_or_default(),
        q.wiki.clone().unwrap_or_default(),
        q.estimator,
    );
    if let Some(hit) = state.cache.get(&cache_key).await {
        return Ok(Json((*hit).clone()));
    }

    let response = compute_stats(&state, q).await.map_err(internal_error)?;
    state.cache.insert(cache_key, Arc::new(response.clone())).await;
    Ok(Json(response))
}

async fn compute_stats(state: &AppState, q: StatsQuery) -> anyhow::Result<StatsResponse> {
    if q.level == LEVEL_GLOBAL {
        let global = match q.estimator {
            Estimator::Exact => crate::db::fetch_cell(&state.pool, q.window, &CellKey::global()).await?,
            Estimator::Hll => crate::db::fetch_cell_hll(&state.pool, q.window, &CellKey::global()).await?,
        };
        let cell = global.map(|c| {
            if suppress_single(&c, state.k) {
                ResultCell::Visible {
                    family: c.family,
                    wiki: c.wiki,
                    namespace: c.namespace,
                    events: c.events,
                    users: c.users,
                }
            } else {
                ResultCell::Other {
                    events: c.events,
                    users: c.users,
                }
            }
        });
        let mut cells: Vec<ResultCell> = cell.into_iter().collect();
        apply_dp_noise(&mut cells, state.dp_enabled, state.dp_epsilon);
        return Ok(StatsResponse {
            window_start: q.window,
            level: q.level,
            cells,
        });
    }

    let parent = parent_key(&q)?;
    let (parent_cell, children) = match q.estimator {
        Estimator::Exact => (
            crate::db::fetch_cell(&state.pool, q.window, &parent).await?,
            crate::db::fetch_children(&state.pool, q.window, &parent).await?,
        ),
        Estimator::Hll => (
            crate::db::fetch_cell_hll(&state.pool, q.window, &parent).await?,
            crate::db::fetch_children_hll(&state.pool, q.window, &parent).await?,
        ),
    };
    let parent_users = parent_cell.map(|c| c.users).unwrap_or(0);
    let mut cells = suppress(parent_users, children, state.k);
    apply_dp_noise(&mut cells, state.dp_enabled, state.dp_epsilon);

    Ok(StatsResponse {
        window_start: q.window,
        level: q.level,
        cells,
    })
}

/// Applies calibrated Laplace noise (see `dp.rs`) to every released cell's
/// `users` count, in place. The suppression decision itself (which cells
/// made it into `cells` at all) is made beforehand on the true count —
/// only the released value is noised, not the threshold check.
fn apply_dp_noise(cells: &mut [ResultCell], enabled: bool, epsilon: f64) {
    if !enabled {
        return;
    }
    let mut rng = rand::thread_rng();
    for cell in cells.iter_mut() {
        let users = match cell {
            ResultCell::Visible { users, .. } => users,
            ResultCell::Other { users, .. } => users,
        };
        *users = crate::dp::noisy_count(*users, crate::dp::USER_COUNT_SENSITIVITY, epsilon, &mut rng);
    }
}

fn parent_key(q: &StatsQuery) -> anyhow::Result<CellKey> {
    match q.level {
        LEVEL_FAMILY => Ok(CellKey::global()),
        LEVEL_WIKI => Ok(CellKey {
            level: LEVEL_FAMILY,
            family: q.family.clone().unwrap(),
            wiki: ROLLUP_STR.to_string(),
            namespace: ROLLUP_NAMESPACE,
        }),
        LEVEL_NAMESPACE => Ok(CellKey {
            level: LEVEL_WIKI,
            family: q.family.clone().unwrap(),
            wiki: q.wiki.clone().unwrap(),
            namespace: ROLLUP_NAMESPACE,
        }),
        _ => anyhow::bail!("unsupported level"),
    }
}

fn internal_error(e: anyhow::Error) -> (StatusCode, Json<ErrorBody>) {
    tracing::error!(error = %e, "query-api internal error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: "internal error".to_string(),
        }),
    )
}

#[derive(Debug, Serialize)]
pub struct LatestWindowResponse {
    pub window_start: Option<DateTime<Utc>>,
}

/// `GET /v1/windows/latest`: the most recent window that every partition
/// has fully flushed, i.e. the newest window safe to query.
pub async fn get_latest_window(
    State(state): State<AppState>,
) -> Result<Json<LatestWindowResponse>, (StatusCode, Json<ErrorBody>)> {
    let min_watermark = crate::db::min_flushed_watermark(&state.pool)
        .await
        .map_err(internal_error)?;
    let window_start = min_watermark.map(|wm| common::window::window_start(wm) - common::window::WINDOW_SIZE);
    Ok(Json(LatestWindowResponse { window_start }))
}

pub async fn healthz() -> StatusCode {
    StatusCode::OK
}

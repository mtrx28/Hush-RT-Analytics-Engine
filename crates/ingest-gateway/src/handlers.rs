use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::Json;
use chrono::Utc;
use common::{Event, EventBatch};
use serde::Serialize;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

use crate::producer::QueuedMessage;
use crate::AppState;

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

/// `POST /v1/events`: validates, pseudonymizes, and enqueues a batch of
/// events for Kafka production.
///
/// Returns 202 once every event in the batch has been accepted onto the
/// internal channel, 400 if the batch itself or any event in it is
/// malformed, and 503 (with `Retry-After`) if the channel to the Kafka
/// producer is full — callers should back off and retry rather than the
/// gateway buffering unboundedly.
pub async fn post_events(
    State(state): State<AppState>,
    Json(batch): Json<EventBatch>,
) -> Result<StatusCode, (StatusCode, HeaderMap, Json<ErrorBody>)> {
    if batch.events.is_empty() {
        return Err(bad_request("event batch must not be empty"));
    }
    if batch.events.len() > state.max_batch_events {
        return Err(bad_request(&format!(
            "batch of {} events exceeds max of {}",
            batch.events.len(),
            state.max_batch_events
        )));
    }

    let now = Utc::now();
    for event in &batch.events {
        if let Err(e) = event.validate(now) {
            metrics::counter!("ingest_validation_errors_total").increment(1);
            return Err(bad_request(&e.to_string()));
        }
    }

    for event in batch.events {
        let queued = pseudonymize_and_serialize(&state, event);
        if let Err(TrySendError::Full(_)) = state.tx.try_send(queued) {
            metrics::counter!("ingest_backpressure_503_total").increment(1);
            warn!("producer channel full, rejecting batch with 503");
            let mut headers = HeaderMap::new();
            headers.insert("Retry-After", HeaderValue::from_static("1"));
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                headers,
                Json(ErrorBody {
                    error: "ingest channel full, retry shortly".to_string(),
                }),
            ));
        }
    }

    metrics::counter!("ingest_events_accepted_total").increment(1);
    Ok(StatusCode::ACCEPTED)
}

fn pseudonymize_and_serialize(state: &AppState, mut event: Event) -> QueuedMessage {
    let key = state.pseudonymizer.pseudonymize(&event.user);
    event.user = key.clone();
    let payload = serde_json::to_vec(&event).expect("Event always serializes");
    QueuedMessage { key, payload }
}

fn bad_request(msg: &str) -> (StatusCode, HeaderMap, Json<ErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        HeaderMap::new(),
        Json(ErrorBody {
            error: msg.to_string(),
        }),
    )
}

pub async fn healthz() -> StatusCode {
    StatusCode::OK
}

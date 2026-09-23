use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single ingested activity event, already normalized to Hush's schema.
///
/// `user` holds the raw username on the wire between `wiki-source`/`loadgen`
/// and `ingest-gateway`; the gateway replaces it with an HMAC pseudonym
/// before the event is ever produced to Kafka or seen downstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub event_id: Uuid,
    pub user: String,
    pub ts: DateTime<Utc>,
    pub family: String,
    pub wiki: String,
    pub namespace: i32,
    pub kind: EventKind,
    pub is_bot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Edit,
    New,
    Log,
    Categorize,
}

/// A batch of events as sent by wiki-source/loadgen to ingest-gateway's
/// `POST /v1/events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
}

#[derive(Debug, thiserror::Error)]
pub enum EventValidationError {
    #[error("event {0} has empty user field")]
    EmptyUser(Uuid),
    #[error("event {0} has empty wiki field")]
    EmptyWiki(Uuid),
    #[error("event {0} has empty family field")]
    EmptyFamily(Uuid),
    #[error("event {0} has negative namespace {1}")]
    NegativeNamespace(Uuid, i32),
    #[error("event {0} timestamp {1} is too far in the future")]
    TimestampInFuture(Uuid, DateTime<Utc>),
}

/// How far into the future an event timestamp may be before it's rejected.
/// Generous enough to absorb clock skew between wiki-source and the gateway.
pub const MAX_FUTURE_SKEW_SECS: i64 = 60;

impl Event {
    pub fn validate(&self, now: DateTime<Utc>) -> Result<(), EventValidationError> {
        if self.user.trim().is_empty() {
            return Err(EventValidationError::EmptyUser(self.event_id));
        }
        if self.wiki.trim().is_empty() {
            return Err(EventValidationError::EmptyWiki(self.event_id));
        }
        if self.family.trim().is_empty() {
            return Err(EventValidationError::EmptyFamily(self.event_id));
        }
        if self.namespace < 0 {
            return Err(EventValidationError::NegativeNamespace(
                self.event_id,
                self.namespace,
            ));
        }
        let max_future = now + chrono::Duration::seconds(MAX_FUTURE_SKEW_SECS);
        if self.ts > max_future {
            return Err(EventValidationError::TimestampInFuture(
                self.event_id,
                self.ts,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event() -> Event {
        Event {
            event_id: Uuid::new_v4(),
            user: "Alice".to_string(),
            ts: Utc::now(),
            family: "wikipedia".to_string(),
            wiki: "enwiki".to_string(),
            namespace: 0,
            kind: EventKind::Edit,
            is_bot: false,
        }
    }

    #[test]
    fn valid_event_passes() {
        let e = sample_event();
        assert!(e.validate(Utc::now()).is_ok());
    }

    #[test]
    fn empty_user_rejected() {
        let mut e = sample_event();
        e.user = "".to_string();
        assert!(matches!(
            e.validate(Utc::now()),
            Err(EventValidationError::EmptyUser(_))
        ));
    }

    #[test]
    fn future_timestamp_rejected() {
        let mut e = sample_event();
        e.ts = Utc::now() + chrono::Duration::hours(1);
        assert!(matches!(
            e.validate(Utc::now()),
            Err(EventValidationError::TimestampInFuture(_, _))
        ));
    }

    #[test]
    fn negative_namespace_rejected() {
        let mut e = sample_event();
        e.namespace = -1;
        assert!(matches!(
            e.validate(Utc::now()),
            Err(EventValidationError::NegativeNamespace(_, _))
        ));
    }
}

use chrono::{DateTime, Utc};
use common::{Event, EventKind};
use serde::Deserialize;
use uuid::Uuid;

/// A recent-change message from the Wikimedia EventStreams `recentchange`
/// stream. Only the fields Hush needs are modeled; everything else in the
/// upstream JSON is ignored by serde.
#[derive(Debug, Deserialize)]
pub struct RecentChange {
    #[serde(rename = "type")]
    pub kind: String,
    pub namespace: i32,
    pub user: String,
    pub bot: bool,
    pub wiki: String,
    /// Unix seconds.
    pub timestamp: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("unknown recentchange type: {0}")]
    UnknownKind(String),
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(i64),
}

/// Best-effort mapping from a Wikimedia `wiki` database name (e.g.
/// `enwiki`, `enwiktionary`, `wikidatawiki`, `commonswiki`) to the project
/// family Hush groups it under. Wikimedia doesn't publish this mapping as
/// part of the stream itself, so it's inferred from the well-known dbname
/// suffix conventions; anything unrecognized falls back to "other".
pub fn family_for_wiki(wiki: &str) -> &'static str {
    match wiki {
        "wikidatawiki" => "wikidata",
        "commonswiki" => "commons",
        "specieswiki" => "wikispecies",
        "mediawikiwiki" => "mediawiki",
        w if w.ends_with("wiktionary") => "wiktionary",
        w if w.ends_with("wikinews") => "wikinews",
        w if w.ends_with("wikibooks") => "wikibooks",
        w if w.ends_with("wikiquote") => "wikiquote",
        w if w.ends_with("wikisource") => "wikisource",
        w if w.ends_with("wikiversity") => "wikiversity",
        w if w.ends_with("wikivoyage") => "wikivoyage",
        w if w.ends_with("wiki") => "wikipedia",
        _ => "other",
    }
}

fn kind_for_str(s: &str) -> Result<EventKind, MappingError> {
    match s {
        "edit" => Ok(EventKind::Edit),
        "new" => Ok(EventKind::New),
        "log" => Ok(EventKind::Log),
        "categorize" => Ok(EventKind::Categorize),
        other => Err(MappingError::UnknownKind(other.to_string())),
    }
}

pub fn to_event(rc: RecentChange) -> Result<Event, MappingError> {
    let ts = DateTime::<Utc>::from_timestamp(rc.timestamp, 0)
        .ok_or(MappingError::InvalidTimestamp(rc.timestamp))?;
    let family = family_for_wiki(&rc.wiki).to_string();
    Ok(Event {
        event_id: Uuid::new_v4(),
        user: rc.user,
        ts,
        family,
        wiki: rc.wiki,
        namespace: rc.namespace,
        kind: kind_for_str(&rc.kind)?,
        is_bot: rc.bot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_families() {
        assert_eq!(family_for_wiki("enwiki"), "wikipedia");
        assert_eq!(family_for_wiki("knwiki"), "wikipedia");
        assert_eq!(family_for_wiki("enwiktionary"), "wiktionary");
        assert_eq!(family_for_wiki("wikidatawiki"), "wikidata");
        assert_eq!(family_for_wiki("commonswiki"), "commons");
        assert_eq!(family_for_wiki("totally-unknown"), "other");
    }

    #[test]
    fn converts_recentchange_to_event() {
        let rc = RecentChange {
            kind: "edit".to_string(),
            namespace: 0,
            user: "Alice".to_string(),
            bot: false,
            wiki: "enwiki".to_string(),
            timestamp: 1_700_000_000,
        };
        let event = to_event(rc).unwrap();
        assert_eq!(event.family, "wikipedia");
        assert_eq!(event.wiki, "enwiki");
        assert_eq!(event.kind, EventKind::Edit);
        assert!(!event.is_bot);
    }

    #[test]
    fn rejects_unknown_kind() {
        let rc = RecentChange {
            kind: "wat".to_string(),
            namespace: 0,
            user: "Alice".to_string(),
            bot: false,
            wiki: "enwiki".to_string(),
            timestamp: 1_700_000_000,
        };
        assert!(matches!(to_event(rc), Err(MappingError::UnknownKind(_))));
    }
}

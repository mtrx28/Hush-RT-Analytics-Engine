use crate::event::Event;
use serde::{Deserialize, Serialize};

/// Sentinel value for `family`/`wiki` at levels that haven't drilled down
/// that far yet (i.e. the cell is a rollup across all values at that axis).
pub const ROLLUP_STR: &str = "*";
/// Sentinel value for `namespace` when the cell hasn't drilled into a
/// specific namespace.
pub const ROLLUP_NAMESPACE: i32 = -1;

pub const LEVEL_GLOBAL: i16 = 0;
pub const LEVEL_FAMILY: i16 = 1;
pub const LEVEL_WIKI: i16 = 2;
pub const LEVEL_NAMESPACE: i16 = 3;

/// Identifies one aggregation cell in the 4-level dimension hierarchy:
/// global -> family -> family/wiki -> family/wiki/namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CellKey {
    pub level: i16,
    pub family: String,
    pub wiki: String,
    pub namespace: i32,
}

impl CellKey {
    pub fn global() -> Self {
        CellKey {
            level: LEVEL_GLOBAL,
            family: ROLLUP_STR.to_string(),
            wiki: ROLLUP_STR.to_string(),
            namespace: ROLLUP_NAMESPACE,
        }
    }

    /// Returns the 4 cells (one per level) that a single event contributes to.
    pub fn cells_for_event(event: &Event) -> [CellKey; 4] {
        [
            CellKey::global(),
            CellKey {
                level: LEVEL_FAMILY,
                family: event.family.clone(),
                wiki: ROLLUP_STR.to_string(),
                namespace: ROLLUP_NAMESPACE,
            },
            CellKey {
                level: LEVEL_WIKI,
                family: event.family.clone(),
                wiki: event.wiki.clone(),
                namespace: ROLLUP_NAMESPACE,
            },
            CellKey {
                level: LEVEL_NAMESPACE,
                family: event.family.clone(),
                wiki: event.wiki.clone(),
                namespace: event.namespace,
            },
        ]
    }

    /// The key of this cell's parent one level up, or `None` at the global root.
    pub fn parent(&self) -> Option<CellKey> {
        match self.level {
            LEVEL_GLOBAL => None,
            LEVEL_FAMILY => Some(CellKey::global()),
            LEVEL_WIKI => Some(CellKey {
                level: LEVEL_FAMILY,
                family: self.family.clone(),
                wiki: ROLLUP_STR.to_string(),
                namespace: ROLLUP_NAMESPACE,
            }),
            LEVEL_NAMESPACE => Some(CellKey {
                level: LEVEL_WIKI,
                family: self.family.clone(),
                wiki: self.wiki.clone(),
                namespace: ROLLUP_NAMESPACE,
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;
    use chrono::Utc;
    use uuid::Uuid;

    fn sample_event() -> Event {
        Event {
            event_id: Uuid::new_v4(),
            user: "Alice".to_string(),
            ts: Utc::now(),
            family: "wikipedia".to_string(),
            wiki: "knwiki".to_string(),
            namespace: 0,
            kind: EventKind::Edit,
            is_bot: false,
        }
    }

    #[test]
    fn event_contributes_to_four_cells() {
        let e = sample_event();
        let cells = CellKey::cells_for_event(&e);
        assert_eq!(cells.len(), 4);
        assert_eq!(cells[0], CellKey::global());
        assert_eq!(cells[1].level, LEVEL_FAMILY);
        assert_eq!(cells[1].family, "wikipedia");
        assert_eq!(cells[2].level, LEVEL_WIKI);
        assert_eq!(cells[2].wiki, "knwiki");
        assert_eq!(cells[3].level, LEVEL_NAMESPACE);
        assert_eq!(cells[3].namespace, 0);
    }

    #[test]
    fn parent_chain_reaches_global() {
        let e = sample_event();
        let leaf = CellKey::cells_for_event(&e)[3].clone();
        let wiki = leaf.parent().unwrap();
        let family = wiki.parent().unwrap();
        let global = family.parent().unwrap();
        assert_eq!(global, CellKey::global());
        assert!(global.parent().is_none());
    }
}

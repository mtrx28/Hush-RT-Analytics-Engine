use serde::Serialize;

/// One cell's summed counts, before or after suppression.
#[derive(Debug, Clone, PartialEq)]
pub struct RawCell {
    pub family: String,
    pub wiki: String,
    pub namespace: i32,
    pub events: u64,
    pub users: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind")]
pub enum ResultCell {
    Visible {
        family: String,
        wiki: String,
        namespace: i32,
        events: u64,
        users: u64,
    },
    /// One or more children merged together because none of them, or their
    /// remaining total, met the privacy threshold on its own.
    Other { events: u64, users: u64 },
}

/// Minimum distinct users a cell must have to be released on its own.
pub const DEFAULT_K: u64 = 10;

/// Applies Hush's k-threshold suppression rules to one parent's children.
///
/// Rule 1 (primary suppression): any child with `users < k` is hidden.
///
/// Rule 2 (secondary suppression, differencing defense): suppose the parent
/// has 50 users and the visible children sum to 49 — subtraction reveals
/// exactly 1 hidden user. To prevent that, we keep pulling the *smallest*
/// visible child into the hidden group until the hidden group itself has
/// at least `k` users (or nothing visible is left to pull). All hidden
/// cells are then merged into a single "Other" row.
///
/// Known limitation, documented in the README: this check sums the hidden
/// children's `users` counts directly. A user who appears in more than one
/// hidden child is counted once per child, so the "hidden group has >= k
/// users" check is an overestimate of true distinct users — approximate,
/// not exact. Calibrated noise (differential privacy) is the proper fix
/// and is tracked as a stretch goal.
pub fn suppress(parent_users: u64, children: Vec<RawCell>, k: u64) -> Vec<ResultCell> {
    let mut visible: Vec<RawCell> = Vec::new();
    let mut hidden: Vec<RawCell> = Vec::new();

    for child in children {
        if child.users < k {
            hidden.push(child);
        } else {
            visible.push(child);
        }
    }

    let hidden_users: u64 = hidden.iter().map(|c| c.users).sum();
    // Sort ascending by users so we always pull in the least-informative
    // child first when the hidden group still isn't large enough.
    visible.sort_by_key(|c| c.users);

    let residual = parent_users.saturating_sub(visible_total(&visible) + hidden_users);
    let mut hidden_effective = hidden_users + residual;

    while hidden_effective > 0 && hidden_effective < k && !visible.is_empty() {
        let pulled = visible.remove(0);
        hidden_effective += pulled.users;
        hidden.push(pulled);
    }

    let mut result: Vec<ResultCell> = visible
        .into_iter()
        .map(|c| ResultCell::Visible {
            family: c.family,
            wiki: c.wiki,
            namespace: c.namespace,
            events: c.events,
            users: c.users,
        })
        .collect();

    // If we ran out of visible children to pull in and the hidden group
    // still can't clear k, releasing it anyway would itself violate the
    // threshold it exists to enforce. Better to release nothing for that
    // group at all than to leak an exact small count.
    if !hidden.is_empty() && hidden_effective >= k {
        let events: u64 = hidden.iter().map(|c| c.events).sum();
        // Report `hidden_effective`, not just the sum of enumerated hidden
        // cells: it also includes the residual (parent users not accounted
        // for by any visible or hidden child we know about). Using only
        // the enumerated sum here would under-report the true hidden
        // count and could release a number below k even though the
        // threshold check above used the full (enumerated + residual) total.
        result.push(ResultCell::Other {
            events,
            users: hidden_effective,
        });
    }

    result
}

fn visible_total(visible: &[RawCell]) -> u64 {
    visible.iter().map(|c| c.users).sum()
}

/// Applies the primary threshold directly to a single cell (used for the
/// top-level global/family/wiki cell itself, which has no siblings to hide
/// behind an "Other" bucket).
pub fn suppress_single(cell: &RawCell, k: u64) -> bool {
    cell.users >= k
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(name: &str, users: u64) -> RawCell {
        RawCell {
            family: "wikipedia".to_string(),
            wiki: name.to_string(),
            namespace: -1,
            events: users * 3,
            users,
        }
    }

    #[test]
    fn hides_small_children_below_k() {
        // Two small children (6 + 5 = 11) together already clear k=10 as an
        // "Other" bucket, so the big one doesn't need to be pulled in too.
        let children = vec![cell("big", 50), cell("small1", 6), cell("small2", 5)];
        let result = suppress(61, children, 10);
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], ResultCell::Visible { users: 50, .. }));
        assert!(matches!(result[1], ResultCell::Other { users: 11, .. }));
    }

    #[test]
    fn differencing_attack_is_defeated() {
        // Parent has 50 users. One child (45) is visible; the rest (5) is
        // hidden. Naive suppression alone would let someone compute
        // 50 - 45 = 5 for the hidden group, right at the edge, but the
        // released "Other" row for 5 already fails k=10 - so we must pull
        // in another child until the hidden group reads >= 10.
        let children = vec![cell("big", 45), cell("small", 5)];
        let result = suppress(50, children, 10);

        // "small" (5) is directly below k, so it's hidden by rule 1.
        // Hidden total = 5, still < k=10, so rule 2 also pulls "big" (45)
        // in, since it's the only visible cell left.
        assert_eq!(result.len(), 1);
        match &result[0] {
            ResultCell::Other { users, .. } => assert_eq!(*users, 50),
            _ => panic!("expected everything suppressed into Other"),
        }
    }

    #[test]
    fn residual_between_parent_and_visible_children_is_treated_as_hidden() {
        // Parent has 60 users; only one visible child worth 55 is passed
        // in (the rest of the cells were never released as siblings), so
        // subtraction would reveal a hidden group of 5. That residual must
        // count toward the k-check even though no RawCell represents it,
        // and the released total must include it too (60, not just the
        // pulled-in child's own 55) or the released number would itself
        // read as only 55 when 5 more people are actually folded in.
        let children = vec![cell("big", 55)];
        let result = suppress(60, children, 10);
        // hidden_effective starts at residual=5 (<k), pulls in "big" (55).
        assert_eq!(result.len(), 1);
        match &result[0] {
            ResultCell::Other { users, .. } => assert_eq!(*users, 60),
            _ => panic!("expected big to be pulled into Other"),
        }
    }

    #[test]
    fn all_children_above_k_stay_visible() {
        let children = vec![cell("a", 20), cell("b", 30)];
        let result = suppress(50, children, 10);
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|c| matches!(c, ResultCell::Visible { .. })));
    }

    #[test]
    fn single_cell_below_k_is_suppressed() {
        let c = cell("solo", 4);
        assert!(!suppress_single(&c, 10));
        let c2 = cell("solo", 10);
        assert!(suppress_single(&c2, 10));
    }

    #[test]
    fn hidden_group_that_can_never_reach_k_is_released_as_nothing() {
        // Only one tiny child exists at all; there's nothing to pull in to
        // bring the hidden group up to k, so it must not be released as an
        // under-threshold "Other" row.
        let children = vec![cell("tiny", 3)];
        let result = suppress(3, children, 10);
        assert!(result.is_empty());
    }

    proptest::proptest! {
        /// For any random parent/children split, no cell `suppress` returns
        /// — visible or "Other" — ever describes fewer than k people. This
        /// is the core k-anonymity invariant: nothing released can be
        /// subtracted down to reveal a smaller-than-k group, because
        /// nothing released is ever smaller than k in the first place.
        #[test]
        fn no_released_cell_is_ever_below_k(
            child_users in proptest::collection::vec(0u64..80, 0..12),
            residual in 0u64..20,
            k in 1u64..30,
        ) {
            let children: Vec<RawCell> = child_users
                .iter()
                .enumerate()
                .map(|(i, &users)| cell(&format!("c{i}"), users))
                .collect();
            let parent_users = child_users.iter().sum::<u64>() + residual;

            let result = suppress(parent_users, children, k);

            for cell in &result {
                let users = match cell {
                    ResultCell::Visible { users, .. } => *users,
                    ResultCell::Other { users, .. } => *users,
                };
                proptest::prop_assert!(
                    users >= k,
                    "released a cell with {} users, below k={}",
                    users, k
                );
            }
        }
    }
}

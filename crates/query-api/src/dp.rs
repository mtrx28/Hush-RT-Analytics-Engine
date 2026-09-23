//! Calibrated (Laplace-mechanism) differential privacy noise — the
//! "proper fix" flagged in `docs/decisions.md` for the k-anonymity
//! suppression's known gap: `suppress()`'s secondary-suppression check
//! sums hidden children's user counts directly, which double-counts a
//! user appearing in more than one hidden child, making its "hidden group
//! has >= k users" check an overestimate rather than exact. Thresholds
//! alone can't fully close that; adding noise means every released number
//! carries uncertainty regardless of how it was internally accounted for.
//!
//! This module implements the standard (epsilon-)differential-privacy
//! Laplace mechanism: for a query with sensitivity `s` (how much one
//! individual's presence/absence can change the true answer) and a privacy
//! budget `epsilon`, releasing `true_value + Laplace(0, s/epsilon)` gives
//! epsilon-DP.
//!
//! Scope, stated plainly: this adds noise to a single query's `users`
//! count. It does not track a cumulative privacy budget across repeated
//! queries (real DP composition), and it does not add noise to `events`
//! (unbounded per-user sensitivity there — a single user can generate
//! arbitrarily many events, so the same fixed sensitivity/epsilon
//! calibration wouldn't give a clean guarantee without separately clipping
//! per-user event contributions). Both are natural next steps, not done
//! here — opt in via `DP_ENABLED`/`DP_EPSILON`; the default (`DP_ENABLED`
//! unset) leaves query-api's behavior exactly as it was with thresholds
//! alone.

use rand::Rng;

/// Sensitivity of a distinct-user count: a single user's presence or
/// absence changes the count by exactly 1, since each user is counted at
/// most once per cell.
pub const USER_COUNT_SENSITIVITY: f64 = 1.0;

/// Draws one sample from Laplace(0, b) via inverse-CDF sampling:
/// for u ~ Uniform(-0.5, 0.5), `-b * sign(u) * ln(1 - 2|u|)` is
/// Laplace(0, b)-distributed.
fn sample_laplace(b: f64, rng: &mut impl Rng) -> f64 {
    let u: f64 = rng.gen_range(-0.5..0.5);
    -b * u.signum() * (1.0 - 2.0 * u.abs()).ln()
}

/// Adds epsilon-DP Laplace noise to a count, clamped to be non-negative
/// (a negative count is meaningless, and clamping only at 0 — never at k —
/// keeps the released value a valid, if occasionally under-k-looking,
/// sample from the calibrated mechanism rather than silently distorting
/// its distribution to hide that noise was applied).
pub fn noisy_count(true_value: u64, sensitivity: f64, epsilon: f64, rng: &mut impl Rng) -> u64 {
    let b = sensitivity / epsilon;
    let noisy = true_value as f64 + sample_laplace(b, rng);
    noisy.max(0.0).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn noise_is_unbiased_on_average() {
        let mut rng = StdRng::seed_from_u64(42);
        let true_value = 1000u64;
        let epsilon = 1.0;
        let n = 20_000;

        let sum: f64 = (0..n)
            .map(|_| noisy_count(true_value, USER_COUNT_SENSITIVITY, epsilon, &mut rng) as f64)
            .sum();
        let mean = sum / n as f64;

        // Laplace noise is mean-zero, so the noisy mean should converge to
        // the true value; allow a generous tolerance since this is a
        // statistical (not exact) property.
        let relative_error = ((mean - true_value as f64) / true_value as f64).abs();
        assert!(relative_error < 0.02, "mean={mean}, true_value={true_value}");
    }

    #[test]
    fn smaller_epsilon_means_more_noise() {
        let mut rng = StdRng::seed_from_u64(7);
        let true_value = 1000u64;
        let n = 5_000;

        let variance_at = |epsilon: f64, rng: &mut StdRng| -> f64 {
            let samples: Vec<f64> = (0..n)
                .map(|_| noisy_count(true_value, USER_COUNT_SENSITIVITY, epsilon, rng) as f64)
                .collect();
            let mean: f64 = samples.iter().sum::<f64>() / n as f64;
            samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64
        };

        let tight_privacy_variance = variance_at(0.1, &mut rng); // small epsilon = more noise
        let loose_privacy_variance = variance_at(5.0, &mut rng); // large epsilon = less noise
        assert!(
            tight_privacy_variance > loose_privacy_variance,
            "tight={tight_privacy_variance}, loose={loose_privacy_variance}"
        );
    }

    #[test]
    fn never_returns_negative_even_for_small_true_values() {
        let mut rng = StdRng::seed_from_u64(1);
        for _ in 0..10_000 {
            // A tiny true value with loose privacy (large noise) is the
            // case most likely to go negative before clamping.
            let _ = noisy_count(2, USER_COUNT_SENSITIVITY, 0.05, &mut rng);
        }
    }
}

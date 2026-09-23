//! A small HyperLogLog cardinality sketch, implemented from scratch rather
//! than pulled in as a dependency so its accuracy/memory trade-off can be
//! measured and reasoned about directly (see `docs/hyperloglog.md`).
//!
//! Hush's exact per-partition `HashSet<String>` (see `aggregator::window`)
//! already gives exact distinct-user counts for free, because Kafka
//! messages are keyed by user so a user's events never split across
//! partitions — HLL doesn't buy correctness here. What it buys is a fixed,
//! tiny memory footprint per cell regardless of how many distinct users
//! that cell ever sees, at the cost of a small, well-understood relative
//! error. It's wired in as an optional second estimate
//! (`query-api`'s `estimator=hll` param) specifically to make that
//! trade-off visible and measurable, not to replace the exact path.

const HASH_SEED: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
const HASH_PRIME: u64 = 0x100000001b3;

/// FNV-1a's high bits have weak avalanche for short inputs — for near-
/// identical short strings like "user-0".."user-99", that alone leaves the
/// top bits (which we use for the register index) barely varying, so
/// almost every insert would land in the same handful of registers. Running
/// the FNV output through MurmurHash3's `fmix64` finalizer spreads entropy
/// across all 64 bits regardless of input length, which is exactly the
/// property a bucketing hash needs.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

fn hash_item(bytes: &[u8]) -> u64 {
    let mut hash = HASH_SEED;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(HASH_PRIME);
    }
    fmix64(hash)
}

/// Default precision: 2^12 = 4096 registers (4KB), standard error
/// ~1.04/sqrt(4096) ≈ 1.6%.
pub const DEFAULT_PRECISION: u8 = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    pub fn new(precision: u8) -> Self {
        assert!((4..=16).contains(&precision), "precision must be in 4..=16");
        HyperLogLog {
            precision,
            registers: vec![0u8; 1 << precision],
        }
    }

    pub fn with_default_precision() -> Self {
        Self::new(DEFAULT_PRECISION)
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Size in bytes of the register array — the whole point of using a
    /// sketch instead of an exact set.
    pub fn memory_bytes(&self) -> usize {
        self.registers.len()
    }

    pub fn insert(&mut self, item: &str) {
        let hash = hash_item(item.as_bytes());
        let m_bits = self.precision as u32;
        let index = (hash >> (64 - m_bits)) as usize;
        // Remaining bits (with a guard 1-bit appended conceptually via the
        // leading_zeros+1 formula) determine the register's rank.
        let remaining = hash << m_bits;
        let rho = (remaining.leading_zeros() + 1).min(64 - m_bits) as u8;
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    /// Merges another sketch (of the same precision) into this one by
    /// taking the element-wise max of registers — the standard HLL union.
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(self.precision, other.precision, "cannot merge sketches of different precision");
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// Estimates the number of distinct items inserted, using the standard
    /// HLL estimator with small-range linear-counting correction.
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha_m = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let sum_inv: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw_estimate = alpha_m * m * m / sum_inv;

        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if raw_estimate <= 2.5 * m && zeros > 0 {
            // Linear counting for the small-cardinality regime, where the
            // raw HLL estimator is biased.
            m * (m / zeros as f64).ln()
        } else {
            raw_estimate
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.registers.len());
        out.push(self.precision);
        out.extend_from_slice(&self.registers);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (&precision, registers) = bytes.split_first()?;
        if registers.len() != 1usize << precision {
            return None;
        }
        Some(HyperLogLog {
            precision,
            registers: registers.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn empty_sketch_estimates_zero() {
        let hll = HyperLogLog::with_default_precision();
        assert_eq!(hll.estimate().round() as u64, 0);
    }

    #[test]
    fn accuracy_within_expected_error_bound_at_several_cardinalities() {
        // Standard error for p=12 (m=4096) is ~1.04/sqrt(4096) ≈ 1.6%.
        // We allow a generous 4x margin (6.5%) to keep this test from
        // being flaky on an unlucky hash draw, while still catching a
        // genuinely broken estimator.
        let stderr = 1.04 / (4096f64).sqrt();
        let max_relative_error = stderr * 4.0;

        for &n in &[100u64, 1_000, 10_000, 100_000] {
            let mut hll = HyperLogLog::with_default_precision();
            for i in 0..n {
                hll.insert(&format!("user-{i}"));
            }
            let estimate = hll.estimate();
            let relative_error = ((estimate - n as f64) / n as f64).abs();
            eprintln!(
                "n={n}: hll_estimate={estimate:.1}, relative_error={:.2}%, memory_bytes={}, exact_hashset_bytes_approx={}",
                relative_error * 100.0,
                hll.memory_bytes(),
                n as usize * (24 + 8), // rough: String heap alloc + HashSet bucket overhead
            );
            assert!(
                relative_error <= max_relative_error,
                "n={n}: estimate={estimate}, relative_error={relative_error:.4}, allowed={max_relative_error:.4}"
            );
        }
    }

    #[test]
    fn merge_matches_union_of_inserted_sets() {
        let mut a = HyperLogLog::with_default_precision();
        let mut b = HyperLogLog::with_default_precision();
        let mut true_union: HashSet<String> = HashSet::new();

        // `a` sees users 0..5000, `b` sees 2000..7000 (a 3000-user overlap),
        // so the true union has 7000 distinct users.
        for i in 0..5000 {
            let user = format!("user-{i}");
            a.insert(&user);
            true_union.insert(user);
        }
        for i in 2000..7000 {
            let user = format!("user-{i}");
            b.insert(&user);
            true_union.insert(user);
        }

        a.merge(&b);
        let estimate = a.estimate();
        let relative_error = ((estimate - true_union.len() as f64) / true_union.len() as f64).abs();
        assert!(
            relative_error < 0.1,
            "merged estimate {estimate} too far from true union size {}",
            true_union.len()
        );
    }

    #[test]
    fn round_trips_through_bytes() {
        let mut hll = HyperLogLog::with_default_precision();
        for i in 0..1000 {
            hll.insert(&format!("user-{i}"));
        }
        let bytes = hll.to_bytes();
        let restored = HyperLogLog::from_bytes(&bytes).unwrap();
        assert_eq!(hll, restored);
        assert_eq!(hll.estimate(), restored.estimate());
    }

    #[test]
    fn memory_is_fixed_regardless_of_cardinality() {
        let mut small = HyperLogLog::with_default_precision();
        small.insert("only-one-user");
        let mut large = HyperLogLog::with_default_precision();
        for i in 0..1_000_000 {
            large.insert(&format!("user-{i}"));
        }
        assert_eq!(small.memory_bytes(), large.memory_bytes());
        assert_eq!(small.memory_bytes(), 4096);
    }
}

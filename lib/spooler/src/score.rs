//! Sainte-Laguë scores in bytes, as fixed point.
//!
//! The objective is Σ (L − T)² / T over nodes, with L a node's load and T its
//! target. Handing a spool of s bytes to a node changes the objective by
//! −s × claim, and taking one away by −s × release, so a move from a to b
//! cuts it by s × (release(a) + claim(b)).

use tape_core::types::StorageUnits;

/// Fractional bits in a score
pub const SCORE_BITS: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Score(pub i128);

impl Score {
    /// A node's claim on one more spool of `size` bytes: (2(T − L) − s) / T
    pub fn claim(target: StorageUnits, load: StorageUnits, size: StorageUnits) -> Self {
        let surplus = 2 * (target.as_u128() as i128 - load.as_u128() as i128);
        Self::per_target(surplus - size.as_u128() as i128, target)
    }

    /// What a node gains by giving up a spool of `size` bytes: (2(L − T) − s) / T
    pub fn release(target: StorageUnits, load: StorageUnits, size: StorageUnits) -> Self {
        let excess = 2 * (load.as_u128() as i128 - target.as_u128() as i128);
        Self::per_target(excess - size.as_u128() as i128, target)
    }

    // A zero target (no stake) divides by one byte, so any load on it scores as far over
    fn per_target(bytes: i128, target: StorageUnits) -> Self {
        let target = target.as_u128().max(1) as i128;
        Score((bytes << SCORE_BITS) / target)
    }

    pub fn saturating_add(self, other: Score) -> Score {
        Score(self.0.saturating_add(other.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: i128 = 1 << SCORE_BITS;

    fn units(bytes: u64) -> StorageUnits {
        StorageUnits::from_bytes(bytes)
    }

    #[test]
    fn claim_rounds_at_half_a_spool() {
        // Sainte-Laguë: a node earns a spool once it is at least half a spool short
        assert_eq!(Score::claim(units(100), units(50), units(100)), Score(0));
        assert!(Score::claim(units(100), units(40), units(100)) > Score(0));
        assert!(Score::claim(units(100), units(60), units(100)) < Score(0));
    }

    #[test]
    fn scores_are_relative_to_target() {
        assert_eq!(Score::claim(units(100), units(0), units(0)), Score(2 * ONE));
        assert_eq!(Score::claim(units(1_000), units(0), units(0)), Score(2 * ONE));
        assert_eq!(Score::release(units(100), units(150), units(0)), Score(ONE));
    }

    #[test]
    fn move_gain_matches_objective() {
        // a: T 100, L 200; b: T 100, L 0; moving 100 bytes lands both on target
        let gain = Score::release(units(100), units(200), units(100))
            .saturating_add(Score::claim(units(100), units(0), units(100)));
        // objective before 100 + 100, after 0: a cut of 200 = 100 bytes × gain 2
        assert_eq!(gain, Score(2 * ONE));
    }

    #[test]
    fn zero_target_scores_far_over() {
        assert!(Score::release(units(0), units(10), units(10)) > Score(9 * ONE));
        assert!(Score::claim(units(0), units(0), units(10)) < Score(-9 * ONE));
    }

    #[test]
    fn extreme_sizes_fit() {
        let max = units(u64::MAX);
        let score = Score::claim(units(1), max, max);
        assert!(score < Score(0));
        let score = Score::release(units(1), max, units(0));
        assert!(score > Score(0));
    }
}

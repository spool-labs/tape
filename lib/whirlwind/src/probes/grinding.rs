//! Entropy-block grinding by a producer colluding with an owner that dropped a
//! fraction p: regenerate the block until the sample lands in retained data.
//!
//! Expected attempts is 1/(1-p). A rational cheater dropping a small fraction
//! hides it in barely more than one grind per led round.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const SEED: u64 = 0xC0FF_EE00_1234_5678;

/// Grinding cost for a given discard fraction.
pub struct GrindingReport {
    pub discard_fraction: f64,
    pub expected_attempts: f64,
    pub empirical_attempts: f64,
}

impl GrindingReport {
    pub fn simulate(discard_fraction: f64, trials: u64) -> Self {
        let probability = discard_fraction.clamp(0.0, 1.0);
        let retained = 1.0 - probability;
        let expected_attempts = if retained > 0.0 {
            1.0 / retained
        } else {
            f64::INFINITY
        };

        let mut rng = SmallRng::seed_from_u64(SEED);
        let cap = 1_000_000u64;
        let mut total = 0u64;
        for _ in 0..trials {
            let mut attempts = 1u64;
            // Success when the reground sample misses the discarded fraction.
            while attempts < cap && rng.gen::<f64>() < probability {
                attempts += 1;
            }
            total += attempts;
        }
        let empirical_attempts = total as f64 / trials as f64;

        Self {
            discard_fraction: probability,
            expected_attempts,
            empirical_attempts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometric_formula() {
        // Retained fraction 0.5 -> expected 2 grinds; simulation should agree.
        let report = GrindingReport::simulate(0.5, 200_000);
        assert!((report.expected_attempts - 2.0).abs() < 1e-9);
        assert!((report.empirical_attempts - report.expected_attempts).abs() < 0.1);
    }

    #[test]
    fn small_discard() {
        // A rational cheater dropping 10% hides it in ~1.11 grinds per round.
        let report = GrindingReport::simulate(0.1, 200_000);
        assert!(report.expected_attempts < 1.2);
    }
}

//! Detection of a silently discarded fraction under one uniform sample per round.
//!
//! One sample per round catches a discarded fraction p with probability p, so
//! expected rounds to first catch is 1/p and survival after l rounds is
//! (1-p)^l. Whole-node loss is caught fast; small partial loss is not.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const SEED: u64 = 0xC0FF_EE00_1234_5678;

/// Detection outcome for a given discard fraction and cadence. Expected rounds
/// is one over p, the survival list gives the miss probability after l rounds,
/// and the empirical mean is present only when p is large enough to simulate.
pub struct DetectionReport {
    pub discard_fraction: f64,
    pub cadence_secs: u64,
    pub expected_rounds: f64,
    pub expected_hours: f64,
    pub empirical_rounds: Option<f64>,
    pub survival: Vec<(u64, f64)>,
}

impl DetectionReport {
    pub fn simulate(discard_fraction: f64, cadence_secs: u64, trials: u64) -> Self {
        let probability = discard_fraction.clamp(0.0, 1.0);
        let expected_rounds = if probability > 0.0 {
            1.0 / probability
        } else {
            f64::INFINITY
        };
        let expected_hours = expected_rounds * cadence_secs as f64 / 3_600.0;

        // Only simulate when the geometric mean is small enough to be cheap.
        let empirical_rounds = if probability >= 0.05 {
            let mut rng = SmallRng::seed_from_u64(SEED);
            let cap = 1_000_000u64;
            let mut total = 0u64;
            for _ in 0..trials {
                let mut rounds = 1u64;
                while rounds < cap && rng.gen::<f64>() >= probability {
                    rounds += 1;
                }
                total += rounds;
            }
            Some(total as f64 / trials as f64)
        } else {
            None
        };

        let survival = [1u64, 8, 32, 128, 1_000]
            .iter()
            .map(|&rounds| (rounds, (1.0 - probability).powi(rounds as i32)))
            .collect();

        Self {
            discard_fraction: probability,
            cadence_secs,
            expected_rounds,
            expected_hours,
            empirical_rounds,
            survival,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empirical_mean() {
        let report = DetectionReport::simulate(0.1, 60, 50_000);
        let empirical = report.empirical_rounds.expect("empirical rounds");
        assert!((empirical - report.expected_rounds).abs() / report.expected_rounds < 0.1);
    }
}

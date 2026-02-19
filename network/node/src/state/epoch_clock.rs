use std::time::{Duration, Instant};

use tape_core::types::EpochNumber;

/// Monotonic local epoch clock.
///
/// This is intentionally independent from on-chain unix timestamps so takeover
/// windows remain stable in sim/test environments where wall-clock time and
/// chain time can diverge.
pub struct EpochClock {
    epoch: EpochNumber,
    observed_at: Instant,
}

impl EpochClock {
    pub fn new(epoch: EpochNumber) -> Self {
        Self {
            epoch,
            observed_at: Instant::now(),
        }
    }

    pub fn reset(&mut self, epoch: EpochNumber) {
        self.epoch = epoch;
        self.observed_at = Instant::now();
    }

    /// Return elapsed time for the requested epoch.
    ///
    /// If the epoch has changed since last observation, reset anchor and
    /// return zero elapsed.
    pub fn elapsed_or_reset(&mut self, epoch: EpochNumber) -> Duration {
        if self.epoch != epoch {
            self.reset(epoch);
            return Duration::from_secs(0);
        }
        self.observed_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_on_epoch_change() {
        let mut clock = EpochClock::new(EpochNumber(1));
        let elapsed = clock.elapsed_or_reset(EpochNumber(2));
        assert_eq!(elapsed, Duration::from_secs(0));
    }

    #[test]
    fn elapsed_same_epoch() {
        let mut clock = EpochClock::new(EpochNumber(1));
        let _ = clock.elapsed_or_reset(EpochNumber(1));
        let elapsed = clock.elapsed_or_reset(EpochNumber(1));
        assert!(elapsed <= Duration::from_secs(1));
    }
}

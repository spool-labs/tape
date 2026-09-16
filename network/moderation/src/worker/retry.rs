//! Doubling delays between failed attempts

use std::time::Duration;

use crate::types::ids::AttemptCount;

/// Largest doubling applied, far past any useful delay
const MAX_DOUBLINGS: u32 = 30;

/// The wait after the given number of failed attempts, capped at `max`
pub fn retry_delay(attempts: AttemptCount, initial: Duration, max: Duration) -> Duration {
    let doublings = attempts.0.saturating_sub(1).min(MAX_DOUBLINGS);
    initial.saturating_mul(1_u32 << doublings).min(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    // delays double from the initial delay and stop at the cap
    #[test]
    fn doubling() {
        let initial = Duration::from_secs(30);
        let max = Duration::from_secs(200);

        let delays: Vec<Duration> = (1..=5)
            .map(|attempt| retry_delay(AttemptCount(attempt), initial, max))
            .collect();

        assert_eq!(
            delays,
            [30, 60, 120, 200, 200].map(Duration::from_secs).to_vec()
        );
    }
}

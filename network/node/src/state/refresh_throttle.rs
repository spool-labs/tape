use std::time::{Duration, Instant};

use tape_core::types::EpochNumber;

pub struct RefreshThrottle {
    last_refresh: Option<Instant>,
    last_epoch: Option<EpochNumber>,
}

impl RefreshThrottle {
    pub fn new() -> Self {
        Self {
            last_refresh: None,
            last_epoch: None,
        }
    }

    pub fn should_skip(&self, min_interval: Duration) -> bool {
        match self.last_refresh {
            Some(t) => t.elapsed() < min_interval,
            None => false,
        }
    }

    pub fn epoch_changed(&self, epoch: EpochNumber) -> bool {
        match self.last_epoch {
            Some(last) => last != epoch,
            None => true,
        }
    }

    pub fn record(&mut self, epoch: Option<EpochNumber>) {
        self.last_refresh = Some(Instant::now());
        if let Some(epoch) = epoch {
            self.last_epoch = Some(epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_no_skip() {
        let throttle = RefreshThrottle::new();
        assert!(!throttle.should_skip(Duration::from_secs(5)));
    }

    #[test]
    fn skip_after_record() {
        let mut throttle = RefreshThrottle::new();
        throttle.record(Some(EpochNumber(1)));
        assert!(throttle.should_skip(Duration::from_secs(30)));
    }

    #[test]
    fn epoch_changed() {
        let mut throttle = RefreshThrottle::new();
        assert!(throttle.epoch_changed(EpochNumber(1)));
        throttle.record(Some(EpochNumber(1)));
        assert!(!throttle.epoch_changed(EpochNumber(1)));
        assert!(throttle.epoch_changed(EpochNumber(2)));
    }
}

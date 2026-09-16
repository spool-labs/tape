//! Whole seconds since the Unix epoch, as records store them

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A point in time in whole seconds since the Unix epoch
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Self(i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
    }

    pub fn after(self, delay: Duration) -> Self {
        let seconds = i64::try_from(delay.as_secs()).unwrap_or(i64::MAX);
        Self(self.0.saturating_add(seconds))
    }

    /// Time from `earlier` to this point, zero when `earlier` is later
    pub fn since(self, earlier: Timestamp) -> Duration {
        let seconds = self.0.saturating_sub(earlier.0);
        Duration::from_secs(u64::try_from(seconds).unwrap_or(0))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

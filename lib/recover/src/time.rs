use std::ops::Add;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Wall time in milliseconds since the unix epoch, zero for never
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WallTime(pub u64);

impl WallTime {
    /// The wall clock now
    pub fn now() -> Self {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(since.as_millis() as u64)
    }

    /// Time since `earlier`, zero when `earlier` is later
    pub fn get_time_since(self, earlier: WallTime) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

impl Add<Duration> for WallTime {
    type Output = WallTime;

    fn add(self, wait: Duration) -> WallTime {
        let millis = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        WallTime(self.0.saturating_add(millis))
    }
}

use std::time::Duration;

use tape_core::types::StorageUnits;

/// The spool manager's limits and waits
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpoolLimits {
    /// Most fetches running at once
    pub max_fetches: usize,
    /// Most bytes being fetched at once
    pub max_fetch_bytes: StorageUnits,
    /// Most of the running fetches that may be recoveries
    pub max_recoveries: usize,
    /// Most of our fetches running against one peer
    pub max_peer_fetches: usize,
    /// Most tracks one fetch asks for
    pub fetch_tracks: usize,
    /// Most bytes one fetch asks for
    pub fetch_bytes: StorageUnits,
    /// Time every fetch gets before it gives up
    pub fetch_timeout: Duration,
    /// Time a fetch gets on top for each MiB it asked for
    pub fetch_time_per_mib: Duration,
    /// Most missing tracks one turn looks at
    pub turn_tracks: usize,
    /// Tracks the scan adds each time the cursor goes round
    pub scan_page: usize,
    /// Most turns one wake takes
    pub turns_per_wake: usize,
    /// Longest the loop sleeps between wakes
    pub wake: Duration,
    /// A peer's rest after its first failure in a row, doubled per failure
    pub peer_rest: Duration,
    /// Longest rest a peer gets for failing
    pub peer_rest_max: Duration,
    /// A peer's rest after it sent wrong data
    pub wrong_data_rest: Duration,
    /// Failing this long in a row makes a peer gone
    pub peer_gone: Duration,
    /// Repairs failing this long may hand a track to recovery
    pub repair_failing: Duration,
    /// A track's wait after its first failure of its own, doubled per failure
    pub track_wait: Duration,
    /// Longest wait a track gets for failing on its own
    pub track_wait_max: Duration,
    /// Epochs a node keeps a spool's slices after it loses the spool
    pub keep_epochs: u64,
}

impl SpoolLimits {
    /// A peer's rest after `failures` failures in a row
    pub fn get_peer_rest(&self, failures: u32) -> Duration {
        get_doubled(self.peer_rest, failures, self.peer_rest_max)
    }

    /// A track's wait after `failures` failures of its own
    pub fn get_track_wait(&self, failures: u16) -> Duration {
        let failures = u32::from(failures);
        get_doubled(self.track_wait, failures, self.track_wait_max)
    }
}

impl Default for SpoolLimits {
    /// The limits every node starts from
    fn default() -> Self {
        Self {
            max_fetches: 32,
            max_fetch_bytes: StorageUnits::mb(128),
            max_recoveries: 4,
            max_peer_fetches: 2,
            fetch_tracks: 256,
            fetch_bytes: StorageUnits::mb(8),
            fetch_timeout: Duration::from_secs(10),
            fetch_time_per_mib: Duration::from_secs(8),
            turn_tracks: 1_024,
            scan_page: 2_048,
            turns_per_wake: 16,
            wake: Duration::from_secs(1),
            peer_rest: Duration::from_secs(2),
            peer_rest_max: Duration::from_secs(60),
            wrong_data_rest: Duration::from_secs(300),
            peer_gone: Duration::from_secs(120),
            repair_failing: Duration::from_secs(300),
            track_wait: Duration::from_secs(1),
            track_wait_max: Duration::from_secs(60),
            keep_epochs: 4,
        }
    }
}

/// `base` doubled for each count past the first, up to `max`
fn get_doubled(base: Duration, count: u32, max: Duration) -> Duration {
    let doublings = count.saturating_sub(1).min(u32::BITS - 1);
    base.saturating_mul(1 << doublings).min(max)
}

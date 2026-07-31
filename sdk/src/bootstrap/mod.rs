//! Persistent bootstrap hints and peer reputation.
//!
//! The cache stores predictions and reputation, never chain data. Every run
//! still fetches topology fresh. This only records which addresses are worth
//! asking for and which peers are worth talking to, so a stale file can cost a
//! wasted round trip but can never serve stale topology.

mod reputation;
mod store;

pub use reputation::{HEDGE_DELAY, Reputation, counts_against_peer, now_secs};
pub use store::{BYPASS_ENV, BootstrapStore, StoreError};

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_crypto::hash::Hash;

/// Identifies the chain a cache belongs to.
///
/// A cache written against another program predicts addresses that do not
/// exist, and the same program key can be deployed to two clusters, so the
/// genesis hash is checked as well. Either half changing drops the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NetworkKey {
    pub program_id: Address,
    pub genesis: Hash,
}

/// What the next run should guess so it can derive account addresses before it
/// has read the system row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Prediction {
    pub epoch: EpochNumber,
    pub total_groups: u64,
    /// Unix seconds at which the recorded epoch began.
    pub epoch_start: u64,
    /// Seconds per epoch, used to advance the guess across a boundary.
    pub epoch_duration: u64,
}

impl Prediction {
    /// Guess the epoch now, advancing arithmetically for elapsed time.
    ///
    /// Epoch advance is driven by a crank that can lag, so this may overshoot.
    /// A wrong guess costs one extra round and never costs correctness, so
    /// guessing forward is worth more than guessing safe.
    pub fn epoch_at(&self, now: u64) -> EpochNumber {
        if self.epoch_duration == 0 {
            return self.epoch;
        }
        let elapsed = now.saturating_sub(self.epoch_start);
        EpochNumber(self.epoch.0.saturating_add(elapsed / self.epoch_duration))
    }
}

/// How a peer has been behaving across runs.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum PeerHealth {
    Healthy,
    /// Allowed exactly one in-flight probe. A node that is still dead should
    /// cost one request, not a full share of the batch.
    Suspect,
    /// Skipped until the given unix second, then demoted to a probe.
    Quarantined { until: u64 },
}

/// Consecutive failures before a peer is quarantined.
pub const QUARANTINE_THRESHOLD: u32 = 3;

/// First quarantine window, doubling per failure past the threshold.
pub const QUARANTINE_BASE_SECS: u64 = 30;

/// Longest a peer is held out before it is probed again.
pub const QUARANTINE_MAX_SECS: u64 = 15 * 60;

/// Weight given to the newest latency sample.
const EWMA_ALPHA: f64 = 0.2;

/// One peer's observed behaviour.
///
/// Latency is held in microseconds because the cache format carries integers
/// only, and microseconds keep sub-millisecond resolution without a float.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct PeerRecord {
    pub node: Address,
    pub health: PeerHealth,
    pub consecutive_failures: u32,
    /// Smoothed round-trip time in microseconds, zero until first measured.
    pub ewma_micros: u32,
    pub last_ok: u64,
    pub last_fail: u64,
}

impl PeerRecord {
    pub fn new(node: Address) -> Self {
        Self {
            node,
            health: PeerHealth::Healthy,
            consecutive_failures: 0,
            ewma_micros: 0,
            last_ok: 0,
            last_fail: 0,
        }
    }

    /// Smoothed round-trip time in milliseconds, for ordering and display.
    pub fn ewma_ms(&self) -> f64 {
        f64::from(self.ewma_micros) / 1_000.0
    }

    /// Record a successful exchange and its latency.
    pub fn record_success(&mut self, now: u64, observed_ms: f64) {
        self.consecutive_failures = 0;
        self.health = PeerHealth::Healthy;
        self.last_ok = now;

        let observed = to_micros(observed_ms);
        self.ewma_micros = match self.ewma_micros {
            0 => observed,
            current => {
                let blended =
                    f64::from(current) * (1.0 - EWMA_ALPHA) + f64::from(observed) * EWMA_ALPHA;
                clamp_micros(blended)
            }
        };
    }

    /// Record a transport failure or timeout.
    ///
    /// Backpressure is not failure: a rate-limit reply carries its own retry
    /// window and must not count against the peer.
    pub fn record_failure(&mut self, now: u64) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_fail = now;
        if self.consecutive_failures >= QUARANTINE_THRESHOLD {
            self.health = PeerHealth::Quarantined {
                until: now.saturating_add(quarantine_window(self.consecutive_failures)),
            };
        }
    }

    /// Move an expired quarantine to a single-probe state.
    pub fn expire_quarantine(&mut self, now: u64) {
        if let PeerHealth::Quarantined { until } = self.health {
            if now >= until {
                self.health = PeerHealth::Suspect;
            }
        }
    }

    /// Whether this peer should be given a request right now.
    pub fn is_available(&self, now: u64) -> bool {
        match self.health {
            PeerHealth::Healthy | PeerHealth::Suspect => true,
            PeerHealth::Quarantined { until } => now >= until,
        }
    }

    /// Repair an entry whose timestamps cannot be true.
    ///
    /// A backwards clock or a restored home directory can leave a quarantine
    /// parked far in the future. Rather than trust it, fall back to a probe.
    pub fn reconcile_clock(&mut self, now: u64) {
        let horizon = now.saturating_add(QUARANTINE_MAX_SECS);
        let future_quarantine = matches!(self.health, PeerHealth::Quarantined { until } if until > horizon);
        if future_quarantine || self.last_ok > horizon || self.last_fail > horizon {
            self.health = PeerHealth::Suspect;
            self.last_ok = self.last_ok.min(now);
            self.last_fail = self.last_fail.min(now);
        }
    }
}

/// Milliseconds to stored microseconds, saturating rather than wrapping.
fn to_micros(observed_ms: f64) -> u32 {
    clamp_micros(observed_ms * 1_000.0)
}

/// A measurement can arrive absurd, from a suspended process or a bad clock.
/// Clamping keeps one bad sample from poisoning the ordering.
fn clamp_micros(micros: f64) -> u32 {
    if !micros.is_finite() || micros <= 0.0 {
        return 0;
    }
    micros.min(f64::from(u32::MAX)) as u32
}

/// How long to hold a peer out after a given number of failures.
pub fn quarantine_window(failures: u32) -> u64 {
    let steps = failures.saturating_sub(QUARANTINE_THRESHOLD);
    QUARANTINE_BASE_SECS
        .checked_shl(steps)
        .unwrap_or(QUARANTINE_MAX_SECS)
        .min(QUARANTINE_MAX_SECS)
}

/// Everything the cache file holds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct BootstrapState {
    pub network: NetworkKey,
    pub fetched_at: u64,
    pub prediction: Option<Prediction>,
    pub peers: Vec<PeerRecord>,
}

impl BootstrapState {
    pub fn new(network: NetworkKey) -> Self {
        Self {
            network,
            fetched_at: 0,
            prediction: None,
            peers: Vec::new(),
        }
    }

    pub fn peer(&self, node: &Address) -> Option<&PeerRecord> {
        self.peers.iter().find(|record| &record.node == node)
    }

    /// Get a peer's record, creating a healthy one if this is the first sight
    /// of it.
    pub fn peer_mut(&mut self, node: Address) -> &mut PeerRecord {
        match self.peers.iter().position(|record| record.node == node) {
            Some(index) => &mut self.peers[index],
            None => {
                self.peers.push(PeerRecord::new(node));
                self.peers.last_mut().expect("just pushed")
            }
        }
    }

    /// Drop records for nodes that are no longer in the network, so the file
    /// does not grow without bound as the fleet churns.
    pub fn retain_known(&mut self, known: &[Address]) {
        let known: std::collections::HashSet<&Address> = known.iter().collect();
        self.peers.retain(|record| known.contains(&record.node));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> NetworkKey {
        NetworkKey {
            program_id: Address::new_unique(),
            genesis: Hash([7u8; 32]),
        }
    }

    // a fresh peer takes the first sample outright, then smooths later ones
    #[test]
    fn ewma_seeds_then_smooths() {
        let mut record = PeerRecord::new(Address::new_unique());
        record.record_success(100, 40.0);
        assert_eq!(record.ewma_micros, 40_000);
        assert_eq!(record.ewma_ms(), 40.0);

        record.record_success(101, 140.0);
        assert_eq!(record.ewma_micros, 60_000);
        assert_eq!(record.ewma_ms(), 60.0);
    }

    // a nonsense sample must not poison the ordering
    #[test]
    fn absurd_latency_is_clamped() {
        let mut record = PeerRecord::new(Address::new_unique());
        record.record_success(1, f64::INFINITY);
        assert_eq!(record.ewma_micros, 0);

        record.record_success(2, -5.0);
        assert_eq!(record.ewma_micros, 0);

        record.record_success(3, 1e12);
        assert_eq!(record.ewma_micros, u32::MAX);
    }

    // failures below the threshold leave the peer usable
    #[test]
    fn quarantine_only_past_the_threshold() {
        let mut record = PeerRecord::new(Address::new_unique());
        record.record_failure(10);
        record.record_failure(11);
        assert_eq!(record.health, PeerHealth::Healthy);
        assert!(record.is_available(11));

        record.record_failure(12);
        assert_eq!(
            record.health,
            PeerHealth::Quarantined {
                until: 12 + QUARANTINE_BASE_SECS
            }
        );
        assert!(!record.is_available(12));
    }

    // the window doubles per extra failure and stops at the cap
    #[test]
    fn quarantine_window_doubles_then_caps() {
        assert_eq!(quarantine_window(3), QUARANTINE_BASE_SECS);
        assert_eq!(quarantine_window(4), QUARANTINE_BASE_SECS * 2);
        assert_eq!(quarantine_window(5), QUARANTINE_BASE_SECS * 4);
        assert_eq!(quarantine_window(64), QUARANTINE_MAX_SECS);
        assert_eq!(quarantine_window(u32::MAX), QUARANTINE_MAX_SECS);
    }

    // an expired quarantine becomes a probe, and one success clears it
    #[test]
    fn expired_quarantine_probes_then_recovers() {
        let mut record = PeerRecord::new(Address::new_unique());
        for tick in 0..3 {
            record.record_failure(tick);
        }
        record.expire_quarantine(1);
        assert!(matches!(record.health, PeerHealth::Quarantined { .. }));

        record.expire_quarantine(2 + QUARANTINE_BASE_SECS);
        assert_eq!(record.health, PeerHealth::Suspect);

        record.record_success(200, 12.0);
        assert_eq!(record.health, PeerHealth::Healthy);
        assert_eq!(record.consecutive_failures, 0);
    }

    // a quarantine parked beyond any real window is not trusted
    #[test]
    fn clock_jump_resets_to_probe() {
        let mut record = PeerRecord::new(Address::new_unique());
        record.health = PeerHealth::Quarantined {
            until: 1_000_000_000,
        };
        record.reconcile_clock(1_000);
        assert_eq!(record.health, PeerHealth::Suspect);
    }

    // the guess walks forward one epoch per elapsed duration
    #[test]
    fn prediction_advances_with_time() {
        let prediction = Prediction {
            epoch: EpochNumber(4),
            total_groups: 5,
            epoch_start: 1_000,
            epoch_duration: 100,
        };
        assert_eq!(prediction.epoch_at(1_000), EpochNumber(4));
        assert_eq!(prediction.epoch_at(1_099), EpochNumber(4));
        assert_eq!(prediction.epoch_at(1_100), EpochNumber(5));
        assert_eq!(prediction.epoch_at(1_350), EpochNumber(7));
    }

    // a zero duration would divide by zero, so the guess simply stands still
    #[test]
    fn prediction_survives_zero_duration() {
        let prediction = Prediction {
            epoch: EpochNumber(4),
            total_groups: 5,
            epoch_start: 1_000,
            epoch_duration: 0,
        };
        assert_eq!(prediction.epoch_at(9_999), EpochNumber(4));
    }

    // peers are created on demand and pruned against the live set
    #[test]
    fn peers_are_created_and_pruned() {
        let mut state = BootstrapState::new(key());
        let kept = Address::new_unique();
        let dropped = Address::new_unique();

        state.peer_mut(kept).record_success(1, 5.0);
        state.peer_mut(dropped).record_failure(1);
        assert_eq!(state.peers.len(), 2);

        state.retain_known(&[kept]);
        assert_eq!(state.peers.len(), 1);
        assert!(state.peer(&kept).is_some());
        assert!(state.peer(&dropped).is_none());
    }
}

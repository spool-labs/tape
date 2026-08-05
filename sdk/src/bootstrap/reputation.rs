//! Live peer reputation, backed by the bootstrap cache.
//!
//! Everything here is short and synchronous on purpose. The lock is never held
//! across an await, so a slow peer cannot block the bookkeeping for the others.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tape_api::program::tapedrive;
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::ApiError;
use tracing::debug;

use super::{BootstrapState, BootstrapStore, NetworkKey, PeerHealth, PeerRecord, Prediction};

/// Wait before widening a query, long enough that a healthy peer usually
/// answers first and short enough that a dead one does not set the pace.
pub const HEDGE_DELAY: Duration = Duration::from_millis(150);

/// Stands in for the genesis hash until the first bootstrap confirms it.
const UNCONFIRMED_GENESIS: Hash = Hash([0u8; 32]);

/// FNV-1a starting value.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a multiplier.
const FNV_PRIME: u64 = 0x100_0000_01b3;

/// Whether an error says anything about the peer's health.
///
/// Only a transport problem counts. A peer that answers "not found", "not
/// responsible" or "blacklisted" is working correctly and simply does not have
/// what was asked for, and a rate limit is backpressure with its own retry
/// window. Counting any of those would quarantine a healthy committee on the
/// first miss, which is the failure mode this whole mechanism exists to avoid.
pub fn counts_against_peer(error: &ApiError) -> bool {
    match error {
        ApiError::NotFound
        | ApiError::NotResponsible
        | ApiError::BlacklistedObject
        | ApiError::NotInCommittee
        | ApiError::RateLimited { .. } => false,
        ApiError::NodeUnresolved(_)
        | ApiError::ConnectionFailed(_)
        | ApiError::Timeout
        | ApiError::Serialization(_)
        | ApiError::ServerError { .. }
        | ApiError::StaleTrackProof
        | ApiError::Other(_) => true,
    }
}

/// Unix seconds now, or zero if the clock predates the epoch.
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Where a peer sits in the preference order. Lower is tried sooner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Rank {
    /// Known good, with a measurement to sort by.
    Measured,
    /// Known good but never timed. Worth a turn, just not ahead of proven ones.
    Unmeasured,
    /// One probe only.
    Suspect,
    /// Last resort. Still ordered, never dropped, because k slices must remain
    /// reachable even when most owners look bad.
    Quarantined,
}

/// Where one peer sits, given its record and the current time.
fn rank_of(record: Option<&PeerRecord>, now: u64) -> (Rank, u32) {
    match record {
        None => (Rank::Unmeasured, 0),
        Some(record) => match record.health {
            PeerHealth::Quarantined { until } if now < until => (Rank::Quarantined, 0),
            PeerHealth::Quarantined { .. } | PeerHealth::Suspect => (Rank::Suspect, 0),
            PeerHealth::Healthy if record.ewma_micros == 0 => (Rank::Unmeasured, 0),
            PeerHealth::Healthy => (Rank::Measured, record.ewma_micros),
        },
    }
}

/// Peer reputation shared by every request path on one client.
pub struct Reputation {
    /// Peers, prediction and network key for this client.
    state: Mutex<BootstrapState>,
    /// Cache file behind that state, disabled when there is nowhere to write.
    store: BootstrapStore,
    /// Set on any change, cleared when the state is written back.
    dirty: AtomicBool,
    /// Breaks ties differently per process so many clients do not all march
    /// through an unmeasured peer list in the same order.
    salt: u64,
}

impl Reputation {
    /// Reputation that is tracked in memory but never persisted.
    pub fn ephemeral(network: NetworkKey) -> Self {
        Self::from_state(BootstrapStore::disabled(), BootstrapState::new(network))
    }

    /// In-memory reputation for the configured program, with no cache file.
    pub fn detached() -> Self {
        Self::attach(BootstrapStore::disabled(), tapedrive::id().into())
    }

    /// Attach a store before the chain is known.
    ///
    /// A client is built before any RPC call, so the genesis hash that
    /// completes the cache key is not available yet. The file is adopted on the
    /// program id alone and the genesis is confirmed on the first bootstrap.
    ///
    /// Speculating on a prediction that turns out to belong to another cluster
    /// is harmless: account addresses are program derived, so the guess is
    /// simply wrong and the epoch check rejects it. Reputation is a different
    /// matter and is discarded the moment the genesis does not match.
    pub fn attach(store: BootstrapStore, program_id: Address) -> Self {
        let placeholder = NetworkKey {
            program_id,
            genesis: UNCONFIRMED_GENESIS,
        };
        let state = store
            .load_any()
            .filter(|state| state.network.program_id == program_id)
            .unwrap_or_else(|| BootstrapState::new(placeholder));
        Self::from_state(store, state)
    }

    /// Wrap loaded state, repairing anything the clock cannot justify.
    fn from_state(store: BootstrapStore, mut state: BootstrapState) -> Self {
        let now = now_secs();
        for record in &mut state.peers {
            record.reconcile_clock(now);
            record.expire_quarantine(now);
        }

        Self {
            state: Mutex::new(state),
            store,
            dirty: AtomicBool::new(false),
            salt: u64::from(std::process::id()),
        }
    }

    /// Confirm which chain we turned out to be talking to.
    ///
    /// Anything loaded on the program id alone is only trustworthy once the
    /// genesis agrees. When it does not, the peers and the prediction came from
    /// a different cluster and are dropped: a node address means nothing across
    /// chains.
    pub fn rekey(&self, network: NetworkKey) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.network == network {
            return;
        }

        let is_unconfirmed = state.network.genesis == UNCONFIRMED_GENESIS;
        let is_same_chain = is_unconfirmed || state.network.genesis == network.genesis;
        match is_same_chain {
            true => state.network = network,
            false => *state = BootstrapState::new(network),
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn network(&self) -> Option<NetworkKey> {
        self.state.lock().ok().map(|state| state.network)
    }

    /// A successful exchange, with how long it took.
    pub fn record_success(&self, node: Address, latency: Duration) {
        let now = now_secs();
        let millis = latency.as_secs_f64() * 1_000.0;
        if let Ok(mut state) = self.state.lock() {
            state.peer_mut(node).record_success(now, millis);
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// A transport failure or timeout. Backpressure must not come through here.
    pub fn record_failure(&self, node: Address) {
        let now = now_secs();
        let is_quarantined = match self.state.lock() {
            Ok(mut state) => {
                let record = state.peer_mut(node);
                record.record_failure(now);
                matches!(record.health, PeerHealth::Quarantined { .. })
            }
            Err(_) => false,
        };
        if is_quarantined {
            debug!(%node, "peer quarantined after repeated failures");
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Whether this peer is currently held out.
    pub fn is_quarantined(&self, node: &Address) -> bool {
        let now = now_secs();
        self.state
            .lock()
            .ok()
            .and_then(|state| state.peer(node).map(|record| !record.is_available(now)))
            .unwrap_or(false)
    }

    /// Order peers best first. Nothing is ever removed, only demoted.
    ///
    /// The lock is taken once for the whole list. Ranking each peer separately
    /// would lock and rescan per element, which on a full committee is hundreds
    /// of acquisitions for one query.
    pub fn order(&self, peers: &[Address]) -> Vec<Address> {
        let now = now_secs();
        let Ok(state) = self.state.lock() else {
            return peers.to_vec();
        };

        let mut ordered = peers.to_vec();
        ordered.sort_by_cached_key(|node| {
            let (rank, ewma) = rank_of(state.peer(node), now);
            (rank, ewma, self.tiebreak(node))
        });
        ordered
    }

    /// Cheap stable hash so equal-ranked peers do not share one global order.
    fn tiebreak(&self, node: &Address) -> u64 {
        let mut hash = FNV_OFFSET_BASIS ^ self.salt;
        for byte in node.as_ref() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// Record what the next run should guess.
    pub fn set_prediction(&self, prediction: Prediction) {
        if let Ok(mut state) = self.state.lock() {
            state.prediction = Some(prediction);
            state.fetched_at = now_secs();
        }
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn prediction(&self) -> Option<Prediction> {
        self.state.lock().ok().and_then(|state| state.prediction)
    }

    /// Forget peers the network no longer knows about.
    pub fn prune(&self, known: &[Address]) {
        if let Ok(mut state) = self.state.lock() {
            let before = state.peers.len();
            state.retain_known(known);
            if state.peers.len() != before {
                self.dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> Option<BootstrapState> {
        self.state.lock().ok().map(|state| state.clone())
    }

    /// Persist if anything changed. Failure is logged, never propagated: a
    /// cache that cannot be written must not fail the command that filled it.
    pub fn flush(&self) {
        if !self.dirty.swap(false, Ordering::Relaxed) || !self.store.is_enabled() {
            return;
        }
        let Some(state) = self.snapshot() else {
            return;
        };
        if let Err(error) = self.store.save(&state) {
            debug!(%error, "bootstrap cache not written");
        }
    }
}

/// Persist on teardown.
///
/// Bootstrap writes the prediction before a single peer has been contacted, so
/// everything reputation learns happens after that point. Without this the file
/// would only ever hold hints and the peer table would start empty every run.
impl Drop for Reputation {
    fn drop(&mut self) {
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::hash::Hash;

    fn network() -> NetworkKey {
        NetworkKey {
            program_id: Address::new_unique(),
            genesis: Hash([1u8; 32]),
        }
    }

    // proven fast peers lead, unknown ones follow, bad ones trail
    #[test]
    fn preference_order() {
        let reputation = Reputation::ephemeral(network());
        let fast = Address::new_unique();
        let slow = Address::new_unique();
        let unknown = Address::new_unique();
        let dead = Address::new_unique();

        reputation.record_success(fast, Duration::from_millis(10));
        reputation.record_success(slow, Duration::from_millis(200));
        for _ in 0..3 {
            reputation.record_failure(dead);
        }

        let ordered = reputation.order(&[dead, unknown, slow, fast]);
        assert_eq!(ordered[0], fast);
        assert_eq!(ordered[1], slow);
        assert_eq!(ordered[2], unknown);
        assert_eq!(ordered[3], dead, "quarantined peers trail, they are not dropped");
    }

    // a quarantined owner is still reachable, because k slices may need it
    #[test]
    fn quarantined_demoted() {
        let reputation = Reputation::ephemeral(network());
        let dead = Address::new_unique();
        for _ in 0..3 {
            reputation.record_failure(dead);
        }

        assert!(reputation.is_quarantined(&dead));

        // A read needs k of the group, so an owner that looks bad still has to
        // be reachable. Ordering may demote it, never drop it.
        let healthy = Address::new_unique();
        reputation.record_success(healthy, Duration::from_millis(5));
        let ordered = reputation.order(&[dead, healthy]);
        assert_eq!(ordered, vec![healthy, dead]);
    }

    // one success clears the failure streak
    #[test]
    fn success_rehabilitates() {
        let reputation = Reputation::ephemeral(network());
        let flaky = Address::new_unique();
        for _ in 0..3 {
            reputation.record_failure(flaky);
        }
        assert!(reputation.is_quarantined(&flaky));

        reputation.record_success(flaky, Duration::from_millis(5));
        assert!(!reputation.is_quarantined(&flaky));
    }

    // ordering must not depend on the order peers were handed in
    #[test]
    fn stable_order() {
        let reputation = Reputation::ephemeral(network());
        let a = Address::new_unique();
        let b = Address::new_unique();
        let c = Address::new_unique();

        let first = reputation.order(&[a, b, c]);
        let second = reputation.order(&[c, b, a]);
        assert_eq!(first, second);
    }
}

//! Live peer reputation, backed by the bootstrap cache.
//!
//! Everything here is short and synchronous on purpose. The lock is never held
//! across an await, so a slow peer cannot block the bookkeeping for the others.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tape_crypto::Address;
use tracing::debug;

use super::{BootstrapState, BootstrapStore, NetworkKey, PeerHealth, Prediction};

/// Wait before widening a query, long enough that a healthy peer usually
/// answers first and short enough that a dead one does not set the pace.
pub const HEDGE_DELAY: Duration = Duration::from_millis(150);

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

/// Peer reputation shared by every request path on one client.
pub struct Reputation {
    state: Mutex<BootstrapState>,
    store: BootstrapStore,
    dirty: AtomicBool,
    /// Breaks ties differently per process so many clients do not all march
    /// through an unmeasured peer list in the same order.
    salt: u64,
}

impl Reputation {
    /// Load reputation for a network, falling back to an empty set.
    pub fn load(store: BootstrapStore, network: NetworkKey) -> Self {
        let mut state = store
            .load(&network)
            .unwrap_or_else(|| BootstrapState::new(network));

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

    /// Reputation that is tracked in memory but never persisted.
    pub fn ephemeral(network: NetworkKey) -> Self {
        Self::load(BootstrapStore::disabled(), network)
    }

    /// Attach a store before the chain is known.
    ///
    /// A client is built before any RPC call, so the genesis hash that
    /// completes the cache key is not available yet. Start empty and adopt the
    /// file once the first bootstrap reports which chain this is.
    pub fn attach(store: BootstrapStore, program_id: Address) -> Self {
        Self::load(
            store,
            NetworkKey {
                program_id,
                genesis: tape_crypto::hash::Hash([0u8; 32]),
            },
        )
    }

    /// Point reputation at the chain we turned out to be talking to.
    ///
    /// Re-keying reloads from disk, so a client that started with a placeholder
    /// key picks up its real history. A cache belonging to another chain simply
    /// does not load, which is the intended outcome.
    pub fn rekey(&self, network: NetworkKey) {
        let already = self
            .state
            .lock()
            .ok()
            .map(|state| state.network == network)
            .unwrap_or(false);
        if already {
            return;
        }

        let mut loaded = self
            .store
            .load(&network)
            .unwrap_or_else(|| BootstrapState::new(network));
        let now = now_secs();
        for record in &mut loaded.peers {
            record.reconcile_clock(now);
            record.expire_quarantine(now);
        }

        if let Ok(mut state) = self.state.lock() {
            *state = loaded;
        }
        self.dirty.store(false, Ordering::Relaxed);
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
        let quarantined = match self.state.lock() {
            Ok(mut state) => {
                let record = state.peer_mut(node);
                record.record_failure(now);
                matches!(record.health, PeerHealth::Quarantined { .. })
            }
            Err(_) => false,
        };
        if quarantined {
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
    pub fn order(&self, peers: &[Address]) -> Vec<Address> {
        let now = now_secs();
        let mut ranked: Vec<(Rank, u32, u64, Address)> = peers
            .iter()
            .map(|node| {
                let (rank, ewma) = self.rank_of(node, now);
                (rank, ewma, self.tiebreak(node), *node)
            })
            .collect();

        ranked.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
        ranked.into_iter().map(|(_, _, _, node)| node).collect()
    }

    /// Split peers into those worth asking now and those held back.
    ///
    /// Used by paths that can afford to skip a peer entirely, unlike downloads
    /// where every owner may be needed.
    pub fn partition(&self, peers: &[Address]) -> (Vec<Address>, Vec<Address>) {
        let ordered = self.order(peers);
        ordered
            .into_iter()
            .partition(|node| !self.is_quarantined(node))
    }

    fn rank_of(&self, node: &Address, now: u64) -> (Rank, u32) {
        let Ok(state) = self.state.lock() else {
            return (Rank::Unmeasured, 0);
        };
        match state.peer(node) {
            None => (Rank::Unmeasured, 0),
            Some(record) => match record.health {
                PeerHealth::Quarantined { until } if now < until => (Rank::Quarantined, 0),
                PeerHealth::Quarantined { .. } | PeerHealth::Suspect => (Rank::Suspect, 0),
                PeerHealth::Healthy if record.ewma_micros == 0 => (Rank::Unmeasured, 0),
                PeerHealth::Healthy => (Rank::Measured, record.ewma_micros),
            },
        }
    }

    /// Cheap stable hash so equal-ranked peers do not share one global order.
    fn tiebreak(&self, node: &Address) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ self.salt;
        for byte in node.as_ref() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
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
    fn order_prefers_proven_then_unknown() {
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
    fn quarantined_peers_are_demoted_not_removed() {
        let reputation = Reputation::ephemeral(network());
        let dead = Address::new_unique();
        for _ in 0..3 {
            reputation.record_failure(dead);
        }

        assert!(reputation.is_quarantined(&dead));
        assert_eq!(reputation.order(&[dead]).len(), 1);

        let (available, held) = reputation.partition(&[dead]);
        assert!(available.is_empty());
        assert_eq!(held, vec![dead]);
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
    fn order_is_stable_regardless_of_input_order() {
        let reputation = Reputation::ephemeral(network());
        let a = Address::new_unique();
        let b = Address::new_unique();
        let c = Address::new_unique();

        let first = reputation.order(&[a, b, c]);
        let second = reputation.order(&[c, b, a]);
        assert_eq!(first, second);
    }
}

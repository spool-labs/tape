//! In-process registry of committee-peer liveness, filled by the background
//! aggregator and read when building the network view.
//!
//! The aggregator publishes one whole probe round at a time, so the map always
//! holds exactly the last round's committee and departed peers drop out.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use tape_crypto::Address;
use tape_observe_api::{LinkStatus, NodeStats, StatsSource};

/// A round older than this is treated as absent, so a stalled aggregator
/// degrades to "?" rather than showing stale liveness. Comfortably covers a few
/// missed ticks at the default 15s interval.
const STALE_AFTER: Duration = Duration::from_secs(60);

/// One peer's last probe result: reachability, where the stats came from, and the
/// stats themselves when it answered.
pub type Liveness = (LinkStatus, StatsSource, Option<NodeStats>);

struct Round {
    at: Instant,
    peers: HashMap<Address, Liveness>,
}

fn registry() -> &'static RwLock<Option<Round>> {
    static REGISTRY: OnceLock<RwLock<Option<Round>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(None))
}

/// Publish one probe round's results, replacing the previous round.
pub fn replace(peers: HashMap<Address, Liveness>) {
    if let Ok(mut reg) = registry().write() {
        *reg = Some(Round { at: Instant::now(), peers });
    }
}

/// Latest liveness for a node, or nothing when never probed or gone stale.
pub fn lookup(node: Address) -> Option<Liveness> {
    let reg = registry().read().ok()?;
    let round = reg.as_ref()?;
    if round.at.elapsed() > STALE_AFTER {
        return None;
    }
    round.peers.get(&node).cloned()
}

//! Lifetime counters for the challenge rounds this node has seen.
//!
//! Cheap atomics on the context rather than metrics, so the observe board can
//! serve them whether or not the metrics feature is compiled in.

use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct ChallengeCounters {
    /// Rounds this node opened and answered, one per spool it holds per round.
    pub opened: AtomicU64,
    /// Spool outcomes settled as certified.
    pub settled_certified: AtomicU64,
    /// Spool outcomes settled as local misses.
    pub settled_missed: AtomicU64,
    /// Incoming answers refused at the door.
    pub answers_refused: AtomicU64,
    /// Rounds whose entropy block never finalized, charged to nobody.
    pub voided: AtomicU64,
    /// Rounds dropped because their candidate block lost.
    pub discarded: AtomicU64,
    /// This node's own rounds that reached a quorum here.
    pub own_certified: AtomicU64,
    /// This node's own rounds that did not, which is the miss its group-mates
    /// each recorded against it.
    pub own_missed: AtomicU64,
}

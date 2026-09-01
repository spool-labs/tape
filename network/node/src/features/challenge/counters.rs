use std::sync::atomic::AtomicU64;

#[derive(Default)]
pub struct ChallengeCounters {
    pub opened: AtomicU64,
    pub settled_certified: AtomicU64,
    pub settled_missed: AtomicU64,
    pub answers_refused: AtomicU64,
    pub voided: AtomicU64,
    pub discarded: AtomicU64,
    pub own_certified: AtomicU64,
    pub own_missed: AtomicU64,
}

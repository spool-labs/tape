//! StateHandle — thread-safe handle for sharing ProtocolState.

use std::sync::Arc;

use arc_swap::ArcSwap;
use tape_core::system::EpochPhase;

use super::ProtocolState;

/// Thread-safe handle for sharing a `ProtocolState` across tasks.
///
/// Uses `ArcSwap` for lock-free reads with atomic updates.
#[derive(Clone)]
pub struct StateHandle {
    inner: Arc<ArcSwap<ProtocolState>>,
}

impl StateHandle {
    /// Create a new handle seeded with the given initial state.
    pub fn new(initial: ProtocolState) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Load the current state (lock-free).
    pub fn load(&self) -> arc_swap::Guard<Arc<ProtocolState>> {
        self.inner.load()
    }

    /// Replace the cached state atomically.
    pub fn store(&self, state: ProtocolState) {
        self.inner.store(Arc::new(state));
    }

    /// Update just the epoch phase (read-modify-write).
    pub fn update_phase(&self, phase: EpochPhase) {
        let current = self.inner.load();
        let mut updated = (**current).clone();
        updated.phase = phase;
        self.inner.store(Arc::new(updated));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::EpochNumber;

    fn empty_state() -> ProtocolState {
        ProtocolState::default()
    }

    #[test]
    fn store_load() {
        let handle = StateHandle::new(empty_state());
        assert_eq!(handle.load().epoch, EpochNumber(0));

        let mut s = empty_state();
        s.epoch = EpochNumber(5);
        handle.store(s);
        assert_eq!(handle.load().epoch, EpochNumber(5));
    }

    #[test]
    fn update_phase() {
        let handle = StateHandle::new(empty_state());
        assert_eq!(handle.load().phase, EpochPhase::Active);

        handle.update_phase(EpochPhase::Syncing);
        assert_eq!(handle.load().phase, EpochPhase::Syncing);
    }
}

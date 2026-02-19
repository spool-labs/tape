use tape_core::types::EpochNumber;

use crate::supervisor::TaskKey;

pub struct LifecycleEpochState {
    epoch: EpochNumber,
    sync_epoch: bool,
    advance_pool: bool,
    join_network: bool,
    advance_epoch: bool,
}

impl LifecycleEpochState {
    pub fn new(epoch: EpochNumber) -> Self {
        Self {
            epoch,
            sync_epoch: false,
            advance_pool: false,
            join_network: false,
            advance_epoch: false,
        }
    }

    pub fn reset(&mut self, epoch: EpochNumber) {
        self.epoch = epoch;
        self.sync_epoch = false;
        self.advance_pool = false;
        self.join_network = false;
        self.advance_epoch = false;
    }

    pub fn epoch(&self) -> EpochNumber {
        self.epoch
    }

    pub fn is_done(&self, key: &TaskKey) -> bool {
        match key {
            TaskKey::SyncEpoch => self.sync_epoch,
            TaskKey::AdvancePool => self.advance_pool,
            TaskKey::JoinNetwork => self.join_network,
            TaskKey::AdvanceEpoch => self.advance_epoch,
            _ => false,
        }
    }

    pub fn mark_done(&mut self, key: &TaskKey) {
        match key {
            TaskKey::SyncEpoch => self.sync_epoch = true,
            TaskKey::AdvancePool => self.advance_pool = true,
            TaskKey::JoinNetwork => self.join_network = true,
            TaskKey::AdvanceEpoch => self.advance_epoch = true,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_not_done() {
        let state = LifecycleEpochState::new(EpochNumber(1));
        assert!(!state.is_done(&TaskKey::SyncEpoch));
        assert!(!state.is_done(&TaskKey::AdvancePool));
        assert!(!state.is_done(&TaskKey::JoinNetwork));
        assert!(!state.is_done(&TaskKey::AdvanceEpoch));
    }

    #[test]
    fn mark_done() {
        let mut state = LifecycleEpochState::new(EpochNumber(1));
        state.mark_done(&TaskKey::SyncEpoch);
        assert!(state.is_done(&TaskKey::SyncEpoch));
        assert!(!state.is_done(&TaskKey::AdvancePool));
    }

    #[test]
    fn reset_clears() {
        let mut state = LifecycleEpochState::new(EpochNumber(1));
        state.mark_done(&TaskKey::SyncEpoch);
        state.mark_done(&TaskKey::AdvancePool);
        state.reset(EpochNumber(2));
        assert_eq!(state.epoch(), EpochNumber(2));
        assert!(!state.is_done(&TaskKey::SyncEpoch));
        assert!(!state.is_done(&TaskKey::AdvancePool));
    }

    #[test]
    fn non_lifecycle_key() {
        let state = LifecycleEpochState::new(EpochNumber(1));
        assert!(!state.is_done(&TaskKey::RefreshOnchainState));
    }
}

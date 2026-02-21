use tape_core::types::EpochNumber;

use crate::runtime::Task;

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

    pub fn is_done(&self, key: &Task) -> bool {
        match key {
            Task::SyncEpoch { .. } => self.sync_epoch,
            Task::AdvancePool { .. } => self.advance_pool,
            Task::JoinNetwork { .. } => self.join_network,
            Task::AdvanceEpoch { .. } => self.advance_epoch,
            _ => false,
        }
    }

    pub fn mark_done(&mut self, key: &Task) {
        match key {
            Task::SyncEpoch { .. } => self.sync_epoch = true,
            Task::AdvancePool { .. } => self.advance_pool = true,
            Task::JoinNetwork { .. } => self.join_network = true,
            Task::AdvanceEpoch { .. } => self.advance_epoch = true,
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
        assert!(!state.is_done(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::JoinNetwork { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvanceEpoch { epoch: EpochNumber(1) }));
    }

    #[test]
    fn mark_done() {
        let mut state = LifecycleEpochState::new(EpochNumber(1));
        state.mark_done(&Task::SyncEpoch { epoch: EpochNumber(1) });
        assert!(state.is_done(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(1) }));
    }

    #[test]
    fn reset_clears() {
        let mut state = LifecycleEpochState::new(EpochNumber(1));
        state.mark_done(&Task::SyncEpoch { epoch: EpochNumber(1) });
        state.mark_done(&Task::AdvancePool { epoch: EpochNumber(1) });
        state.reset(EpochNumber(2));
        assert_eq!(state.epoch(), EpochNumber(2));
        assert!(!state.is_done(&Task::SyncEpoch { epoch: EpochNumber(2) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(2) }));
    }

    #[test]
    fn non_lifecycle_key() {
        let state = LifecycleEpochState::new(EpochNumber(1));
        assert!(!state.is_done(&Task::RefreshOnchainState));
    }
}

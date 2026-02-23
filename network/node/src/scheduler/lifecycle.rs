use std::collections::HashSet;

use tape_core::types::EpochNumber;
use tape_core::system::EpochPhase;
use tape_store::types::{NodeInfo, NodeStatus};

use crate::core::committee::our_member_index;
use crate::Task;

pub struct LifecycleState {
    epoch: EpochNumber,
    sync_epoch: bool,
    advance_pool: bool,
    join_network: bool,
    advance_epoch: bool,
}

impl LifecycleState {
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

pub struct LifecyclePlanner {
    pub state: LifecycleState,
}

impl LifecyclePlanner {
    pub fn new() -> Self {
        Self {
            state: LifecycleState::new(EpochNumber(0)),
        }
    }

    pub fn state(&self) -> &LifecycleState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut LifecycleState {
        &mut self.state
    }

    /// Recompute the desired set for epoch-scoped lifecycle tasks based on the
    /// current chain phase. Also keeps local lifecycle epoch aligned to chain epoch.
    pub fn schedule(
        &mut self,
        chain_phase: Option<EpochPhase>,
        node_status: NodeStatus,
        epoch: EpochNumber,
        desired: &mut HashSet<Task>,
    ) {
        tracing::trace!(epoch = epoch.0, "executing lifecycle scheduling");
        if !matches!(node_status, NodeStatus::Active) {
            tracing::trace!(epoch = epoch.0, "schedule_lifecycle skipped for non-active node");
            return;
        }

        // Keep local lifecycle epoch (scheduler-owned) aligned to chain epoch,
        // even when epoch changes arrive via refresh/replay without EpochAdvanced state changes.
        if self.state.epoch() != epoch {
            self.state.reset(epoch);
        }
        desired.retain(|key| !matches!(key.scheduled_epoch(), Some(x) if x != epoch));
        tracing::trace!(
            epoch = epoch.0,
            chain_phase = ?chain_phase,
            in_standby_lifecycle_epoch = self.state.epoch().0,
            "schedule_lifecycle phase snapshot"
        );

        // Recompute lifecycle desired-set from phase each time to avoid stale keys.
        desired.remove(&Task::SyncEpoch { epoch });
        desired.remove(&Task::AdvancePool { epoch });
        desired.remove(&Task::JoinNetwork { epoch });

        let chain_phase_is_active = matches!(chain_phase, Some(EpochPhase::Active));
        match chain_phase {
            Some(EpochPhase::Syncing) | Some(EpochPhase::Unknown) | None => {
                if !self.state.is_done(&Task::SyncEpoch { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling SyncEpoch in lifecycle");
                    desired.insert(Task::SyncEpoch { epoch });
                } else {
                    tracing::trace!(epoch = epoch.0, "schedule_lifecycle: SyncEpoch already done for epoch");
                }
            }
            Some(EpochPhase::Settling) => {
                if !self.state.is_done(&Task::AdvancePool { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling AdvancePool in lifecycle");
                    desired.insert(Task::AdvancePool { epoch });
                }
                if !self.state.is_done(&Task::JoinNetwork { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling JoinNetwork in lifecycle");
                    desired.insert(Task::JoinNetwork { epoch });
                } else {
                    tracing::trace!(epoch = epoch.0, "schedule_lifecycle: JoinNetwork already done for epoch");
                }
            }
            Some(EpochPhase::Active) => {
                tracing::trace!(epoch = epoch.0, "schedule_lifecycle: chain phase active, waiting for Refresh/AdvanceEpoch loop");
            }
        }
        if chain_phase_is_active && !self.state.is_done(&Task::AdvanceEpoch { epoch }) {
            tracing::trace!(epoch = epoch.0, "scheduling AdvanceEpoch in lifecycle");
            desired.insert(Task::AdvanceEpoch { epoch });
        } else if self.state.is_done(&Task::AdvanceEpoch { epoch }) {
            tracing::trace!(
                epoch = epoch.0,
                "schedule_lifecycle: AdvanceEpoch already done for epoch"
            );
        } else {
            tracing::trace!(
                epoch = epoch.0,
                "schedule_lifecycle: chain not active for AdvanceEpoch schedule"
            );
        }
    }

    /// Log this node's committee index for the epoch when available.
    pub fn log_member_index(
        committee: &[NodeInfo],
        keypair_pubkey: tape_crypto::Pubkey,
        epoch: EpochNumber,
    ) {
        if committee.is_empty() {
            tracing::warn!(
                epoch = epoch.0,
                "cannot resolve committee when logging member index"
            );
            return;
        }

        match our_member_index(committee, keypair_pubkey) {
            Ok(member_index) => {
                tracing::info!(
                    epoch = epoch.0,
                    member_index,
                    committee_size = committee.len(),
                    "node member index for epoch"
                );
            }
            Err(error) => {
                tracing::warn!(
                    epoch = epoch.0,
                    error = %error,
                    "node not found in committee for epoch"
                );
            }
        }
    }

    /// Called on the timer tick. Schedules AdvanceEpoch if the node is Active
    /// and the chain is in the Active phase.
    pub fn periodic(
        &self,
        node_status: NodeStatus,
        epoch: EpochNumber,
        chain_phase: Option<EpochPhase>,
        desired: &mut HashSet<Task>,
    ) {
        let lifecycle_done = self.state.is_done(&Task::AdvanceEpoch { epoch });
        tracing::trace!(
            epoch = epoch.0,
            node_status = ?node_status,
            lifecycle_done,
            "periodic lifecycle scheduling check"
        );

        if !matches!(node_status, NodeStatus::Active) {
            tracing::trace!(
                epoch = epoch.0,
                node_status = ?node_status,
                "periodic lifecycle scheduling skipped: node not active"
            );
            return;
        }

        if lifecycle_done {
            tracing::trace!(
                epoch = epoch.0,
                "periodic lifecycle scheduling skipped: AdvanceEpoch already done for epoch"
            );
            return;
        }

        if !matches!(chain_phase, Some(EpochPhase::Active)) {
            tracing::trace!(
                epoch = epoch.0,
                chain_phase = ?chain_phase,
                "periodic lifecycle scheduling skipped: chain phase not active"
            );
            return;
        }

        tracing::trace!(epoch = epoch.0, "periodic scheduler adding AdvanceEpoch task");
        desired.insert(Task::AdvanceEpoch { epoch });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_not_done() {
        let state = LifecycleState::new(EpochNumber(1));
        assert!(!state.is_done(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::JoinNetwork { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvanceEpoch { epoch: EpochNumber(1) }));
    }

    #[test]
    fn mark_done() {
        let mut state = LifecycleState::new(EpochNumber(1));
        state.mark_done(&Task::SyncEpoch { epoch: EpochNumber(1) });
        assert!(state.is_done(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(1) }));
    }

    #[test]
    fn reset_clears() {
        let mut state = LifecycleState::new(EpochNumber(1));
        state.mark_done(&Task::SyncEpoch { epoch: EpochNumber(1) });
        state.mark_done(&Task::AdvancePool { epoch: EpochNumber(1) });
        state.reset(EpochNumber(2));
        assert_eq!(state.epoch(), EpochNumber(2));
        assert!(!state.is_done(&Task::SyncEpoch { epoch: EpochNumber(2) }));
        assert!(!state.is_done(&Task::AdvancePool { epoch: EpochNumber(2) }));
    }

    #[test]
    fn non_lifecycle_key() {
        let state = LifecycleState::new(EpochNumber(1));
        assert!(!state.is_done(&Task::SpoolSync { spool: 1 }));
    }
}

use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_store::TapeStore;

use tape_core::types::EpochNumber;
use tape_store::ops::{CommitteeOps, MetaOps};
use tape_store::types::NodeStatus;

use crate::runtime::committee::our_member_index;
use crate::state::LifecycleEpochState;
use crate::runtime::Task;

pub struct LifecyclePlanner {
    pub state: LifecycleEpochState,
}

impl LifecyclePlanner {
    pub fn new() -> Self {
        Self {
            state: LifecycleEpochState::new(EpochNumber(0)),
        }
    }

    pub fn state(&self) -> &LifecycleEpochState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut LifecycleEpochState {
        &mut self.state
    }

    /// Recompute the desired set for epoch-scoped lifecycle tasks based on the
    /// current chain phase. Also keeps local lifecycle epoch aligned to chain epoch.
    pub fn schedule<S: Store>(
        &mut self,
        store: &TapeStore<S>,
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
        let chain_phase = store.get_chain_epoch_phase().ok().flatten();
        tracing::trace!(
            epoch = epoch.0,
            chain_phase = ?chain_phase,
            in_standby_lifecycle_epoch = self.state.epoch().0,
            "schedule_lifecycle phase snapshot"
        );

        /*
        PHASE1:DISABLED — phase-based lifecycle selection
        // Recompute lifecycle desired-set from phase each time to avoid stale keys.
        desired.remove(&Task::SyncEpoch { epoch });
        desired.remove(&Task::AdvancePool { epoch });
        desired.remove(&Task::JoinNetwork { epoch });

        let phase = store.get_chain_epoch_phase().ok().flatten();
        match phase {
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
        */

        // PHASE1: unconditionally schedule AdvanceEpoch in lifecycle
        if !self.state.is_done(&Task::AdvanceEpoch { epoch }) {
            tracing::trace!(epoch = epoch.0, "scheduling AdvanceEpoch in lifecycle (phase1)");
            desired.insert(Task::AdvanceEpoch { epoch });
        }
    }

    /// Log this node's committee index for the epoch when available.
    pub fn log_member_index<S: Store>(
        store: &TapeStore<S>,
        keypair_pubkey: Pubkey,
        epoch: EpochNumber,
    ) {
        let committee = match store.get_committee(epoch).ok().flatten() {
            Some(committee) => committee,
            None => {
                tracing::warn!(
                    epoch = epoch.0,
                    "cannot resolve committee when logging member index"
                );
                return;
            }
        };

        match our_member_index(&committee, keypair_pubkey) {
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

        /*
        PHASE1:DISABLED — chain phase gate for periodic tasks
        if !matches!(chain_phase, Some(EpochPhase::Active)) {
            tracing::trace!(
                epoch = epoch.0,
                chain_phase = ?chain_phase,
                "periodic lifecycle scheduling skipped: chain phase not active"
            );
            return;
        }
        */

        tracing::trace!(epoch = epoch.0, "periodic scheduler adding AdvanceEpoch task");
        desired.insert(Task::AdvanceEpoch { epoch });
    }
}

use std::collections::HashSet;

use tape_core::types::EpochNumber;
use tape_core::system::EpochPhase;
use tape_store::types::NodeStatus;

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

        // Always align epoch and prune stale tasks, regardless of status.
        if self.state.epoch() != epoch {
            self.state.reset(epoch);
        }
        desired.retain(|key| !matches!(key.scheduled_epoch(), Some(x) if x != epoch));

        // Recovery states: skip all lifecycle scheduling.
        if !matches!(node_status, NodeStatus::Active | NodeStatus::Standby) {
            tracing::trace!(epoch = epoch.0, "schedule_lifecycle skipped for recovery node");
            return;
        }

        tracing::trace!(
            epoch = epoch.0,
            chain_phase = ?chain_phase,
            ?node_status,
            "schedule_lifecycle phase snapshot"
        );

        let past_syncing = matches!(
            chain_phase,
            Some(EpochPhase::Settling) | Some(EpochPhase::Active)
        );

        // SyncEpoch: Active-only (Standby has no spools to sync).
        if matches!(node_status, NodeStatus::Active) {
            if past_syncing {
                desired.remove(&Task::SyncEpoch { epoch });
            } else if !self.state.is_done(&Task::SyncEpoch { epoch }) {
                desired.insert(Task::SyncEpoch { epoch });
            }
        }

        // AdvancePool: Active + Standby (needed for re-join).
        if past_syncing && !self.state.is_done(&Task::AdvancePool { epoch }) {
            desired.insert(Task::AdvancePool { epoch });
        }

        // JoinNetwork: Active + Standby, gated on AdvancePool done.
        if past_syncing
            && self.state.is_done(&Task::AdvancePool { epoch })
            && !self.state.is_done(&Task::JoinNetwork { epoch })
        {
            desired.insert(Task::JoinNetwork { epoch });
        }

        // AdvanceEpoch: Active-only (committee members trigger epoch advance).
        if matches!(node_status, NodeStatus::Active)
            && matches!(chain_phase, Some(EpochPhase::Active))
            && !self.state.is_done(&Task::AdvanceEpoch { epoch })
        {
            desired.insert(Task::AdvanceEpoch { epoch });
        }
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

    #[test]
    fn bootstrap_fast_path() {
        let epoch = EpochNumber(2);
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        // Syncing→Active (no Settling observed, e.g. empty committee_prev)
        planner.schedule(Some(EpochPhase::Syncing), NodeStatus::Active, epoch, &mut desired);
        assert!(desired.contains(&Task::SyncEpoch { epoch }));

        planner.state.mark_done(&Task::SyncEpoch { epoch });
        planner.schedule(Some(EpochPhase::Active), NodeStatus::Active, epoch, &mut desired);

        assert!(desired.contains(&Task::AdvancePool { epoch }));
        // JoinNetwork gated on AdvancePool being done
        assert!(!desired.contains(&Task::JoinNetwork { epoch }));
        assert!(desired.contains(&Task::AdvanceEpoch { epoch }));

        planner.state.mark_done(&Task::AdvancePool { epoch });
        planner.schedule(Some(EpochPhase::Active), NodeStatus::Active, epoch, &mut desired);
        assert!(desired.contains(&Task::JoinNetwork { epoch }));
    }

    #[test]
    fn sync_removed_past_syncing() {
        let epoch = EpochNumber(3);
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        planner.schedule(Some(EpochPhase::Syncing), NodeStatus::Active, epoch, &mut desired);
        assert!(desired.contains(&Task::SyncEpoch { epoch }));

        planner.schedule(Some(EpochPhase::Settling), NodeStatus::Active, epoch, &mut desired);
        assert!(!desired.contains(&Task::SyncEpoch { epoch }));
    }

    #[test]
    fn monotonic_no_cancel() {
        let epoch = EpochNumber(4);
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        planner.schedule(Some(EpochPhase::Settling), NodeStatus::Active, epoch, &mut desired);
        assert!(desired.contains(&Task::AdvancePool { epoch }));
        // JoinNetwork not yet scheduled (AdvancePool not done)
        assert!(!desired.contains(&Task::JoinNetwork { epoch }));

        planner.state.mark_done(&Task::AdvancePool { epoch });

        // Repeated calls during Active never drop these tasks
        for _ in 0..3 {
            planner.schedule(Some(EpochPhase::Active), NodeStatus::Active, epoch, &mut desired);
            assert!(desired.contains(&Task::JoinNetwork { epoch }));
        }
    }

    #[test]
    fn done_not_readded() {
        let epoch = EpochNumber(5);
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        planner.schedule(Some(EpochPhase::Settling), NodeStatus::Active, epoch, &mut desired);
        planner.state.mark_done(&Task::AdvancePool { epoch });
        planner.state.mark_done(&Task::JoinNetwork { epoch });
        desired.remove(&Task::AdvancePool { epoch });
        desired.remove(&Task::JoinNetwork { epoch });

        planner.schedule(Some(EpochPhase::Active), NodeStatus::Active, epoch, &mut desired);
        assert!(!desired.contains(&Task::AdvancePool { epoch }));
        assert!(!desired.contains(&Task::JoinNetwork { epoch }));
    }

    #[test]
    fn standby_rejoin() {
        let epoch = EpochNumber(3);
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        // Standby node in Settling phase gets AdvancePool but not SyncEpoch.
        planner.schedule(Some(EpochPhase::Settling), NodeStatus::Standby, epoch, &mut desired);
        assert!(!desired.contains(&Task::SyncEpoch { epoch }));
        assert!(desired.contains(&Task::AdvancePool { epoch }));
        assert!(!desired.contains(&Task::JoinNetwork { epoch }));

        // After AdvancePool completes, JoinNetwork is scheduled.
        planner.state.mark_done(&Task::AdvancePool { epoch });
        planner.schedule(Some(EpochPhase::Active), NodeStatus::Standby, epoch, &mut desired);
        assert!(desired.contains(&Task::JoinNetwork { epoch }));
        assert!(!desired.contains(&Task::AdvanceEpoch { epoch }));
    }

    #[test]
    fn standby_prunes_stale() {
        let mut planner = LifecyclePlanner::new();
        let mut desired = HashSet::new();

        // Simulate leftover tasks from a previous epoch.
        let old = EpochNumber(2);
        let new = EpochNumber(3);
        desired.insert(Task::SyncEpoch { epoch: old });
        desired.insert(Task::AdvancePool { epoch: old });
        planner.state.reset(old);

        // Standby node at new epoch prunes stale tasks.
        planner.schedule(Some(EpochPhase::Settling), NodeStatus::Standby, new, &mut desired);
        assert!(!desired.contains(&Task::SyncEpoch { epoch: old }));
        assert!(!desired.contains(&Task::AdvancePool { epoch: old }));
    }
}

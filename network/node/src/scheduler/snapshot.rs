use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;

use tape_core::types::EpochNumber;
use tape_store::ops::{CommitteeOps, MetaOps};
use tape_store::types::ChunkIndex;

use crate::core::NodeContext;
use crate::core::committee::our_snapshot_groups;
use crate::scheduler::lifecycle::LifecycleState;
use crate::snapshot::{
    derive_snapshot_local_epoch, is_snapshot_build_complete, is_snapshot_chunk_ready, GroupState,
    SnapshotProgress,
};
use crate::Task;

pub struct SnapshotPlanner {
    pub progress: SnapshotProgress,
}

#[allow(dead_code)]
impl SnapshotPlanner {
    pub fn new() -> Self {
        Self {
            progress: SnapshotProgress::new(EpochNumber(0)),
        }
    }

    pub fn progress_mut(&mut self) -> &mut SnapshotProgress {
        &mut self.progress
    }

    /// Drive the snapshot pipeline: Build -> Collect -> Register -> Submit.
    /// Reads per-group readiness from the store and advances tasks through the
    /// pipeline stages. Only schedules tasks for spool groups this node owns.
    pub fn schedule<S: Store, R: Rpc>(
        &mut self,
        context: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        desired: &mut HashSet<Task>,
        scheduled: &mut HashSet<Task>,
        lifecycle: &LifecycleState,
        chain_phase_is_active: bool,
    ) {
        tracing::trace!(epoch = epoch.0, "scheduling snapshot pipeline");
        let snapshot_build = Task::SnapshotBuild { epoch };
        let snapshot_collect = Task::SnapshotCollect { epoch };
        let register_snapshot = Task::RegisterSnapshot { epoch };
        let snapshot_submit = Task::SnapshotSubmit { epoch };

        let Some(local_epoch) = derive_snapshot_local_epoch(epoch) else {
            tracing::trace!(epoch = epoch.0, "snapshot scheduling skipped: no local epoch");
            desired.remove(&snapshot_build);
            desired.remove(&snapshot_collect);
            desired.remove(&register_snapshot);
            desired.remove(&snapshot_submit);
            return;
        };

        if self.progress.epoch() != epoch {
            self.progress.reset(epoch);
        }

        let all_built = match is_snapshot_build_complete(context, local_epoch) {
            Ok(built) => built,
            Err(e) => {
                tracing::warn!("snapshot pipeline: failed to read build state: {e}");
                false
            }
        };
        tracing::trace!(
            epoch = epoch.0,
            local_epoch = local_epoch.0,
            all_built,
            "snapshot build state checked"
        );

        if !all_built {
            tracing::trace!(epoch = epoch.0, "scheduling snapshot build");
            desired.insert(snapshot_build.clone());
        }

        let owned_groups: HashSet<u64> = match context.store.get_committee(epoch) {
            Ok(Some(committee)) => {
                match our_snapshot_groups(&committee, context.keypair.pubkey()) {
                    Ok(groups) => groups,
                    Err(e) => {
                        tracing::warn!("snapshot pipeline: {e}");
                        tracing::trace!(epoch = epoch.0, "no snapshot groups due to committee resolution error");
                        HashSet::new()
                    }
                }
            }
            Ok(None) => {
                tracing::warn!("snapshot pipeline: missing committee for epoch {}", epoch.0);
                tracing::trace!(epoch = epoch.0, "snapshot ownership unknown: missing committee");
                HashSet::new()
            }
            Err(e) => {
                tracing::warn!("snapshot pipeline: failed to read committee: {e}");
                tracing::trace!(epoch = epoch.0, "snapshot ownership unknown: committee read failed");
                HashSet::new()
            }
        };

        if owned_groups.is_empty() {
            desired.remove(&snapshot_collect);
            desired.remove(&register_snapshot);
            desired.remove(&snapshot_submit);
            tracing::trace!(epoch = epoch.0, owned_groups = 0, "snapshot collect/register/submit unschedulable");
            if !all_built {
                // Cannot yet determine owned groups; keep build running until committee is known.
            } else {
                desired.remove(&snapshot_build);
            }
            return;
        }

        let mut ready_groups: Vec<usize> = Vec::new();

        // Rebuild per-group progress from store state.
        for &group in &owned_groups {
            let ready = match is_snapshot_chunk_ready(context, local_epoch, group) {
                Ok(ready) => ready,
                Err(e) => {
                    tracing::warn!("snapshot pipeline: failed to read group readiness: {e}");
                    false
                }
            };
            tracing::trace!(
                epoch = epoch.0,
                group = group,
                ready,
                "snapshot group readiness"
            );
            if ready {
                ready_groups.push(group as usize);
            }
            if !ready {
                continue;
            }

            let ci = ChunkIndex(group);
            let has_cert = context
                .store
                .get_snapshot_cert(local_epoch, ci)
                .ok()
                .flatten()
                .is_some();
            if has_cert {
                self.progress
                    .advance(group as usize, GroupState::Certified);
            }
        }

        let owned_vec: Vec<usize> = owned_groups.iter().map(|&g| g as usize).collect();
        let all_certified = self.progress.all_local_cert(&owned_vec);
        let all_onchain = self.progress.all_done_onchain(&owned_vec);

        if all_onchain {
            desired.remove(&register_snapshot);
            desired.remove(&snapshot_submit);
        } else {
            if !ready_groups.is_empty() {
                desired.insert(register_snapshot);
            } else {
                desired.remove(&register_snapshot);
            }
            if !ready_groups.is_empty() && self.progress.any_local_cert(&owned_vec) {
                desired.insert(snapshot_submit);
            } else {
                desired.remove(&snapshot_submit);
            }
        }

        if all_certified {
            desired.remove(&snapshot_collect);
        } else if !ready_groups.is_empty() {
            desired.insert(snapshot_collect);
        } else {
            desired.remove(&snapshot_collect);
        }

        // Advance-wait gap fix: when all owned groups have completed on-chain,
        // force-reschedule AdvanceEpoch so we don't wait for the next tick.
        if all_onchain {
            tracing::trace!(epoch = epoch.0, "snapshot all groups onchain -> forcing advance epoch reschedule");
            scheduled.remove(&Task::AdvanceEpoch { epoch });
            if !lifecycle.is_done(&Task::AdvanceEpoch { epoch }) && chain_phase_is_active {
                desired.insert(Task::AdvanceEpoch { epoch });
            }
        }
    }

    /// Advance snapshot pipeline progress when a snapshot stage completes, then
    /// re-run `schedule` to unlock the next stage.
    pub fn on_success<S: Store, R: Rpc>(
        &mut self,
        context: &Arc<NodeContext<S, R>>,
        key: &Task,
        desired: &mut HashSet<Task>,
        scheduled: &mut HashSet<Task>,
        lifecycle: &LifecycleState,
        chain_phase_is_active: bool,
    ) {
        /*
        PHASE1:DISABLED — no snapshot scheduling
        if !matches!(
            key,
            Task::SnapshotCollect { .. }
                | Task::RegisterSnapshot { .. }
                | Task::SnapshotSubmit { .. }
        ) {
            return;
        }
        tracing::trace!(task = ?key, "scheduler handling snapshot stage success");
        let Some(epoch) = context.store.get_chain_epoch().ok().flatten() else {
            return;
        };
        if self.progress.epoch() == epoch {
            match key {
                Task::SnapshotCollect { .. } => {
                    self.mark_groups_store(context, epoch, GroupState::Certified);
                }
                Task::RegisterSnapshot { .. } => {
                    self.mark_owned_groups(context, epoch, GroupState::Registered);
                }
                Task::SnapshotSubmit { .. } => {
                    self.mark_owned_groups(context, epoch, GroupState::CertifiedOnchain);
                }
                _ => {}
            }
        }
        self.schedule(context, epoch, desired, scheduled, lifecycle, chain_phase_is_active);
        */
        let _ = (context, key, desired, scheduled, lifecycle, chain_phase_is_active);
    }

    /// Snapshot spool groups this node owns for the given epoch's committee.
    pub fn groups_for_epoch<S: Store, R: Rpc>(
        context: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
    ) -> HashSet<u64> {
        match context.store.get_committee(epoch) {
            Ok(Some(committee)) => {
                our_snapshot_groups(&committee, context.keypair.pubkey()).unwrap_or_default()
            }
            _ => HashSet::new(),
        }
    }

    /// Advance snapshot progress for all groups this node owns.
    pub fn mark_owned_groups<S: Store, R: Rpc>(
        &mut self,
        context: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        state: GroupState,
    ) {
        for group in Self::groups_for_epoch(context, epoch) {
            self.progress.advance(group as usize, state);
        }
    }

    /// Advance snapshot progress for owned groups that have a cert in the store.
    pub fn mark_groups_store<S: Store, R: Rpc>(
        &mut self,
        context: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        state: GroupState,
    ) {
        let Some(local_epoch) = derive_snapshot_local_epoch(epoch) else {
            return;
        };

        for group in Self::groups_for_epoch(context, epoch) {
            if context
                .store
                .get_snapshot_cert(local_epoch, ChunkIndex(group))
                .ok()
                .flatten()
                .is_some()
            {
                self.progress.advance(group as usize, state);
            }
        }
    }
}

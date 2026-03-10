use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_store::TapeStore;

use tape_core::erasure::{spool_in_group, SPOOL_COUNT};
use tape_core::spooler::{SpoolAssignment, SpoolIndex};
use tape_core::system::CommitteeMember;
use tape_core::types::{EpochNumber, NodeId};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{NodeStatus, Pubkey as StorePubkey, SpoolState, SpoolStatus};

use crate::Task;

pub struct SpoolPlanner;

impl SpoolPlanner {
    fn prev_owner_for(
        spool: SpoolIndex,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> Option<NodeId> {
        prev_spools
            .0
            .get(spool as usize)
            .and_then(|&member_idx| prev_committee.get(member_idx as usize))
            .map(|member| member.id)
    }

    /// Sync the desired set with current spool ownership. Removes tasks for
    /// spools we no longer own and adds SpoolSync/SpoolRecovery for new ones.
    pub fn reconcile<S: Store>(
        store: &TapeStore<S>,
        node_status: NodeStatus,
        desired: &mut HashSet<Task>,
    ) {
        if matches!(node_status, NodeStatus::Standby) {
            tracing::trace!("reconcile_spools skipped for standby node");
            return;
        }
        tracing::trace!("reconciling spools in active execution path");

        let owned_spools = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("failed to read spool status: {e}");
                return;
            }
        };
        tracing::trace!(owned_spools = owned_spools.len(), "reconciling spool tasks");

        let schedulable_spools = Self::schedulable_spools(&owned_spools);
        Self::prune_desired_spool_tasks(desired, &schedulable_spools);
        Self::add_spool_tasks(store, &owned_spools, desired);
    }

    /// Build the spool-id set that may have active spool tasks scheduled.
    /// `LockedToMove` spools are intentionally excluded.
    fn schedulable_spools(owned_spools: &[(u16, SpoolState)]) -> HashSet<u16> {
        owned_spools
            .iter()
            .filter_map(|(spool_id, state)| {
                if state.is_locked() {
                    None
                } else {
                    Some(*spool_id)
                }
            })
            .collect()
    }

    /// Remove spool-scoped tasks for spools that are no longer schedulable.
    fn prune_desired_spool_tasks(desired: &mut HashSet<Task>, schedulable_spools: &HashSet<u16>) {
        desired.retain(|task| match task.spool_id() {
            Some(spool_id) => schedulable_spools.contains(&spool_id),
            None => true,
        });
    }

    /// Add spool tasks based on each spool's current status.
    fn add_spool_tasks<S: Store>(
        store: &TapeStore<S>,
        owned_spools: &[(u16, SpoolState)],
        desired: &mut HashSet<Task>,
    ) {
        for (spool_id, state) in owned_spools {
            if state.is_syncing() {
                tracing::trace!(spool_id, status = ?state.status, "scheduling spool sync");
                desired.insert(Task::SpoolSync { spool: *spool_id });
            }
            if state.is_recovering() {
                tracing::trace!(spool_id, status = ?state.status, "scheduling spool recovery");
                desired.insert(Task::SpoolRecovery { spool: *spool_id });
                if !store.is_scan_done(*spool_id).unwrap_or(false) {
                    desired.insert(Task::RecoveryScan { spool: *spool_id });
                }
            }
        }
    }

    /// Transition a spool from ActiveSync to ActiveRecover when sync has failed.
    /// Called from the scheduler when a SpoolSync task fails permanently.
    pub fn transition_to_recovery<S: Store>(store: &TapeStore<S>, spool: u16) {
        let current_state = store.get_spool_state(spool).ok().flatten();
        let Some(state) = current_state else {
            tracing::debug!(spool, "ignoring stale spool sync failure: no state");
            return;
        };
        if !state.is_syncing() {
            tracing::debug!(spool, status = ?state.status, "ignoring stale spool sync failure");
            return;
        }
        let new_state = SpoolState { status: SpoolStatus::ActiveRecover, epoch: state.epoch, prev_owner: state.prev_owner };
        if let Err(e) = store.set_spool_state(spool, new_state) {
            tracing::error!(spool, "failed to set ActiveRecover: {e}");
            return;
        }
        let _ = store.remove_spool_sync_cursor(spool);
        let _ = store.clear_scan_done(spool);
        tracing::info!(spool, "spool sync failed, transitioning to recovery");
    }

    /// After a track is certified, check owned spools for missing slices and
    /// enqueue SpoolRecovery tasks for any gaps.
    pub fn check_slices<S: Store>(
        store: &TapeStore<S>,
        node_status: NodeStatus,
        track: &Pubkey,
        desired: &mut HashSet<Task>,
    ) {
        tracing::trace!(track = %track, "checking slices for track");
        if matches!(node_status, NodeStatus::Standby) {
            tracing::trace!(track = %track, "check_slices skipped for standby node");
            return;
        }

        let store_track: StorePubkey = track.into();

        let track_info = match store.get_track(store_track) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::trace!(track = %track, "check_slices skipped: track not found");
                return;
            }
            Err(error) => {
                tracing::trace!(
                    track = %track,
                    error = ?error,
                    "check_slices skipped: failed to read track"
                );
                return;
            }
        };

        let owned_spools = match store.iter_all_spools() {
            Ok(s) => s,
            Err(error) => {
                tracing::trace!(
                    track = %track,
                    error = ?error,
                    "check_slices skipped: failed to read owned spools"
                );
                return;
            }
        };

        for (spool_id, state) in &owned_spools {
            tracing::trace!(
                track = %track,
                spool_id,
                spool_status = ?state.status,
                "evaluating spool recovery scheduling"
            );
            if !state.is_active() && !state.is_recovering() {
                continue;
            }
            if !spool_in_group(*spool_id, track_info.spool_group) {
                continue;
            }
            match store.has_slice(*spool_id, store_track) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::trace!(spool_id, track = %track, "scheduling spool recovery for missing slice");
                    let _ = store.add_pending_recovery(*spool_id, store_track);
                    desired.insert(Task::SpoolRecovery { spool: *spool_id });
                }
                Err(_) => {}
            }
        }
    }

    /// Remove pending recovery entries for a track that was deleted or invalidated.
    pub fn remove_recoveries<S: Store>(store: &TapeStore<S>, track: &Pubkey) {
        let store_track: StorePubkey = track.into();
        let owned_spools = match store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };
        for (spool_id, _) in &owned_spools {
            let _ = store.remove_pending_recovery(*spool_id, store_track);
        }
    }

    /// Create store rows for newly assigned spools, lock lost ones.
    ///
    /// Compares `chain_spools` against existing store rows.
    /// New assignments get `ActiveSync` status, triggering `SpoolSync` tasks.
    /// Lost spools transition to `LockedToMove` — the old owner must keep
    /// serving data until the new owner completes sync.
    ///
    /// Returns true if any spool rows changed.
    pub fn reconcile_ownership<S: Store>(
        store: &TapeStore<S>,
        chain_spools: &HashSet<u16>,
        epoch: EpochNumber,
        self_node_id: NodeId,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> bool {
        let existing = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("reconcile_ownership: failed to read spools: {e}");
                return false;
            }
        };

        let existing_ids: HashSet<u16> = existing.iter().map(|(id, _)| *id).collect();

        let mut changed = false;

        // New assignments → ActiveSync
        for &spool in chain_spools {
            if !existing_ids.contains(&spool) {
                let prev_owner = Self::prev_owner_for(spool, prev_spools, prev_committee);
                let state = SpoolState { status: SpoolStatus::ActiveSync, epoch, prev_owner };
                if let Err(e) = store.set_spool_state(spool, state) {
                    tracing::error!(spool, "reconcile_ownership: failed to create spool: {e}");
                } else {
                    tracing::info!(spool, ?prev_owner, "spool assigned, marked ActiveSync");
                    changed = true;
                }
            }
        }

        // Lost assignments → LockedToMove (keep data for new owner to sync)
        // Skip if already LockedToMove to preserve original lock epoch.
        for &(spool, ref state) in &existing {
            if !chain_spools.contains(&spool) {
                if state.is_locked() {
                    continue;
                }
                let new_state = SpoolState { status: SpoolStatus::LockedToMove, epoch, prev_owner: None };
                if let Err(e) = store.set_spool_state(spool, new_state) {
                    tracing::error!(spool, "reconcile_ownership: failed to lock spool: {e}");
                } else {
                    let _ = store.clear_scan_done(spool);
                    tracing::info!(spool, "spool lost, marked LockedToMove");
                    changed = true;
                }
            }

            // Reacquired: spool was LockedToMove but is back in chain_spools
            if chain_spools.contains(&spool) && state.is_locked() {
                let new_state = SpoolState {
                    status: SpoolStatus::ActiveSync,
                    epoch,
                    prev_owner: Some(self_node_id),
                };
                if let Err(e) = store.set_spool_state(spool, new_state) {
                    tracing::error!(spool, "reconcile_ownership: failed to reactivate spool: {e}");
                } else {
                    tracing::info!(spool, "locked spool reacquired, marked ActiveSync");
                    changed = true;
                }
            }
        }

        changed
    }

    /// Remove spools marked `LockedToMove` that were locked at least 2 epochs ago.
    /// Called at `EpochAdvanced` so old owners keep serving data long enough for
    /// new owners to complete sync.
    pub fn cleanup_locked<S: Store>(store: &TapeStore<S>, current_epoch: EpochNumber) {
        let spools = match store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };
        for (spool_id, state) in &spools {
            if state.is_locked() && state.epoch.0 + 2 <= current_epoch.0 {
                match store.delete_all_slices_for_spool(*spool_id) {
                    Ok(count) => {
                        if count > 0 {
                            tracing::info!(spool_id, count, "deleted orphaned slices");
                        }
                    }
                    Err(e) => tracing::error!(spool_id, "delete slices: {e}"),
                }
                let _ = store.clear_all_pending_recoveries(*spool_id);
                let _ = store.remove_spool_sync_cursor(*spool_id);
                let _ = store.clear_scan_done(*spool_id);
                if let Err(e) = store.remove_spool_state(*spool_id) {
                    tracing::error!(spool_id, "cleanup_locked: {e}");
                } else {
                    tracing::info!(spool_id, "locked spool cleaned up");
                }
            }
        }
    }

    /// Remove pending recovery entries whose tracks no longer exist in the store
    /// (e.g. deleted or invalidated). Clears SpoolRecovery from desired when a
    /// spool has no remaining pending recoveries.
    pub fn prune_recoveries<S: Store>(store: &TapeStore<S>, desired: &mut HashSet<Task>) {
        let spools = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(_) => return,
        };

        for (spool, state) in &spools {
            let pending = match store.iter_pending_recoveries(*spool, 1024) {
                Ok(pending) => pending,
                Err(_) => continue,
            };

            let mut remaining = pending.len();
            for track in &pending {
                let missing = match store.get_track(*track) {
                    Ok(track_info) => track_info.is_none(),
                    Err(_) => false,
                };
                if missing {
                    let _ = store.remove_pending_recovery(*spool, *track);
                    remaining -= 1;
                }
            }

            let has_pending = remaining > 0;

            if !has_pending && !state.is_recovering() {
                desired.remove(&Task::SpoolRecovery { spool: *spool });
            }
        }
    }
}

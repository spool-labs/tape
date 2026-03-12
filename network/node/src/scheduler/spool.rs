use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_store::TapeStore;

use tape_core::erasure::{slice_for_spool, spool_in_group, SPOOL_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::{SpoolAssignment, SpoolIndex};
use tape_core::system::CommitteeMember;
use tape_core::types::{EpochNumber, NodeId};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{NodeStatus, Pubkey as StorePubkey, SpoolState, TrackInfo};

use crate::Task;

/// Validate an untrusted slice before local persistence.
pub fn validate_slice_entry(
    spool: SpoolIndex,
    track_info: &TrackInfo,
    data: &[u8],
) -> Result<(), String> {

    let slice_index = slice_for_spool(track_info.spool_group, spool)
        .ok_or_else(|| "track not mapped to this spool group".to_string())?;

    if track_info.original_size > 0 && data.is_empty() {
        return Err("empty slice for non-empty track".to_string());
    }

    let expected_max = track_info
        .stripe_size
        .checked_mul(track_info.stripe_count)
        .ok_or_else(|| "invalid stripe dimensions".to_string())?;

    if expected_max > 0 && data.len() as u64 > expected_max {
        return Err("slice exceeds expected decoded size".to_string());
    }

    if !track_info.verify_slice(slice_index, data) {
        return Err("slice does not match commitment".to_string());
    }

    Ok(())
}

const LOCKED_SPOOL_RETENTION_EPOCHS: u64 = 4;

pub struct SpoolPlanner;

impl SpoolPlanner {
    /// Recompute desired spool-scoped tasks from durable spool store state.
    ///
    /// This keeps the scheduler aligned with the node's current owned spools by
    /// pruning tasks for unschedulable spools and adding `SpoolSync`,
    /// `RecoveryScan`, and `SpoolRecovery` for spools in `Sync` / `Scan` / `Recover`.
    pub fn plan_spool_tasks<S: Store>(
        store: &TapeStore<S>,
        node_status: NodeStatus,
        desired: &mut HashSet<Task>,
    ) {
        if matches!(node_status, NodeStatus::Standby) {
            tracing::trace!("plan_spool_tasks skipped for standby node");
            return;
        }
        tracing::trace!("planning spool tasks");

        let owned_spools = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("failed to read spool status: {e}");
                return;
            }
        };
        tracing::trace!(owned_spools = owned_spools.len(), "planning spool tasks");

        let schedulable_spools = Self::schedulable_spools(&owned_spools);
        Self::prune_desired_spool_tasks(desired, &schedulable_spools);
        Self::add_spool_tasks(store, &owned_spools, desired);
    }

    /// Build the spool-id set that may have active spool tasks scheduled.
    /// `LockedToMove` spools are intentionally excluded.
    fn schedulable_spools(
        owned_spools: &[(SpoolIndex, SpoolState)],
    ) -> HashSet<SpoolIndex> {
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
    fn prune_desired_spool_tasks(
        desired: &mut HashSet<Task>,
        schedulable_spools: &HashSet<SpoolIndex>,
    ) {
        desired.retain(|task| match task.spool_id() {
            Some(spool_id) => schedulable_spools.contains(&spool_id),
            None => true,
        });
    }

    /// Add spool tasks based on each spool's current status.
    fn add_spool_tasks<S: Store>(
        _store: &TapeStore<S>,
        owned_spools: &[(SpoolIndex, SpoolState)],
        desired: &mut HashSet<Task>,
    ) {
        for (spool_id, state) in owned_spools {
            if state.is_syncing() {
                tracing::trace!(spool_id, ?state, "scheduling spool sync");
                desired.insert(Task::SpoolSync { spool: *spool_id });
            }
            if state.is_scanning() {
                tracing::trace!(spool_id, ?state, "scheduling recovery scan");
                desired.insert(Task::RecoveryScan { spool: *spool_id });
            }
            if state.is_recovering() {
                tracing::trace!(spool_id, ?state, "scheduling spool recovery");
                desired.insert(Task::SpoolRecovery { spool: *spool_id });
            }
        }
    }

    /// Move a spool from `Sync` to `Scan` after sync permanently fails.
    ///
    /// This preserves frozen handoff metadata while clearing sync-specific
    /// runtime markers so the scan phase starts from a clean lifecycle boundary.
    pub fn transition_to_scan<S: Store>(store: &TapeStore<S>, spool: SpoolIndex) {
        let current_state = store.get_spool_state(spool).ok().flatten();
        let Some(state) = current_state else {
            tracing::debug!(spool, "ignoring stale spool sync failure: no state");
            return;
        };
        if !state.is_syncing() {
            tracing::debug!(spool, ?state, "ignoring stale spool sync failure");
            return;
        }
        let new_state = match state {
            SpoolState::Sync { epoch, prev_owner, prev_helpers } => {
                SpoolState::Scan { epoch, prev_owner, prev_helpers }
            }
            _ => return,
        };
        if let Err(e) = store.set_spool_state(spool, new_state) {
            tracing::error!(spool, "failed to set Scan: {e}");
            return;
        }
        Self::reset_spool_task_state(store, spool);
        tracing::info!(spool, "spool sync failed, transitioning to scan");
    }

    /// Scan owned spools for a certified track and enqueue recovery for gaps.
    ///
    /// Only `Active` and `Recover` spools are eligible. `Sync` spools are still
    /// converging via handoff, and `LockedToMove` spools are old-owner retention
    /// state that should not schedule new recovery work.
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
                spool_state = ?state,
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
                Err(error) => {
                    tracing::warn!(
                        spool_id,
                        track = %track,
                        error = ?error,
                        "check_slices skipped: failed to read local slice presence"
                    );
                }
            }
        }
    }

    /// Remove pending recovery entries for a track that no longer needs repair.
    ///
    /// Called after delete/invalidate flows so stale recovery work does not
    /// linger on any locally retained spool state.
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

    /// Reconcile durable spool rows against the spools assigned to this node.
    ///
    /// Compares `chain_spools` against existing store rows.
    /// New assignments get `Sync` state, triggering `SpoolSync` tasks.
    /// Lost spools transition to `LockedToMove` — the old owner must keep
    /// serving data until the new owner completes sync.
    ///
    /// Returns true if any spool rows changed.
    pub fn apply_ownership_changes<S: Store>(
        store: &TapeStore<S>,
        chain_spools: &HashSet<SpoolIndex>,
        epoch: EpochNumber,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> bool {
        let existing = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("apply_ownership_changes: failed to read spools: {e}");
                return false;
            }
        };

        let existing_ids: HashSet<SpoolIndex> =
            existing.iter().map(|(id, _)| *id).collect();
        let mut changed = false;

        changed |= Self::apply_new_assignments(
            store,
            chain_spools,
            &existing_ids,
            epoch,
            prev_spools,
            prev_committee,
        );
        changed |= Self::apply_losses_and_reacquires(
            store,
            chain_spools,
            &existing,
            epoch,
            prev_spools,
            prev_committee,
        );

        changed
    }

    /// Delete expired `LockedToMove` spool rows and their retained local state.
    ///
    /// Called at epoch boundaries so old owners keep serving long enough for new
    /// owners to complete `Sync`, but are eventually purged once the retention
    /// window has elapsed.
    pub fn cleanup_locked<S: Store>(store: &TapeStore<S>, current_epoch: EpochNumber) {
        let spools = match store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };
        for (spool_id, state) in &spools {
            if state.is_locked()
                && state
                    .epoch().0
                    .saturating_add(LOCKED_SPOOL_RETENTION_EPOCHS)
                    <= current_epoch.0
            {
                Self::purge_locked_spool(store, *spool_id);
            }
        }
    }

    /// Remove stale pending recovery entries for tracks no longer in the store.
    ///
    /// This keeps `SpoolRecovery` scheduling aligned with actual recoverable
    /// work after delete/invalidate flows or store cleanup.
    pub fn prune_recoveries<S: Store>(store: &TapeStore<S>, desired: &mut HashSet<Task>) {
        let spools = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(_) => return,
        };

        for (spool, state) in &spools {
            let pending = match store.iter_pending_recoveries(*spool, 1024) {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::warn!(
                        spool,
                        error = ?error,
                        "prune_recoveries skipped: failed to enumerate pending recoveries"
                    );
                    continue;
                }
            };

            let mut remaining = pending.len();
            for track in &pending {
                let missing = match store.get_track(*track) {
                    Ok(track_info) => track_info.is_none(),
                    Err(error) => {
                        tracing::warn!(
                            spool,
                            track = ?track,
                            error = ?error,
                            "prune_recoveries skipped: failed to read track"
                        );
                        false
                    }
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

    fn apply_new_assignments<S: Store>(
        store: &TapeStore<S>,
        chain_spools: &HashSet<SpoolIndex>,
        existing_ids: &HashSet<SpoolIndex>,
        epoch: EpochNumber,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> bool {
        let mut changed = false;

        for &spool in chain_spools {
            if existing_ids.contains(&spool) {
                continue;
            }

            let state = Self::make_sync_state(spool, epoch, prev_spools, prev_committee);
            let prev_owner = state.prev_owner();
            if let Err(e) = store.set_spool_state(spool, state) {
                tracing::error!(spool, "apply_ownership_changes: failed to create spool: {e}");
            } else {
                tracing::info!(spool, ?prev_owner, "spool assigned, marked Sync");
                changed = true;
            }
        }

        changed
    }

    fn apply_losses_and_reacquires<S: Store>(
        store: &TapeStore<S>,
        chain_spools: &HashSet<SpoolIndex>,
        existing: &[(SpoolIndex, SpoolState)],
        epoch: EpochNumber,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> bool {
        let mut changed = false;

        for &(spool, ref state) in existing {
            if !chain_spools.contains(&spool) {
                if state.is_locked() {
                    continue;
                }

                let new_state = SpoolState::LockedToMove { epoch };
                if let Err(e) = store.set_spool_state(spool, new_state) {
                    tracing::error!(spool, "apply_ownership_changes: failed to lock spool: {e}");
                } else {
                    Self::reset_spool_task_state(store, spool);
                    tracing::info!(spool, "spool lost, marked LockedToMove");
                    changed = true;
                }
                continue;
            }

            if state.is_locked() {
                let new_state = Self::make_sync_state(spool, epoch, prev_spools, prev_committee);
                if let Err(e) = store.set_spool_state(spool, new_state) {
                    tracing::error!(spool, "apply_ownership_changes: failed to reactivate spool: {e}");
                } else {
                    Self::reset_spool_task_state(store, spool);
                    tracing::info!(spool, "locked spool reacquired, marked Sync");
                    changed = true;
                }
            }
        }

        changed
    }

    fn make_sync_state(
        spool: SpoolIndex,
        epoch: EpochNumber,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> SpoolState {
        let prev_owner = Self::prev_owner_for(spool, prev_spools, prev_committee);
        let prev_helpers = Self::prev_helpers_for(spool, prev_spools, prev_committee);

        SpoolState::Sync {
            epoch,
            prev_owner,
            prev_helpers,
        }
    }

    fn reset_spool_task_state<S: Store>(store: &TapeStore<S>, spool: SpoolIndex) {
        let _ = store.clear_all_pending_recoveries(spool);
        let _ = store.remove_spool_sync_cursor(spool);
    }

    fn purge_locked_spool<S: Store>(store: &TapeStore<S>, spool: SpoolIndex) {
        match store.delete_all_slices_for_spool(spool) {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(spool, count, "deleted orphaned slices");
                }
            }
            Err(e) => tracing::error!(spool, "delete slices: {e}"),
        }
        Self::reset_spool_task_state(store, spool);
        if let Err(e) = store.remove_spool_state(spool) {
            tracing::error!(spool, "cleanup_locked: {e}");
        } else {
            tracing::info!(spool, "locked spool cleaned up");
        }
    }

    fn prev_owner_for(
        spool: SpoolIndex,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> Option<NodeId> {
        prev_spools.0
            .get(spool as usize)
            .and_then(|&member_idx| prev_committee.get(member_idx as usize))
            .map(|member| member.id)
    }

    fn prev_helpers_for(
        spool: SpoolIndex,
        prev_spools: &SpoolAssignment<SPOOL_COUNT>,
        prev_committee: &[CommitteeMember],
    ) -> [Option<NodeId>; SPOOL_GROUP_SIZE] {
        let group = tape_core::spooler::SpoolGroup::of(spool);
        let mut out = [None; SPOOL_GROUP_SIZE];

        for (slot, owner) in out.iter_mut().enumerate() {
            let peer_spool = group.base() + slot as u16;
            *owner = prev_spools
                .0
                .get(peer_spool as usize)
                .and_then(|&member_idx| prev_committee.get(member_idx as usize))
                .map(|member| member.id);
        }

        out
    }
}

use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_store::TapeStore;

use tape_core::erasure::spool_in_group;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{NodeStatus, Pubkey as StorePubkey, SpoolStatus};

use crate::Task;

pub struct SpoolPlanner;

impl SpoolPlanner {
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

        // Remove SpoolSync/SpoolRecovery/RecoveryScan for spools we no longer own
        desired.retain(|key| match key {
            Task::SpoolSync { spool }
            | Task::SpoolRecovery { spool }
            | Task::RecoveryScan { spool } => owned_spools.iter().any(|(id, _)| *id == *spool),
            _ => true,
        });

        // Add tasks for owned spools based on their status
        for (spool_id, status) in &owned_spools {
            if matches!(status, SpoolStatus::ActiveSync) {
                tracing::trace!(spool_id, status = ?status, "scheduling spool sync");
                desired.insert(Task::SpoolSync { spool: *spool_id });
            }
            if matches!(status, SpoolStatus::ActiveRecover) {
                tracing::trace!(spool_id, status = ?status, "scheduling spool recovery");
                desired.insert(Task::SpoolRecovery { spool: *spool_id });
            }
        }
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

        for (spool_id, status) in &owned_spools {
            tracing::trace!(
                track = %track,
                spool_id,
                spool_status = ?status,
                "evaluating spool recovery scheduling"
            );
            if !matches!(status, SpoolStatus::Active | SpoolStatus::ActiveRecover) {
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

    /// Remove pending recovery entries whose tracks no longer exist in the store
    /// (e.g. deleted or invalidated). Clears SpoolRecovery from desired when a
    /// spool has no remaining pending recoveries.
    pub fn prune_recoveries<S: Store>(store: &TapeStore<S>, desired: &mut HashSet<Task>) {
        let spools = match store.iter_all_spools() {
            Ok(spools) => spools,
            Err(_) => return,
        };

        for (spool, status) in &spools {
            let pending = match store.iter_pending_recoveries(*spool, 1024) {
                Ok(pending) => pending,
                Err(_) => continue,
            };

            for track in &pending {
                let missing = match store.get_track(*track) {
                    Ok(track_info) => track_info.is_none(),
                    Err(_) => false,
                };
                if missing {
                    let _ = store.remove_pending_recovery(*spool, *track);
                }
            }

            let has_pending = !pending.is_empty();

            if !has_pending && !matches!(status, SpoolStatus::ActiveRecover) {
                desired.remove(&Task::SpoolRecovery { spool: *spool });
            }
        }
    }
}

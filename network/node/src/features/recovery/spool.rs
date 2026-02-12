//! Spool recovery — bulk transfer + per-track recovery for newly assigned spools.
//!
//! For each recovering spool:
//! 1. Try bulk transfer from the previous owner (ActiveSync phase)
//! 2. Fall back to per-track recovery for any remaining slices (ActiveRecover phase)
//! 3. Mark spool Active on completion
//!
//! Calls `mark_local_sync_complete` when all spools are recovered.

use std::sync::Arc;

use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey, SpoolStatus};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::core::context::NodeContext;
use crate::features::sync::{SpoolSyncHandler, SyncError, SyncSlice};

use super::deferral::LiveUploadDeferral;
use super::helpers::resolve_previous_owner;
use super::scan::run_scan;
use super::scheduler::TrackSyncHandler;
use super::worker::recover_track_slice;

/// Maximum queued recovery tasks before backpressure (spool recovery phase 2).
const RECOVERY_TRACK_CONCURRENCY: usize = 1000;

/// Verify a sync slice against the on-chain commitment and store it if valid.
///
/// When track metadata is available, verifies the slice data against the stored
/// commitment leaf hash for the spool's position in the group. Falls back to
/// transit-integrity check (sender's claimed leaf hash) when metadata is unavailable.
///
/// Returns Ok(true) if stored, Ok(false) if skipped (verification failed).
pub(crate) fn verify_and_store_sync_slice<S: Store>(
    store: &tape_store::TapeStore<S>,
    spool: SpoolIndex,
    group: SpoolGroup,
    slice: &SyncSlice,
) -> Result<bool, SyncError> {
    let position = (spool - group_start(group)) as usize;

    // If we have track metadata, verify against on-chain commitment
    if let Ok(Some(track_info)) = store.get_track(slice.track_address) {
        if !track_info.verify_slice(position, &slice.data) {
            return Ok(false);
        }
    } else {
        // Fallback: verify sender's claimed hash (transit integrity only)
        let computed_hash = tape_crypto::merkle::hash_leaf(&slice.data);
        if computed_hash != slice.leaf_hash {
            return Ok(false);
        }
    }

    store
        .put_slice(spool, slice.track_address, slice.data.clone())
        .map_err(|e| SyncError::Storage(e.to_string()))?;
    Ok(true)
}

/// Recover all non-Active spools using bulk transfer + per-track recovery.
///
/// For each recovering spool:
/// 1. Try bulk transfer from the previous owner (ActiveSync phase)
/// 2. Fall back to per-track recovery for any remaining slices (ActiveRecover phase)
/// 3. Mark spool Active on completion
///
/// Calls `mark_local_sync_complete` when all spools are recovered.
pub async fn start_spool_recovery<S: Store + 'static>(
    ctx: Arc<NodeContext<S>>,
    sync_handler: SpoolSyncHandler,
    cancel: CancellationToken,
) {
    let our_spools = ctx.control_plane.get_our_spools();
    let epoch = ctx.control_plane.current_epoch();

    // Identify spools needing recovery (not Active in store)
    let mut spools_to_recover: Vec<SpoolIndex> = Vec::new();
    for &spool in &our_spools {
        match ctx.storage.store.get_spool_status(spool) {
            Ok(Some(SpoolStatus::Active)) => continue,
            _ => spools_to_recover.push(spool),
        }
    }

    if spools_to_recover.is_empty() {
        info!("all spools already active");
        ctx.control_plane.mark_local_sync_complete(epoch);
        return;
    }

    info!(spools = spools_to_recover.len(), "starting spool recovery");

    // Phase 1: Bulk transfer from previous owners
    for &spool in &spools_to_recover {
        if cancel.is_cancelled() {
            break;
        }

        if let Err(e) = ctx.storage.store.set_spool_status(spool, SpoolStatus::ActiveSync) {
            warn!(spool, error = %e, "failed to set spool status to ActiveSync");
        }

        let group = group_for_spool(spool);
        if let Some(prev_addr) = resolve_previous_owner(&ctx, spool) {
            // Load persisted cursor for crash resume
            let resume_cursor = ctx.storage.store.get_spool_sync_cursor(spool)
                .ok()
                .flatten();

            let store_ref = &ctx.storage.store;
            let on_slice = |slice: SyncSlice| -> Result<(), SyncError> {
                if !verify_and_store_sync_slice(store_ref, spool, group, &slice)? {
                    warn!(track = %slice.track_address, "received slice failed verification, skipping");
                }
                Ok(())
            };

            let on_batch = |last_track: &Pubkey| -> Result<(), SyncError> {
                store_ref
                    .set_spool_sync_cursor(spool, *last_track)
                    .map_err(|e| SyncError::Storage(e.to_string()))?;
                Ok(())
            };

            let result = sync_handler
                .sync_spool_with_retry(
                    spool,
                    epoch,
                    prev_addr,
                    on_slice,
                    resume_cursor,
                    Some(on_batch),
                    &cancel,
                )
                .await;

            match result {
                Ok(count) => {
                    info!(spool, slices = count, "bulk transfer complete");
                    if let Err(e) = ctx.storage.store.remove_spool_sync_cursor(spool) {
                        warn!(spool, error = %e, "failed to remove spool sync cursor");
                    }
                    if let Err(e) = ctx.storage.store.set_spool_status(spool, SpoolStatus::Active) {
                        warn!(spool, error = %e, "failed to set spool status to Active after sync");
                    }
                    continue; // Skip per-track recovery for this spool
                }
                Err(e) => {
                    warn!(spool, error = %e, "bulk transfer failed, falling through to per-track recovery");
                }
            }
        }

        if let Err(e) = ctx.storage.store.set_spool_status(spool, SpoolStatus::ActiveRecover) {
            warn!(spool, error = %e, "failed to set spool status to ActiveRecover");
        }
    }

    // Phase 2: Per-track recovery for remaining non-Active spools
    let mut remaining: Vec<SpoolIndex> = Vec::new();
    for &spool in &spools_to_recover {
        match ctx.storage.store.get_spool_status(spool) {
            Ok(Some(SpoolStatus::Active)) => continue,
            _ => remaining.push(spool),
        }
    }

    if !remaining.is_empty() {
        // Scan to populate pending recovery queues
        let recovering: Vec<(SpoolIndex, SpoolGroup)> = remaining
            .iter()
            .map(|&s| (s, group_for_spool(s)))
            .collect();

        match run_scan(&ctx.storage.store, &recovering) {
            Ok(result) => info!(scanned = result.scanned, enqueued = result.enqueued, "scan complete"),
            Err(e) => warn!(error = %e, "scan failed"),
        }

        // Dispatch per-track recovery
        let track_sync = Arc::new(TrackSyncHandler::new());
        let deferral = Arc::new(LiveUploadDeferral::default());
        let slice_semaphore = track_sync.slice_semaphore();
        let queue_semaphore = Arc::new(Semaphore::new(RECOVERY_TRACK_CONCURRENCY));

        for &spool in &remaining {
            if cancel.is_cancelled() {
                break;
            }

            let pending = match ctx.storage.store.iter_pending_recoveries(spool, usize::MAX) {
                Ok(p) => p,
                Err(e) => {
                    warn!(spool, error = %e, "read pending failed");
                    continue;
                }
            };

            for track_address in pending {
                if cancel.is_cancelled() {
                    break;
                }

                let permit = match queue_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let ctx = Arc::clone(&ctx);
                let deferral = Arc::clone(&deferral);
                let slice_sem = Arc::clone(&slice_semaphore);
                let cancel = cancel.clone();

                track_sync
                    .start_sync(track_address, Box::pin(async move {
                        recover_track_slice(ctx, spool, track_address, deferral, slice_sem, cancel)
                            .await;
                        drop(permit);
                    }))
                    .await;
            }
        }

        track_sync.wait_all().await;
    }

    // Mark all recovered spools as Active
    for &spool in &spools_to_recover {
        if let Err(e) = ctx.storage.store.set_spool_status(spool, SpoolStatus::Active) {
            warn!(spool, error = %e, "failed to set spool status to Active after recovery");
        }
    }

    info!("spool recovery complete");
    ctx.control_plane.mark_local_sync_complete(epoch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::erasure::group_start;
    use tape_crypto::Hash;
    use tape_node_api::MERKLE_HEIGHT;
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_sync_slice(data: Vec<u8>, leaf_hash: Hash) -> SyncSlice {
        SyncSlice {
            track_address: Pubkey::new_unique(),
            slice_index: 0,
            data,
            leaf_hash,
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        }
    }

    #[test]
    fn sync_slice_valid_hash_stores() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group) + 1;
        let data = vec![1, 2, 3, 4, 5];
        let hash = tape_crypto::merkle::hash_leaf(&data);
        let slice = make_sync_slice(data, hash);
        let track = slice.track_address;

        let stored = verify_and_store_sync_slice(&store, spool, group, &slice).unwrap();
        assert!(stored);
        assert!(store.has_slice(spool, track).unwrap());
    }

    #[test]
    fn sync_slice_tampered_data_skips() {
        let store = test_store();
        let group: SpoolGroup = 0;
        let spool: SpoolIndex = group_start(group) + 2;
        let data = vec![1, 2, 3, 4, 5];
        let wrong_hash = tape_crypto::merkle::hash_leaf(&[99]);
        let slice = make_sync_slice(data, wrong_hash);
        let track = slice.track_address;

        let stored = verify_and_store_sync_slice(&store, spool, group, &slice).unwrap();
        assert!(!stored);
        assert!(!store.has_slice(spool, track).unwrap());
    }
}

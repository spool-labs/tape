//! NodeRecoveryHandler — orchestrates full-node recovery.
//!
//! Triggered by NodeStatus FSM when entering `RecoveryInProgress(epoch)`.
//! Scans all certified tracks before the given epoch and dispatches
//! per-track recovery tasks via TrackSyncHandler.

use std::sync::Arc;

use store::Store;
use tape_core::erasure::{group_for_spool, spool_in_group};
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::EpochNumber;
use tape_store::ops::{MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::ObjectInfo;
use tape_store::types::{Pubkey, SpoolStatus};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::core::context::NodeContext;
use crate::features::spool_sync::{SpoolSyncHandler, SyncError, track_id_to_pubkey};

use super::deferral::LiveUploadDeferral;
use super::helpers::resolve_previous_owner;
use super::node_status;
use super::scan::run_scan;
use super::track_sync::TrackSyncHandler;
use super::track_synchronizer::recover_track_slice;
use super::{NodeEvent, evaluate_transition};

/// Run metadata sync for newly assigned spools.
///
/// The block processor stores TrackInfo for all RegisterTrack instructions.
/// Any remaining missing metadata is handled on-demand during per-track
/// recovery (fetch_metadata_from_peers in track_synchronizer).
/// This function unblocks the RecoverMetadata state.
pub async fn run_metadata_sync<S: Store + 'static>(
    ctx: Arc<NodeContext<S>>,
    sync_handler: SpoolSyncHandler,
    cancel: CancellationToken,
) {
    info!("metadata sync starting");

    if cancel.is_cancelled() {
        return;
    }

    let current_status = ctx.control_plane.get_node_status();
    let event = NodeEvent::MetadataSyncComplete;
    if let Some(new_status) = evaluate_transition(&current_status, &event) {
        info!(from = ?current_status, to = ?new_status, "metadata sync complete");
        ctx.control_plane.set_node_status(new_status.clone());
        if let Err(e) = ctx.storage.store.set_node_status(new_status) {
            warn!(error = %e, "failed to persist node status");
        }
        // Spawn spool recovery — calls mark_local_sync_complete when done
        let ctx2 = Arc::clone(&ctx);
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            start_spool_recovery(ctx2, sync_handler, cancel2).await;
        });
    }
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
    let insecure = ctx.config.insecure;
    for &spool in &spools_to_recover {
        if cancel.is_cancelled() {
            break;
        }

        let _ = ctx.storage.store.set_spool_status(spool, SpoolStatus::ActiveSync);

        if let Some((prev_addr, _client)) = resolve_previous_owner(&ctx, spool, insecure) {
            // Load persisted cursor for crash resume
            let resume_cursor = ctx.storage.store.get_spool_sync_cursor(spool)
                .ok()
                .flatten()
                .map(|p| {
                    let sdk_pubkey: solana_sdk::pubkey::Pubkey = p.into();
                    sdk_pubkey.to_string()
                });

            let store_ref = &ctx.storage.store;
            let on_slice = |slice: crate::features::spool_sync::SyncSlice| -> Result<(), SyncError> {
                let track_pubkey: Pubkey = track_id_to_pubkey(&slice.track_id)
                    .map(|p| p.into())
                    .map_err(|e| SyncError::Storage(e.to_string()))?;
                store_ref
                    .put_slice(slice.slice_index, track_pubkey, slice.data)
                    .map_err(|e| SyncError::Storage(e.to_string()))?;
                Ok(())
            };

            let on_batch = |last_track: &str| -> Result<(), SyncError> {
                let track_id = last_track.to_string();
                let track_pubkey: Pubkey = track_id_to_pubkey(&track_id)
                    .map(|p| p.into())
                    .map_err(|e| SyncError::Storage(e.to_string()))?;
                store_ref
                    .set_spool_sync_cursor(spool, track_pubkey)
                    .map_err(|e| SyncError::Storage(e.to_string()))?;
                Ok(())
            };

            let result = sync_handler
                .sync_spool_with_retry(
                    spool,
                    epoch,
                    &prev_addr,
                    on_slice,
                    resume_cursor,
                    Some(on_batch),
                    &cancel,
                )
                .await;

            match result {
                Ok(count) => {
                    info!(spool, slices = count, "bulk transfer complete");
                    let _ = ctx.storage.store.remove_spool_sync_cursor(spool);
                    let _ = ctx.storage.store.set_spool_status(spool, SpoolStatus::Active);
                    continue; // Skip per-track recovery for this spool
                }
                Err(e) => {
                    warn!(spool, error = %e, "bulk transfer failed, falling through to per-track recovery");
                }
            }
        }

        let _ = ctx.storage.store.set_spool_status(spool, SpoolStatus::ActiveRecover);
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
                    .start_sync(track_address, async move {
                        recover_track_slice(ctx, spool, track_address, deferral, slice_sem, cancel)
                            .await;
                        drop(permit);
                    })
                    .await;
            }
        }

        track_sync.wait_all().await;
    }

    // Mark all recovered spools as Active
    for &spool in &spools_to_recover {
        let _ = ctx.storage.store.set_spool_status(spool, SpoolStatus::Active);
    }

    info!("spool recovery complete");
    ctx.control_plane.mark_local_sync_complete(epoch);
}

/// Tracks scanned per DB page during node recovery.
const SCAN_BATCH_SIZE: usize = 1000;

/// Maximum queued recovery tasks before backpressure.
const RECOVERY_TRACK_CONCURRENCY: usize = 1000;

/// Run full node recovery for all certified tracks before the given epoch.
///
/// For each certified track whose spool group intersects with our spools:
/// - Skip if we already have the slice
/// - Otherwise dispatch a per-track recovery via TrackSyncHandler
///
/// After all syncs complete:
/// 1. Emits NodeEvent::RecoveryComplete to transition NodeStatus → Active
/// 2. Calls mark_local_sync_complete so SyncEpoch can be submitted
pub async fn start_node_recovery<S: Store + 'static>(
    ctx: Arc<NodeContext<S>>,
    epoch: EpochNumber,
    our_spools: Vec<SpoolIndex>,
    track_sync: Arc<TrackSyncHandler>,
    deferral: Arc<LiveUploadDeferral>,
    cancel: CancellationToken,
) {
    // Guard: abort if node is still replaying
    if node_status::is_replaying(&ctx.control_plane.get_node_status()) {
        info!("node is still replaying, deferring recovery");
        return;
    }

    info!(
        epoch = epoch.as_u64(),
        spools = our_spools.len(),
        "starting node recovery scan"
    );

    // Two-level concurrency: queue_semaphore (1000) limits queued tasks,
    // track_semaphore (100) in TrackSyncHandler limits active tasks
    let queue_semaphore = Arc::new(Semaphore::new(RECOVERY_TRACK_CONCURRENCY));
    let slice_semaphore = track_sync.slice_semaphore();

    let mut dispatched = 0usize;
    let mut scanned = 0usize;
    let mut cursor: Option<Pubkey> = None;

    loop {
        if cancel.is_cancelled() {
            info!("node recovery cancelled");
            break;
        }

        let batch = match ctx.storage.store.iter_tracks_from(cursor, SCAN_BATCH_SIZE) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "recovery scan DB read failed");
                break;
            }
        };

        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len();

        for (track_address, track_info) in &batch {
            scanned += 1;
            cursor = Some(*track_address);

            // Only recover certified tracks (certified_epoch is in ObjectInfo)
            let certified = matches!(
                ctx.storage.store.get_object_info(*track_address),
                Ok(Some(ObjectInfo::Valid { certified_epoch: Some(ce), .. })) if ce <= epoch
            );
            if !certified {
                continue;
            }

            // Skip empty tracks
            if track_info.original_size == 0 {
                continue;
            }

            let group = track_info.spool_group;

            // For each of our spools in this track's group, check if we need recovery
            for &spool in &our_spools {
                if !spool_in_group(spool, group) {
                    continue;
                }

                // Skip if slice already stored
                match ctx.storage.store.has_slice(spool, *track_address) {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        warn!(
                            spool,
                            track = %track_address,
                            error = %e,
                            "storage check failed during recovery scan"
                        );
                        continue;
                    }
                }

                // Backpressure: wait for queue slot
                let _queue_permit = match queue_semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("queue semaphore closed");
                        break;
                    }
                };

                let ctx = Arc::clone(&ctx);
                let track_addr = *track_address;
                let cancel = cancel.clone();
                let deferral = Arc::clone(&deferral);
                let slice_sem = Arc::clone(&slice_semaphore);

                track_sync
                    .start_sync(track_addr, async move {
                        recover_track_slice(ctx, spool, track_addr, deferral, slice_sem, cancel)
                            .await;
                        drop(_queue_permit);
                    })
                    .await;

                dispatched += 1;
            }
        }

        if batch_len < SCAN_BATCH_SIZE {
            break;
        }
    }

    info!(
        scanned,
        dispatched,
        epoch = epoch.as_u64(),
        "node recovery scan complete, waiting for syncs"
    );

    // Wait for all dispatched track syncs to complete
    track_sync.wait_all().await;

    info!(
        dispatched,
        epoch = epoch.as_u64(),
        "node recovery complete"
    );

    // Transition NodeStatus → Active via RecoveryComplete event
    let current_status = ctx.control_plane.get_node_status();
    let event = NodeEvent::RecoveryComplete { epoch };
    if let Some(new_status) = evaluate_transition(&current_status, &event) {
        ctx.control_plane.set_node_status(new_status.clone());
        if let Err(e) = ctx.storage.store.set_node_status(new_status) {
            warn!(error = %e, "failed to persist node status after recovery");
        }
    }

    // Signal that local sync is complete so SyncEpoch can proceed
    ctx.control_plane.mark_local_sync_complete(epoch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::erasure::group_start;
    use tape_store::types::TrackInfo;
    use tape_store::TapeStore;

    fn make_ctx_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_certified_track(group: u64) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: group,
            original_size: 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 2,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    fn make_uncertified_track(group: u64) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: group,
            original_size: 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 2,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    #[test]
    fn test_track_construction() {
        let info = make_certified_track(0);
        assert_eq!(info.spool_group, 0);

        let info2 = make_uncertified_track(0);
        assert_eq!(info2.spool_group, 0);
    }

    #[test]
    fn test_spool_group_intersection() {
        use tape_core::erasure::spool_in_group;

        let spool: SpoolIndex = group_start(3) + 5; // spool in group 3
        assert!(spool_in_group(spool, 3));
        assert!(!spool_in_group(spool, 4));
    }
}

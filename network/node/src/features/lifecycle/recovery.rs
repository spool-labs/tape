//! Node recovery scan — scans all tracks and dispatches per-track recovery tasks.
//!
//! Triggered by NodeStatus FSM when entering `RecoveryInProgress(epoch)`.
//! Scans all certified tracks before the given epoch and dispatches
//! per-track recovery tasks via TrackSyncHandler.

use std::sync::Arc;

use store::Store;
use tape_core::erasure::spool_in_group;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_store::ops::{MetaOps, ObjectInfoOps, SliceOps, TrackOps};
use tape_store::types::ObjectInfo;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::core::context::NodeContext;

use super::{NodeEvent, evaluate_transition};
use super::fsm;
use crate::features::recovery::{
    LiveUploadDeferral, TrackSyncHandler, recover_track_slice,
    RECOVERY_TRACK_CONCURRENCY, SCAN_BATCH_SIZE,
};

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
    if fsm::is_replaying(&ctx.control_plane.get_node_status()) {
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
    let mut cursor: Option<tape_store::types::Pubkey> = None;

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

                // Backpressure: wait for queue slot (with cancellation)
                let _queue_permit = tokio::select! {
                    _ = cancel.cancelled() => break,
                    result = queue_semaphore.clone().acquire_owned() => {
                        match result {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("queue semaphore closed");
                                break;
                            }
                        }
                    }
                };

                let ctx = Arc::clone(&ctx);
                let track_addr = *track_address;
                let cancel = cancel.clone();
                let deferral = Arc::clone(&deferral);
                let slice_sem = Arc::clone(&slice_semaphore);

                track_sync
                    .start_sync(track_addr, Box::pin(async move {
                        recover_track_slice(ctx, spool, track_addr, deferral, slice_sem, cancel)
                            .await;
                        drop(_queue_permit);
                    }))
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
    track_sync.wait_all(&cancel).await;

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

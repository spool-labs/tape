//! Thread D - Erasure Recovery
//!
//! Scans for missing slices and repairs them using bandwidth-optimal
//! Clay code repair from helper nodes in the same spool group.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::SpoolIndex;
use tape_slicer::adaptive::pick_stripe_size;
use tape_slicer::clay::ClayCoder;
use tape_slicer::metadata::SliceMetadata;
use tape_slicer::slicer::Slicer;
use tape_slicer::SliceIndex;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey as StorePubkey, SpoolAllocation, SpoolStatus, TrackInfo};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::context::NodeContext;

use super::helpers::{fan_out_repair_requests, resolve_group_helpers, GroupHelper};

/// Recovery polling interval.
const RECOVERY_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Tracks scanned per batch (DB reads only, fast).
const SCAN_BATCH_SIZE: usize = 1000;

/// Slices repaired per batch (network I/O, slow).
const REPAIR_BATCH_SIZE: usize = 10;

/// Error type for recovery operations.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("no committee members available")]
    NoCommittee,

    #[error("not enough helpers: needed {needed}, available {available}")]
    NotEnoughHelpers { needed: usize, available: usize },

    #[error("repair failed: {0}")]
    RepairFailed(String),

    #[error("node client error: {0}")]
    NodeClient(String),

    #[error("slicer error: {0}")]
    Slicer(String),
}

/// Run the recovery worker loop.
pub async fn run<S: Store>(
    ctx: Arc<NodeContext<S>>,
    cancel: CancellationToken,
) -> Result<(), RecoveryError> {
    info!("Recovery thread starting");

    let mut interval = tokio::time::interval(RECOVERY_POLL_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Recovery thread shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = poll_recovery(&ctx).await {
                    warn!(error = %e, "recovery poll failed");
                }
            }
        }
    }

    Ok(())
}

/// Single recovery poll cycle.
async fn poll_recovery<S: Store>(ctx: &NodeContext<S>) -> Result<(), RecoveryError> {
    let our_spools = ctx.control_plane.get_our_spools();
    if our_spools.is_empty() {
        return Ok(());
    }

    // Group spools by spool group to share helper resolution
    let mut by_group: HashMap<u64, Vec<SpoolIndex>> = HashMap::new();
    for &spool in &our_spools {
        let group = group_for_spool(spool);
        by_group.entry(group).or_default().push(spool);
    }

    for (group, spools) in &by_group {
        // Resolve helpers once per group
        let helpers = match resolve_group_helpers(ctx, spools[0]) {
            Ok(h) => h,
            Err(e) => {
                warn!(group, error = %e, "failed to resolve group helpers");
                continue;
            }
        };

        for &spool in spools {
            let status = ctx
                .storage
                .store
                .get_spool_status(spool)
                .map_err(|e| RecoveryError::Storage(e.to_string()))?;

            match status {
                Some(SpoolStatus::Active) => continue,
                Some(SpoolStatus::LockedToMove) => continue,
                None | Some(SpoolStatus::None) | Some(SpoolStatus::ActiveSync) => {
                    // Transition to ActiveRecover to begin scan
                    ctx.storage
                        .store
                        .set_spool_status(spool, SpoolStatus::ActiveRecover)
                        .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                    debug!(spool, "transitioned to ActiveRecover");
                }
                Some(SpoolStatus::ActiveRecover) => {
                    // Already in recovery, continue processing
                }
            }

            let scan_complete = scan_batch(ctx, spool, *group)?;
            let _repaired = repair_batch(ctx, spool, &helpers).await?;

            if scan_complete {
                // Check if pending queue is also empty
                let pending = ctx
                    .storage
                    .store
                    .iter_pending_recoveries(spool, 1)
                    .map_err(|e| RecoveryError::Storage(e.to_string()))?;

                if pending.is_empty() {
                    ctx.storage
                        .store
                        .set_spool_status(spool, SpoolStatus::Active)
                        .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                    ctx.storage
                        .store
                        .remove_sync_progress(spool)
                        .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                    info!(spool, "recovery complete, spool now Active");
                }
            }
        }
    }

    Ok(())
}

/// Scan a batch of tracks to find missing slices for the given spool.
///
/// Returns true if the scan is complete (last page reached).
fn scan_batch<S: Store>(
    ctx: &NodeContext<S>,
    spool: SpoolIndex,
    group: u64,
) -> Result<bool, RecoveryError> {
    let store = &ctx.storage.store;

    let cursor = store
        .get_sync_progress(spool)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;

    let batch = store
        .iter_tracks_from(cursor, SCAN_BATCH_SIZE)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;

    if batch.is_empty() {
        return Ok(true);
    }

    let mut enqueued = 0;
    let mut last_key = None;

    for (track_address, track_info) in &batch {
        last_key = Some(*track_address);

        // Filter: only tracks allocated to this spool group
        match track_info.spool_allocation {
            SpoolAllocation::SpoolGroup(g) if g == group => {}
            _ => continue,
        }

        // Skip if we already have the slice
        let has = store
            .has_slice(spool, *track_address)
            .map_err(|e| RecoveryError::Storage(e.to_string()))?;
        if has {
            continue;
        }

        store
            .add_pending_recovery(spool, *track_address)
            .map_err(|e| RecoveryError::Storage(e.to_string()))?;
        enqueued += 1;
    }

    // Persist cursor for crash-resumable scan
    if let Some(key) = last_key {
        store
            .set_sync_progress(spool, key)
            .map_err(|e| RecoveryError::Storage(e.to_string()))?;
    }

    let complete = batch.len() < SCAN_BATCH_SIZE;
    if enqueued > 0 || complete {
        debug!(spool, enqueued, complete, "scan batch");
    }

    Ok(complete)
}

/// Process a batch of pending recoveries for the given spool.
///
/// Returns the number of successfully repaired slices.
async fn repair_batch<S: Store>(
    ctx: &NodeContext<S>,
    spool: SpoolIndex,
    helpers: &[GroupHelper],
) -> Result<usize, RecoveryError> {
    let store = &ctx.storage.store;

    let batch: Vec<StorePubkey> = store
        .iter_pending_recoveries(spool, REPAIR_BATCH_SIZE)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;
    if batch.is_empty() {
        return Ok(0);
    }

    let mut repaired = 0;

    for track_address in batch {
        // Guard: already repaired (crash between put_slice and remove_pending)
        let has = store
            .has_slice(spool, track_address)
            .map_err(|e| RecoveryError::Storage(e.to_string()))?;
        if has {
            store
                .remove_pending_recovery(spool, track_address)
                .map_err(|e| RecoveryError::Storage(e.to_string()))?;
            continue;
        }

        // Guard: track deleted concurrently
        let track_info = match store
            .get_track(track_address)
            .map_err(|e| RecoveryError::Storage(e.to_string()))?
        {
            Some(info) => info,
            None => {
                store
                    .remove_pending_recovery(spool, track_address)
                    .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                continue;
            }
        };

        // Guard: spool reassigned
        if !ctx.control_plane.owns_spool(spool) {
            debug!(spool, "spool no longer owned, stopping repair batch");
            break;
        }

        match repair_slice(ctx, spool, track_address, &track_info, helpers).await {
            Ok(()) => {
                store
                    .remove_pending_recovery(spool, track_address)
                    .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                repaired += 1;
            }
            Err(e) => {
                warn!(
                    spool,
                    track = %track_address,
                    error = %e,
                    "repair failed, will retry"
                );
            }
        }
    }

    if repaired > 0 {
        debug!(spool, repaired, "repair batch complete");
    }

    Ok(repaired)
}

/// Repair a single missing slice using Clay code repair.
///
/// All metadata is derived locally from TrackInfo — zero RPC calls.
async fn repair_slice<S: Store>(
    ctx: &NodeContext<S>,
    our_spool: SpoolIndex,
    track_address: StorePubkey,
    track_info: &TrackInfo,
    helpers: &[GroupHelper],
) -> Result<(), RecoveryError> {
    let profile = track_info.profile();
    let blob_len = track_info.original_size as usize;
    let stripe_size = pick_stripe_size(blob_len);
    let clay_params = profile.clay_params();

    let coder = ClayCoder::from_params(clay_params);
    let slicer = Slicer::with_profile(coder, stripe_size, profile.is_clay(), profile);

    let group = group_for_spool(our_spool);
    let start = group_start(group);
    let our_position = (our_spool - start) as usize;

    let lost = SliceIndex::new(our_position)
        .ok_or_else(|| RecoveryError::RepairFailed("invalid position".into()))?;

    let available: Vec<SliceIndex> = helpers
        .iter()
        .filter_map(|h| SliceIndex::new(h.position))
        .collect();

    let plan = slicer
        .repair_plan_from_params(lost, &available, blob_len, stripe_size)
        .map_err(|e| RecoveryError::Slicer(e.to_string()))?;

    let track_id = tape_crypto::Pubkey::from(track_address).to_string();
    let insecure = ctx.config.insecure;
    let helper_data =
        fan_out_repair_requests(helpers, &plan, &track_id, insecure).await?;

    let metadata = SliceMetadata::with_profile(blob_len, stripe_size, profile);
    let metadata_bytes = metadata.to_bytes();

    let repaired_slice = slicer
        .repair(&plan, &helper_data, &metadata_bytes)
        .map_err(|e| RecoveryError::Slicer(e.to_string()))?;

    ctx.storage
        .store
        .put_slice(our_spool, track_address, repaired_slice)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;

    debug!(
        spool = our_spool,
        track = %track_address,
        blob_len,
        "slice repaired"
    );

    Ok(())
}

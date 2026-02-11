use std::collections::HashMap;

use futures::stream::{self, StreamExt};
use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::SpoolIndex;
use tape_slicer::adaptive::pick_stripe_size;
use tape_slicer::clay::ClayCoder;
use tape_slicer::metadata::SliceMetadata;
use tape_slicer::slicer::Slicer;
use tape_slicer::SliceIndex;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};
use tape_store::types::{Pubkey as StorePubkey, TrackInfo};
use tracing::{debug, warn};

use crate::core::context::NodeContext;

use super::error::RecoveryError;
use super::helpers::{fan_out_repair_requests, resolve_group_helpers, GroupHelper};

/// Slices repaired per batch (network I/O, slow).
const REPAIR_BATCH_SIZE: usize = 10;

/// Maximum concurrent repair operations within a batch.
const REPAIR_CONCURRENCY: usize = 4;

/// Maximum times a track repair can fail before being skipped.
const MAX_TRACK_RETRIES: u32 = 5;

/// Repair a single missing slice using Clay code bandwidth-optimal repair.
///
/// Resolves helpers, computes the repair plan, fans out sub-chunk requests,
/// reconstructs the slice, and verifies against the commitment.
/// Returns the repaired slice bytes — the caller decides whether to store.
pub async fn repair_single_slice<S: Store>(
    ctx: &NodeContext<S>,
    our_spool: SpoolIndex,
    track_address: StorePubkey,
    track_info: &TrackInfo,
) -> Result<Vec<u8>, RecoveryError> {
    let profile = track_info.profile();

    if !profile.is_clay() {
        return Err(RecoveryError::NotEnoughHelpers {
            needed: 0,
            available: 0,
        });
    }

    let insecure = ctx.config.insecure;
    let helpers = resolve_group_helpers(ctx, our_spool, insecure)?;

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

    let track_id = track_address.to_string();
    let helper_data = fan_out_repair_requests(&helpers, &plan, &track_id).await?;

    let metadata = SliceMetadata::with_profile(blob_len, stripe_size, profile);
    let metadata_bytes = metadata.to_bytes();

    let repaired_slice = slicer
        .repair(&plan, &helper_data, &metadata_bytes)
        .map_err(|e| RecoveryError::Slicer(e.to_string()))?;

    if !track_info.commitment.is_empty()
        && !track_info.verify_slice(our_position, &repaired_slice)
    {
        return Err(RecoveryError::RepairFailed(
            "repaired slice failed leaf hash verification".into(),
        ));
    }

    Ok(repaired_slice)
}

/// Process a batch of pending recoveries for the given spool.
///
/// Phase 1: Pre-filter (sequential, fast DB ops)
/// Phase 2: Concurrent repair (slow network I/O)
/// Phase 3: Post-process results (sequential, fast DB ops)
///
/// Returns the number of successfully repaired slices.
pub async fn repair_batch<S: Store>(
    ctx: &NodeContext<S>,
    spool: SpoolIndex,
    _helpers: &[GroupHelper],
    failures: &mut HashMap<(SpoolIndex, StorePubkey), u32>,
) -> Result<usize, RecoveryError> {
    let store = &ctx.storage.store;

    let batch: Vec<StorePubkey> = store.iter_pending_recoveries(spool, REPAIR_BATCH_SIZE)?;
    if batch.is_empty() {
        return Ok(0);
    }

    // Phase 1: Pre-filter (sequential, fast DB ops)
    let mut to_repair: Vec<(StorePubkey, TrackInfo)> = Vec::new();
    let mut removed = 0;

    for track_address in &batch {
        // Guard: already repaired (crash between put_slice and remove_pending)
        if store.has_slice(spool, *track_address)? {
            store.remove_pending_recovery(spool, *track_address)?;
            removed += 1;
            continue;
        }

        // Guard: track deleted concurrently
        let track_info = match store.get_track(*track_address)? {
            Some(info) => info,
            None => {
                store.remove_pending_recovery(spool, *track_address)?;
                removed += 1;
                continue;
            }
        };

        // Guard: spool reassigned
        if !ctx.control_plane.owns_spool(spool) {
            debug!(spool, "spool no longer owned, stopping repair batch");
            break;
        }

        // Guard: skip tracks that have failed too many times
        let fail_count = failures
            .get(&(spool, *track_address))
            .copied()
            .unwrap_or(0);
        if fail_count >= MAX_TRACK_RETRIES {
            debug!(
                spool,
                track = %track_address,
                fail_count,
                "skipping after too many failures"
            );
            continue;
        }

        to_repair.push((*track_address, track_info));
    }

    if to_repair.is_empty() {
        if removed > 0 {
            ctx.metrics
                .recovery_queue_len
                .set(batch.len() as i64 - removed as i64);
        }
        return Ok(0);
    }

    // Phase 2: Concurrent repair (slow network I/O)
    let results: Vec<(StorePubkey, Result<(), RecoveryError>)> = stream::iter(to_repair)
        .map(|(addr, info)| {
            let ctx = &ctx;
            async move {
                let result = repair_and_store(ctx, spool, addr, &info).await;
                (addr, result)
            }
        })
        .buffer_unordered(REPAIR_CONCURRENCY)
        .collect()
        .await;

    // Phase 3: Post-process (sequential, fast DB ops)
    let mut repaired = 0;
    for (addr, result) in results {
        match result {
            Ok(()) => {
                store.remove_pending_recovery(spool, addr)?;
                repaired += 1;
                ctx.metrics.slices_recovered_total.inc();
            }
            Err(e) => {
                *failures.entry((spool, addr)).or_default() += 1;
                ctx.metrics.recovery_failures_total.inc();
                warn!(
                    spool,
                    track = %addr,
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

/// Repair a single slice and store it. Used by repair_batch.
async fn repair_and_store<S: Store>(
    ctx: &NodeContext<S>,
    our_spool: SpoolIndex,
    track_address: StorePubkey,
    track_info: &TrackInfo,
) -> Result<(), RecoveryError> {
    let repaired_slice = repair_single_slice(ctx, our_spool, track_address, track_info).await?;

    ctx.storage
        .store
        .put_slice(our_spool, track_address, repaired_slice)?;

    debug!(
        spool = our_spool,
        track = %track_address,
        blob_len = track_info.original_size,
        "slice repaired"
    );

    Ok(())
}

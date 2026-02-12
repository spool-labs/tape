//! RecoveryService — full decode/re-encode recovery fallback.
//!
//! When bandwidth-optimal Clay repair fails (< d helpers available),
//! falls back to downloading k full slices, decoding the original data,
//! re-encoding all slices, and extracting the target slice.
//!
//! Includes:
//! - Merkle proof verification on downloaded slices
//! - Commitment verification after re-encode
//! - spawn_blocking for CPU-intensive decode/encode
//! - Adaptive download via SliceTracker pattern

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::SpoolIndex;
use tape_crypto::Hash;
use tape_node_client::NodeClient;
use tape_slicer::adaptive::pick_stripe_size;
use tape_slicer::clay::ClayCoder;
use tape_slicer::coder::ErasureCoder;
use tape_slicer::merkle_helpers::blob_merkle_root;
use tape_slicer::slicer::Slicer;
use tape_store::types::{Pubkey, TrackInfo};
use tracing::{debug, warn};

use crate::core::context::NodeContext;

use super::error::RecoveryError;
use super::helpers::resolve_group_helpers;

/// Number of concurrent slice downloads during full recovery.
const DOWNLOAD_CONCURRENCY: usize = 8;

/// Download k slices from helpers, decode, re-encode, and return the computed
/// merkle root along with all re-encoded slices.
///
/// This is the shared core of `compute_recovery_root`, `attempt_full_recovery`,
/// and the API inconsistency verification endpoint.
pub(crate) async fn download_and_reencode(
    helpers: Vec<(usize, SpoolIndex, NodeClient)>,
    track_info: &TrackInfo,
    track_address_str: &str,
    concurrency: usize,
) -> Result<(Hash, Vec<Vec<u8>>), RecoveryError> {
    let profile = track_info.profile();
    let clay_params = profile.clay_params();
    let k = clay_params.k() as usize;
    let blob_len = track_info.original_size as usize;

    if helpers.len() < k {
        return Err(RecoveryError::NotEnoughHelpers {
            needed: k,
            available: helpers.len(),
        });
    }

    let collected_count = Arc::new(AtomicUsize::new(0));
    let track_id = track_address_str.to_string();

    let download_results: Vec<(usize, Result<Vec<u8>, RecoveryError>)> = stream::iter(
        helpers.into_iter(),
    )
    .map(|(position, spool_idx, client)| {
        let tid = track_id.clone();
        let collected = Arc::clone(&collected_count);
        async move {
            if collected.load(Ordering::Relaxed) >= k {
                return (position, Err(RecoveryError::Skipped));
            }
            let result = client
                .get_slice(&tid, spool_idx)
                .await
                .map_err(|e| RecoveryError::NodeClient(e.to_string()));
            if result.is_ok() {
                collected.fetch_add(1, Ordering::Relaxed);
            }
            (position, result)
        }
    })
    .buffer_unordered(concurrency)
    .collect()
    .await;

    let mut collected_slices: Vec<(usize, Vec<u8>)> = Vec::new();
    for (position, result) in download_results {
        match result {
            Ok(data) => {
                if !track_info.commitment.is_empty()
                    && !track_info.verify_slice(position, &data)
                {
                    warn!(position, "downloaded slice failed leaf verification, skipping");
                    continue;
                }
                collected_slices.push((position, data));
                if collected_slices.len() >= k {
                    break;
                }
            }
            Err(RecoveryError::Skipped) => {}
            Err(e) => {
                warn!(position, error = %e, "failed to download slice");
            }
        }
    }

    if collected_slices.len() < k {
        return Err(RecoveryError::NotEnoughHelpers {
            needed: k,
            available: collected_slices.len(),
        });
    }

    let stripe_size = pick_stripe_size(blob_len);

    let all_slices = tokio::task::spawn_blocking(move || {
        let coder = ClayCoder::from_params(clay_params);
        let mut slicer = Slicer::with_profile(coder, stripe_size, true, profile);

        let chunks: Vec<(usize, &[u8])> = collected_slices
            .iter()
            .map(|(pos, data)| (*pos, data.as_slice()))
            .collect();

        let original = slicer
            .decode(&chunks)
            .map_err(|e| RecoveryError::Slicer(format!("decode failed: {}", e)))?;

        let all_slices = slicer
            .encode(&original)
            .map_err(|e| RecoveryError::Slicer(format!("re-encode failed: {}", e)))?;

        Ok::<_, RecoveryError>(all_slices)
    })
    .await
    .map_err(|e| RecoveryError::RepairFailed(format!("spawn_blocking panicked: {}", e)))??;

    let computed_root = blob_merkle_root(&all_slices);

    Ok((computed_root, all_slices))
}

/// Attempt full recovery: download k slices, decode, re-encode, extract target.
///
/// This is the fallback path when bandwidth-optimal Clay repair fails because
/// not enough helpers are available (< d required for repair).
pub async fn attempt_full_recovery<S: Store>(
    ctx: &NodeContext<S>,
    track_address: Pubkey,
    track_info: &TrackInfo,
    target_spool: SpoolIndex,
) -> Result<Vec<u8>, RecoveryError> {
    let group = group_for_spool(target_spool);
    let start = group_start(group);
    let target_position = (target_spool - start) as usize;

    let helpers = resolve_group_helpers(ctx, target_spool, ctx.config.insecure)?;

    let available: Vec<(usize, SpoolIndex, NodeClient)> = helpers
        .into_iter()
        .map(|h| (h.position, h.spool_idx, h.client))
        .collect();

    let track_id = track_address.to_string();
    let commitment_hash = track_info.commitment_root();

    let (computed_root, all_slices) =
        download_and_reencode(available, track_info, &track_id, DOWNLOAD_CONCURRENCY).await?;

    // Verify commitment: check re-encoded merkle root matches on-chain
    if computed_root != commitment_hash {
        return Err(RecoveryError::InconsistencyProof {
            track: track_address,
            computed_root,
        });
    }

    let target_slice = all_slices
        .into_iter()
        .nth(target_position)
        .ok_or_else(|| {
            RecoveryError::RepairFailed(format!(
                "target position {} not in encoded slices",
                target_position
            ))
        })?;

    debug!(
        track = %track_address,
        spool = target_spool,
        "full recovery produced target slice"
    );

    Ok(target_slice)
}

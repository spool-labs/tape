//! TrackSynchronizer — per-track recovery logic.
//!
//! Handles recovery of a single slice for a given track:
//! 1. Wait for recovery window (deferral)
//! 2. Check if slice already stored → return early
//! 3. Attempt bandwidth-optimal clay repair (via repair_single_slice)
//! 4. If InsufficientHelpers → attempt full recovery
//! 5. On error → sleep 30s and retry (infinite)
//! 6. Clear recovery deferral on completion

use std::sync::Arc;
use std::time::Duration;

use store::Store;
use tape_core::erasure::{group_for_spool, group_start};
use tape_core::spooler::SpoolIndex;
use solana_sdk::signer::Signer;
use tape_api::program::tapedrive::node_pda;
use tape_node_client::NodeClientBuilder;
use tape_store::ops::{CommitteeOps, SliceOps, TrackOps};
use tape_store::types::{Pubkey, TrackInfo};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::core::context::NodeContext;

use super::deferral::LiveUploadDeferral;
use super::error::RecoveryError;
use super::recovery_service::attempt_full_recovery;
use super::repair::repair_single_slice;

/// Delay between retry attempts when a track sync fails (30s fixed).
const RETRY_DELAY: Duration = Duration::from_secs(30);

/// Timeout for per-node metadata fetch requests.
const METADATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Fetch track metadata from committee peers when not available locally.
///
/// Iterates committee members sequentially, returning the first valid response.
/// Skips our own node and applies a per-request timeout.
async fn fetch_metadata_from_peers<S: Store>(
    ctx: &NodeContext<S>,
    track_address: Pubkey,
) -> Option<TrackInfo> {
    let epoch = ctx.control_plane.current_epoch();
    let committee = ctx.storage.store.get_committee(epoch).ok()??;
    let insecure = ctx.config.insecure;
    let track_id = track_address.to_string();

    let (our_node_address, _) = node_pda(ctx.keypair.pubkey());
    let our_node_address: Pubkey = our_node_address.into();

    for member in &committee {
        if member.node_address == our_node_address {
            continue;
        }

        let addr = match member.network_address.to_socket_addr() {
            Ok(a) => a,
            Err(_) => continue,
        };
        let client = match NodeClientBuilder::new()
            .accept_invalid_certs(insecure)
            .build(&addr.to_string())
        {
            Ok(c) => c,
            Err(_) => continue,
        };

        let result = tokio::time::timeout(
            METADATA_REQUEST_TIMEOUT,
            client.get_metadata(&track_id),
        )
        .await;

        let bytes = match result {
            Ok(Ok(b)) => b,
            Ok(Err(_)) | Err(_) => continue,
        };

        match wincode::deserialize::<TrackInfo>(&bytes) {
            Ok(info) if info.original_size > 0 => return Some(info),
            _ => continue,
        }
    }
    None
}

/// Recover a single slice for a track, with infinite retries.
///
/// This is the core recovery loop for one (track, spool) pair:
/// 1. Wait for recovery window (deferral)
/// 2. Check if already stored
/// 3. Attempt repair via Clay code helpers
/// 4. Fall back to full recovery if insufficient helpers
/// 5. Retry infinitely on failure with 30s fixed delay
/// 6. Clear deferral on completion
pub async fn recover_track_slice<S: Store>(
    ctx: Arc<NodeContext<S>>,
    our_spool: SpoolIndex,
    track_address: Pubkey,
    deferral: Arc<LiveUploadDeferral>,
    slice_semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
) {
    // Step 1: Wait for recovery window
    deferral.wait_for_recovery_window(&track_address).await;

    let mut attempt = 0u64;
    loop {
        if cancel.is_cancelled() {
            return;
        }

        // Check if already recovered (idempotent)
        match ctx.storage.store.has_slice(our_spool, track_address) {
            Ok(true) => {
                debug!(spool = our_spool, track = %track_address, "slice already stored");
                deferral.end_recovery(&track_address).await;
                return;
            }
            Ok(false) => {}
            Err(e) => {
                warn!(spool = our_spool, track = %track_address, error = %e, "storage check failed");
                // Retry on storage errors
            }
        }

        // Get track metadata — try local, then fan-out to peers
        let track_info = match ctx.storage.store.get_track(track_address) {
            Ok(Some(info)) => info,
            Ok(None) => {
                match fetch_metadata_from_peers(&ctx, track_address).await {
                    Some(info) => {
                        let _ = ctx.storage.store.put_track(track_address, info.clone());
                        info
                    }
                    None => {
                        warn!(track = %track_address, attempt, "metadata unavailable from peers");
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(RETRY_DELAY) => {}
                        }
                        attempt += 1;
                        continue;
                    }
                }
            }
            Err(e) => {
                warn!(track = %track_address, error = %e, "failed to get track info");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(RETRY_DELAY) => {}
                }
                attempt += 1;
                continue;
            }
        };

        // Attempt repair using shared repair_single_slice
        match repair_single_slice(&ctx, our_spool, track_address, &track_info).await {
            Ok(repaired_slice) => {
                if let Err(e) = ctx.storage.store.put_slice(our_spool, track_address, repaired_slice) {
                    warn!(spool = our_spool, track = %track_address, error = %e, "failed to store repaired slice");
                } else {
                    debug!(
                        spool = our_spool,
                        track = %track_address,
                        attempt,
                        "track slice recovered via repair"
                    );
                    deferral.end_recovery(&track_address).await;
                    return;
                }
            }
            Err(RecoveryError::NotEnoughHelpers { needed, available }) => {
                warn!(
                    spool = our_spool,
                    track = %track_address,
                    needed,
                    available,
                    attempt,
                    "insufficient helpers for repair, trying full recovery"
                );

                // Fall back to full recovery: download k slices, decode, re-encode
                let _permit = match slice_semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("slice semaphore closed");
                        return;
                    }
                };

                match attempt_full_recovery(&ctx, track_address, &track_info, our_spool).await {
                    Ok(slice_data) => {
                        let group = group_for_spool(our_spool);
                        let start = group_start(group);
                        let position = (our_spool - start) as usize;
                        if !track_info.commitment.is_empty()
                            && !track_info.verify_slice(position, &slice_data)
                        {
                            warn!(
                                spool = our_spool,
                                track = %track_address,
                                "full recovery slice failed leaf verification, retrying"
                            );
                        } else if let Err(e) =
                            ctx.storage
                                .store
                                .put_slice(our_spool, track_address, slice_data)
                        {
                            warn!(spool = our_spool, track = %track_address, error = %e, "failed to store recovered slice");
                        } else {
                            debug!(spool = our_spool, track = %track_address, "full recovery succeeded");
                            deferral.end_recovery(&track_address).await;
                            return;
                        }
                    }
                    Err(RecoveryError::InconsistencyProof { track }) => {
                        warn!(track = %track, "inconsistency detected, stubbed");
                        deferral.end_recovery(&track_address).await;
                        return;
                    }
                    Err(e) => {
                        warn!(
                            spool = our_spool,
                            track = %track_address,
                            error = %e,
                            "full recovery also failed"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    spool = our_spool,
                    track = %track_address,
                    attempt,
                    error = %e,
                    "repair attempt failed"
                );
            }
        }

        // Wait before retry — infinite loop with 30s fixed delay
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RETRY_DELAY) => {}
        }
        attempt += 1;
    }
}

//! TrackSynchronizer — per-track recovery logic.
//!
//! Handles recovery of a single slice for a given track:
//! 1. Wait for recovery window (deferral)
//! 2. Check if slice already stored → return early
//! 3. Attempt bandwidth-optimal clay repair (via repair_single_slice)
//! 4. If InsufficientHelpers → attempt full recovery
//! 5. On error → retry with exponential backoff (30s → 5min)
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
use crate::core::{Backoff, BackoffConfig};

use super::deferral::LiveUploadDeferral;
use super::error::RecoveryError;
use super::helpers::resolve_group_helpers;
use super::decode::attempt_full_recovery;
use super::repair::repair_single_slice;

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
    let committee = match ctx.storage.store.get_committee(epoch) {
        Ok(Some(c)) => c,
        Ok(None) => return None,
        Err(e) => {
            warn!(epoch = epoch.as_u64(), error = %e, "failed to read committee");
            return None;
        }
    };
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

/// Resolve track metadata: try local store, fall back to peer fan-out.
pub async fn resolve_track_metadata<S: Store>(
    ctx: &NodeContext<S>,
    track_address: Pubkey,
) -> Result<TrackInfo, RecoveryError> {
    match ctx.storage.store.get_track(track_address) {
        Ok(Some(info)) => Ok(info),
        Ok(None) => {
            match fetch_metadata_from_peers(ctx, track_address).await {
                Some(info) => {
                    if let Err(e) = ctx.storage.store.put_track(track_address, info.clone()) {
                        warn!(track = %track_address, error = %e, "failed to persist fetched track metadata");
                    }
                    Ok(info)
                }
                None => Err(RecoveryError::MetadataUnavailable),
            }
        }
        Err(e) => Err(e.into()),
    }
}

/// Verify a slice against the track commitment and store it.
pub fn verify_and_store_slice<S: Store>(
    store: &tape_store::TapeStore<S>,
    spool: SpoolIndex,
    track_address: Pubkey,
    track_info: &TrackInfo,
    position: usize,
    slice_data: Vec<u8>,
) -> Result<(), RecoveryError> {
    if !track_info.commitment.is_empty()
        && !track_info.verify_slice(position, &slice_data)
    {
        return Err(RecoveryError::RepairFailed(
            "slice failed leaf hash verification".into(),
        ));
    }
    store.put_slice(spool, track_address, slice_data)?;
    Ok(())
}

/// Recover a single slice for a track, with exponential backoff retries.
///
/// This is the core recovery loop for one (track, spool) pair:
/// 1. Wait for recovery window (deferral)
/// 2. Check if already stored
/// 3. Attempt repair via Clay code helpers
/// 4. Fall back to full recovery if insufficient helpers
/// 5. Retry on failure with exponential backoff (30s → 5min)
/// 6. Clear deferral on completion
pub async fn recover_track_slice<S: Store + 'static>(
    ctx: Arc<NodeContext<S>>,
    our_spool: SpoolIndex,
    track_address: Pubkey,
    deferral: Arc<LiveUploadDeferral>,
    slice_semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
) {
    // Step 1: Wait for recovery window
    deferral.wait_for_recovery_window(&track_address).await;

    let group = group_for_spool(our_spool);
    let start = group_start(group);
    let position = (our_spool - start) as usize;

    let mut backoff = Backoff::new(BackoffConfig::track_recovery());
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
            }
        }

        // Resolve track metadata
        let track_info = match resolve_track_metadata(&ctx, track_address).await {
            Ok(info) => info,
            Err(e) => {
                warn!(track = %track_address, attempt = backoff.attempt(), error = %e, "metadata unavailable");
                let delay = backoff.next_delay().unwrap_or(BackoffConfig::track_recovery().max_delay);
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                continue;
            }
        };

        // Resolve helpers for repair
        let insecure = ctx.config.insecure;
        let helpers = match resolve_group_helpers(&ctx, our_spool, insecure) {
            Ok(h) => h,
            Err(e) => {
                warn!(spool = our_spool, track = %track_address, error = %e, "failed to resolve helpers");
                let delay = backoff.next_delay().unwrap_or(BackoffConfig::track_recovery().max_delay);
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                continue;
            }
        };

        // Attempt repair
        match repair_single_slice(&ctx, our_spool, track_address, &track_info, &helpers).await {
            Ok(repaired_slice) => {
                match verify_and_store_slice(&ctx.storage.store, our_spool, track_address, &track_info, position, repaired_slice) {
                    Ok(()) => {
                        debug!(spool = our_spool, track = %track_address, attempt = backoff.attempt(), "track slice recovered via repair");
                        deferral.end_recovery(&track_address).await;
                        return;
                    }
                    Err(e) => {
                        warn!(spool = our_spool, track = %track_address, error = %e, "verify/store failed after repair");
                    }
                }
            }
            Err(RecoveryError::UnsupportedEncoding)
            | Err(RecoveryError::NotEnoughHelpers { .. }) => {
                warn!(
                    spool = our_spool,
                    track = %track_address,
                    attempt = backoff.attempt(),
                    "repair not possible, trying full recovery"
                );

                // Fall back to full recovery
                let _permit = match slice_semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("slice semaphore closed");
                        return;
                    }
                };

                match attempt_full_recovery(&ctx, track_address, &track_info, our_spool).await {
                    Ok(slice_data) => {
                        match verify_and_store_slice(&ctx.storage.store, our_spool, track_address, &track_info, position, slice_data) {
                            Ok(()) => {
                                debug!(spool = our_spool, track = %track_address, "full recovery succeeded");
                                deferral.end_recovery(&track_address).await;
                                return;
                            }
                            Err(e) => {
                                warn!(spool = our_spool, track = %track_address, error = %e, "verify/store failed after full recovery");
                            }
                        }
                    }
                    Err(RecoveryError::InconsistencyProof { track, computed_root }) => {
                        warn!(track = %track, "inconsistency detected, submitting proof");
                        if let Err(e) = crate::features::inconsistency::handle_inconsistency(
                            Arc::clone(&ctx), track, computed_root, &track_info
                        ).await {
                            warn!(track = %track, error = %e, "inconsistency proof failed");
                        }
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
                    attempt = backoff.attempt(),
                    error = %e,
                    "repair attempt failed"
                );
            }
        }

        // Wait before retry with exponential backoff
        let delay = backoff.next_delay().unwrap_or(BackoffConfig::track_recovery().max_delay);
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::erasure::group_start;
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info(commitment: Vec<tape_crypto::hash::Hash>) -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: 0,
            original_size: 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 2,
            encoding_params: 0,
            commitment,
        }
    }

    #[test]
    fn verify_and_store_succeeds_without_commitment() {
        let store = test_store();
        let spool: SpoolIndex = group_start(0) + 3;
        let track = Pubkey::new_unique();
        let info = make_track_info(vec![]);
        let data = vec![1, 2, 3, 4];

        let result = verify_and_store_slice(&store, spool, track, &info, 3, data.clone());
        assert!(result.is_ok());
        assert!(store.has_slice(spool, track).unwrap());
    }

    #[test]
    fn verify_and_store_succeeds_with_valid_commitment() {
        let store = test_store();
        let spool: SpoolIndex = group_start(0);
        let track = Pubkey::new_unique();
        let data = vec![10, 20, 30];
        let leaf_hash = tape_crypto::merkle::hash_leaf(&data);

        // commitment has one leaf at position 0
        let info = make_track_info(vec![leaf_hash]);

        let result = verify_and_store_slice(&store, spool, track, &info, 0, data);
        assert!(result.is_ok());
        assert!(store.has_slice(spool, track).unwrap());
    }

    #[test]
    fn verify_and_store_rejects_invalid_commitment() {
        let store = test_store();
        let spool: SpoolIndex = group_start(0);
        let track = Pubkey::new_unique();
        let data = vec![10, 20, 30];
        let wrong_hash = tape_crypto::merkle::hash_leaf(&[99, 99]);

        let info = make_track_info(vec![wrong_hash]);

        let result = verify_and_store_slice(&store, spool, track, &info, 0, data);
        assert!(result.is_err());
        assert!(!store.has_slice(spool, track).unwrap());
    }
}

//! Thread D - Erasure Recovery
//!
//! Handles recovery of slices that failed to sync from previous owners.
//! Uses erasure coding to reconstruct slices from the committee.
//!
//! Recovery flow:
//! 1. Poll recovery queue for pending items
//! 2. For items ready for retry (based on exponential backoff)
//! 3. Fetch DATA_SLICES from committee members via SDK
//! 4. Decode blob using Reed-Solomon
//! 5. Re-encode to get all slices
//! 6. Store the target slice

use std::sync::Arc;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_crypto::merkle::hash_leaf;
use tape_crypto::Hash;
use tape_sdk::communication::NodeCommunicationFactory;
use tape_sdk::downloader::ParallelDownloader;
use tape_sdk::error::DownloadError;
use tape_slicer::{BasicSlicer, Slicer, SliceIndex, SLICE_COUNT, MERKLE_HEIGHT};
use tape_store::ops::{is_ready_for_retry, Compression, RecoveryOps, SliceMeta};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::context::NodeContext;

/// Recovery polling interval.
const RECOVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum recovery attempts before giving up.
const MAX_RECOVERY_ATTEMPTS: u8 = 10;

/// Error type for recovery operations.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("download failed: {0}")]
    Download(#[from] DownloadError),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("no committee members available")]
    NoCommittee,

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("slice index out of range: {0}")]
    InvalidSliceIndex(u16),
}

/// Run the recovery worker loop.
///
/// This is Thread D's main entry point. It:
/// 1. Polls the recovery queue periodically
/// 2. Processes items that are ready for retry
/// 3. Fetches slices from committee and reconstructs
pub async fn run(
    ctx: Arc<NodeContext>,
    cancel: CancellationToken,
) -> Result<(), RecoveryError> {
    info!("Recovery thread starting");

    // Create a shared client factory for connection pooling across all recovery operations
    let factory = NodeCommunicationFactory::new();

    let mut interval = tokio::time::interval(RECOVERY_POLL_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Recovery thread shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = process_recovery_queue(&ctx, &factory).await {
                    error!(error = %e, "Error processing recovery queue");
                }
            }
        }
    }

    Ok(())
}

/// Process pending items in the recovery queue.
async fn process_recovery_queue(
    ctx: &NodeContext,
    factory: &NodeCommunicationFactory,
) -> Result<(), RecoveryError> {
    // Get all pending recoveries
    let pending = ctx
        .storage
        .store
        .get_all_recoveries()
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;

    if pending.is_empty() {
        return Ok(());
    }

    debug!(count = pending.len(), "Processing recovery queue");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Resolve committee addresses once per batch (cached by factory for HTTP connections)
    let addresses = get_committee_addresses(ctx).await?;
    if addresses.is_empty() {
        return Err(RecoveryError::NoCommittee);
    }

    let mut recovered = 0;
    let mut failed = 0;

    for (spool_idx, track_address, info) in pending {
        // Check if ready for retry based on exponential backoff
        if !is_ready_for_retry(&info, now) {
            continue;
        }

        // Skip if max attempts exceeded
        if info.attempts >= MAX_RECOVERY_ATTEMPTS {
            warn!(
                spool = spool_idx,
                track = ?track_address,
                attempts = info.attempts,
                "Max recovery attempts exceeded, giving up"
            );
            // Remove from queue to prevent infinite retries
            let _ = ctx.storage.store.dequeue_recovery(spool_idx, track_address);
            continue;
        }

        // Convert track address types
        let track_pubkey = Pubkey::from(track_address.to_bytes());

        // Attempt recovery
        match recover_slice(ctx, factory, &addresses, spool_idx, track_pubkey).await {
            Ok(()) => {
                info!(
                    spool = spool_idx,
                    track = %track_pubkey,
                    "Successfully recovered slice"
                );
                // Remove from queue
                ctx.storage
                    .store
                    .dequeue_recovery(spool_idx, track_address)
                    .map_err(|e| RecoveryError::Storage(e.to_string()))?;
                recovered += 1;
                ctx.metrics.slices_recovered_total.inc();
            }
            Err(e) => {
                warn!(
                    spool = spool_idx,
                    track = %track_pubkey,
                    error = %e,
                    attempt = info.attempts + 1,
                    "Recovery attempt failed"
                );
                // Update attempt counter for backoff
                let _ = ctx.storage.store.update_recovery_attempt(
                    spool_idx,
                    track_address,
                    now,
                );
                failed += 1;
            }
        }
    }

    if recovered > 0 || failed > 0 {
        info!(recovered, failed, "Recovery batch complete");
    }

    Ok(())
}

/// Recover a single slice via erasure decoding.
async fn recover_slice(
    ctx: &NodeContext,
    factory: &NodeCommunicationFactory,
    addresses: &[String],
    target_spool_idx: SpoolIndex,
    track_address: Pubkey,
) -> Result<(), RecoveryError> {
    debug!(
        spool = target_spool_idx,
        track = %track_address,
        "Attempting slice recovery"
    );

    // Use ParallelDownloader from SDK with client pooling via factory
    let downloader = ParallelDownloader::new(
        track_address.to_string(),
        addresses.to_vec(),
        factory.clone(),
    )
    .exclude_slice(target_spool_idx);

    // Fetch enough slices from the committee (excludes the target we're recovering)
    let slices = downloader.download_enough_slices().await?;

    // Decode the blob
    let mut slicer = BasicSlicer::default();
    let mut slice_array: [Option<tape_slicer::Slice>; SLICE_COUNT] =
        std::array::from_fn(|_| None);

    for (idx, data) in &slices {
        let slice_idx = SliceIndex::new(*idx as usize)
            .ok_or(RecoveryError::InvalidSliceIndex(*idx))?;
        slice_array[*idx as usize] = Some(tape_slicer::Slice::new(slice_idx, data.clone()));
    }

    let blob = slicer
        .decode(&slice_array)
        .map_err(|e| RecoveryError::Decode(e.to_string()))?;

    // Re-encode to get all slices
    let all_slices = slicer
        .encode(blob)
        .map_err(|e| RecoveryError::Encode(e.to_string()))?;

    // Find and store the target slice
    let target_slice = all_slices
        .iter()
        .find(|s| *s.index as u16 == target_spool_idx)
        .ok_or(RecoveryError::InvalidSliceIndex(target_spool_idx))?;

    // Compute leaf hash for the slice
    let leaf_hash = hash_leaf(&target_slice.data);

    // Create minimal metadata for recovered slice
    let meta = SliceMeta {
        len: target_slice.data.len() as u32,
        leaf_hash,
        merkle_proof: [Hash::default(); MERKLE_HEIGHT], // TODO: compute proper proof
        compression: Compression::None,
        received_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    };

    // Store the recovered slice
    ctx.storage
        .put_slice(target_spool_idx, track_address, target_slice.data.clone(), meta)
        .map_err(|e| RecoveryError::Storage(e.to_string()))?;

    Ok(())
}

/// Get network addresses for all committee members.
///
/// Resolves NodeId -> Node account -> NetworkAddress for each committee member.
/// Skips empty slots (NodeId 0) and logs warnings for resolution failures.
async fn get_committee_addresses(ctx: &NodeContext) -> Result<Vec<String>, RecoveryError> {
    let system = ctx.control_plane.get_system();
    let mut addresses = Vec::new();

    for member in system.committee.iter() {
        // Skip empty slots (NodeId 0 means unoccupied)
        if member.id == NodeId(0) {
            continue;
        }

        // Look up node to get network address
        match ctx.rpc.get_node_by_id(member.id).await {
            Ok((_pubkey, node)) => {
                match node.metadata.network_address.to_socket_addr() {
                    Ok(addr) => {
                        addresses.push(format!("http://{}", addr));
                    }
                    Err(e) => {
                        debug!(node_id = member.id.as_u64(), error = %e, "Invalid network address");
                    }
                }
            }
            Err(e) => {
                debug!(node_id = member.id.as_u64(), error = %e, "Failed to lookup node");
            }
        }
    }

    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_constants() {
        // Ensure we're using correct erasure coding parameters
        assert_eq!(SLICE_COUNT, 1024);
        assert_eq!(DATA_SLICES, 683);
        assert!(DATA_SLICES < SLICE_COUNT);
    }

    #[test]
    fn test_max_attempts() {
        assert!(MAX_RECOVERY_ATTEMPTS > 0);
        assert!(MAX_RECOVERY_ATTEMPTS <= 20); // Sanity check
    }
}

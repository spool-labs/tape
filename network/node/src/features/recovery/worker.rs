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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use tape_core::spooler::SpoolIndex;
use tape_core::prelude::EncodingType;
use tape_core::types::NodeId;
use tape_crypto::merkle::hash_leaf;
use tape_crypto::Hash;
use tape_sdk::communication::NodeCommunicationFactory;
use tape_sdk::downloader::ParallelDownloader;
use tape_sdk::error::DownloadError;
use tape_slicer::{
    build_blob_merkle_tree, BasicSlicer, StripedSlicer, RotatedSlicer,
    Slicer, SliceIndex, Slice, Blob, SLICE_COUNT, MERKLE_HEIGHT,
};
use tape_store::ops::{is_ready_for_retry, Compression, RecoveryOps, SliceMeta};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::core::context::NodeContext;

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
    // Skip recovery during catch-up - committee data may be stale
    if ctx.control_plane.is_catching_up() {
        debug!("Skipping recovery during catch-up");
        return Ok(());
    }

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

    // Build slice → address mapping once per batch using spool assignment
    let slice_to_address = get_slice_address_mapping(ctx).await?;
    if slice_to_address.is_empty() {
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
        match recover_slice(ctx, factory, &slice_to_address, spool_idx, track_pubkey).await {
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

/// Decode and re-encode slices using the appropriate slicer for the encoding type.
fn decode_and_encode(
    encoding_type: EncodingType,
    slice_array: &[Option<Slice>; SLICE_COUNT],
) -> Result<(Blob, [Slice; SLICE_COUNT]), RecoveryError> {
    match encoding_type {
        EncodingType::Basic => {
            let mut slicer = BasicSlicer::default();
            let blob = slicer
                .decode(slice_array)
                .map_err(|e| RecoveryError::Decode(e.to_string()))?;
            let all_slices = slicer
                .encode(blob.clone())
                .map_err(|e| RecoveryError::Encode(e.to_string()))?;
            Ok((blob, all_slices))
        }
        EncodingType::Striped => {
            let mut slicer = StripedSlicer::default();
            let blob = slicer
                .decode(slice_array)
                .map_err(|e| RecoveryError::Decode(e.to_string()))?;
            let all_slices = slicer
                .encode(blob.clone())
                .map_err(|e| RecoveryError::Encode(e.to_string()))?;
            Ok((blob, all_slices))
        }
        EncodingType::Rotated | EncodingType::Unknown => {
            let mut slicer = RotatedSlicer::default();
            let blob = slicer
                .decode(slice_array)
                .map_err(|e| RecoveryError::Decode(e.to_string()))?;
            let all_slices = slicer
                .encode(blob.clone())
                .map_err(|e| RecoveryError::Encode(e.to_string()))?;
            Ok((blob, all_slices))
        }
    }
}

/// Recover a single slice via erasure decoding.
async fn recover_slice(
    ctx: &NodeContext,
    factory: &NodeCommunicationFactory,
    slice_to_address: &HashMap<SpoolIndex, String>,
    target_spool_idx: SpoolIndex,
    track_address: Pubkey,
) -> Result<(), RecoveryError> {
    debug!(
        spool = target_spool_idx,
        track = %track_address,
        "Attempting slice recovery"
    );

    // TODO: Fetch encoding type from on-chain track data:
    //   let track = ctx.rpc.get_track_by_address(&track_address).await?;
    //   let encoding_type = track.data.encoding_type().unwrap_or(EncodingType::Rotated);
    let encoding_type = EncodingType::Rotated;

    // Use ParallelDownloader from SDK with client pooling via factory
    let downloader = ParallelDownloader::new(
        track_address.to_string(),
        slice_to_address.clone(),
        factory.clone(),
    )
    .exclude_slice(target_spool_idx);

    // Fetch enough slices from the committee (excludes the target we're recovering)
    let slices = downloader.download_enough_slices().await?;

    // Build slice array from downloaded slices
    let mut slice_array: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);

    for (idx, data) in &slices {
        let slice_idx = SliceIndex::new(*idx as usize)
            .ok_or(RecoveryError::InvalidSliceIndex(*idx))?;
        slice_array[*idx as usize] = Some(Slice::new(slice_idx, data.clone()));
    }

    // Decode and re-encode using the appropriate slicer
    let (_blob, all_slices) = decode_and_encode(encoding_type, &slice_array)?;

    // Build merkle tree from all slices for proof generation
    let merkle_tree = build_blob_merkle_tree(&all_slices);

    // Collect leaves for proof creation (the raw slice data)
    let leaves: Vec<&[u8]> = all_slices.iter().map(|s| s.data.as_slice()).collect();

    // Create merkle proof for the target slice
    let proof_vec = merkle_tree
        .create_proof(&leaves, target_spool_idx as usize)
        .map_err(|e| RecoveryError::Decode(format!("merkle proof error: {:?}", e)))?;

    // Convert Vec<Hash> to [Hash; MERKLE_HEIGHT]
    let merkle_proof: [Hash; MERKLE_HEIGHT] = proof_vec
        .try_into()
        .map_err(|_| RecoveryError::Decode("invalid merkle proof length".to_string()))?;

    // Get the target slice
    let target_slice = &all_slices[target_spool_idx as usize];

    // Compute leaf hash for the slice
    let leaf_hash = hash_leaf(&target_slice.data);

    // Create metadata for recovered slice with proper merkle proof
    let meta = SliceMeta {
        len: target_slice.data.len() as u32,
        leaf_hash,
        merkle_proof,
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
/// Build slice_index → address mapping using spool assignment for proper routing.
async fn get_slice_address_mapping(ctx: &NodeContext) -> Result<HashMap<SpoolIndex, String>, RecoveryError> {
    let system = ctx.control_plane.get_system();
    let mut slice_to_address: HashMap<SpoolIndex, String> = HashMap::new();

    // Build member_index → address lookup
    let mut member_addresses: Vec<Option<String>> = vec![None; 128];

    for (member_idx, member) in system.committee.iter().enumerate() {
        // Skip empty slots (NodeId 0 means unoccupied)
        if member.id == NodeId(0) {
            continue;
        }

        // Look up node to get network address
        match ctx.rpc.get_node_by_id(member.id).await {
            Ok((_pubkey, node)) => {
                match node.metadata.network_address.to_socket_addr() {
                    Ok(addr) => {
                        member_addresses[member_idx] = Some(format!("http://{}", addr));
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

    // Build slice_index → address mapping using spool assignment
    for slice_idx in 0..SLICE_COUNT as SpoolIndex {
        let member_idx = system.spools.0[slice_idx as usize] as usize;
        if let Some(ref addr) = member_addresses.get(member_idx).and_then(|a| a.as_ref()) {
            slice_to_address.insert(slice_idx, addr.to_string());
        }
    }

    Ok(slice_to_address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::DATA_SLICES;

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

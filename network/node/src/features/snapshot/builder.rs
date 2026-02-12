//! Snapshot builder — serialize SnapshotLog, two-level encode, store slices.
//!
//! At each epoch boundary, the builder:
//! 1. Reads all events from the event log for the completed epoch
//! 2. Builds a SnapshotLog
//! 3. Outer-RS-encodes into 50 chunks (one per spool group)
//! 4. Inner-Clay-encodes each chunk into 20 slices
//! 5. Stores slices for owned spools
//! 6. Returns commitments needed for on-chain certification

use std::sync::Arc;

use store::Store;
use tape_core::erasure::{group_for_spool, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::{SnapshotEntry, SnapshotLog};
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_slicer::{blob_merkle_root, ClayCoder, ClayParams, ErasureCoder, OuterCoder, Slicer, DEFAULT_K_OUTER};
use tape_api::program::tapedrive::snapshot_pda;
use tape_store::ops::{EventLogOps, MetaOps, SliceOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_crypto::Hash;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::core::context::NodeContext;

/// Result of building an epoch snapshot.
pub struct SnapshotBuildResult {
    /// Merkle root commitment for each of the 50 chunks.
    pub commitments: Vec<Hash>,
    /// Byte size of each outer chunk (before inner encoding).
    pub chunk_sizes: Vec<usize>,
    /// The 50 sets of 20 inner slices (only populated for owned spool groups).
    /// Index: [group_index] -> Some(slices) if we own spools in that group.
    pub group_slices: Vec<Option<Vec<Vec<u8>>>>,
}

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("store error: {0}")]
    Store(#[from] tape_store::error::TapeStoreError),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("no events for epoch {0}")]
    NoEvents(EpochNumber),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("cancelled")]
    Cancelled,
}

/// Build the epoch snapshot: read events, serialize, two-level encode, store slices.
pub async fn build_epoch_snapshot<S: Store>(
    ctx: &Arc<NodeContext<S>>,
    epoch: EpochNumber,
) -> Result<SnapshotBuildResult, SnapshotError> {
    // 1. Read events from the event log
    let entries: Vec<SnapshotEntry> = ctx.storage.store.get_epoch_events(epoch)?;

    if entries.is_empty() {
        return Err(SnapshotError::NoEvents(epoch));
    }

    let start_slot = entries.first().map(|e| e.slot).unwrap_or_default();
    let end_slot = entries.last().map(|e| e.slot).unwrap_or_default();

    info!(
        epoch = epoch.as_u64(),
        entries = entries.len(),
        start_slot = start_slot.as_u64(),
        end_slot = end_slot.as_u64(),
        "Building snapshot"
    );

    // 2. Build SnapshotLog
    let log = SnapshotLog {
        version: 1,
        epoch,
        start_slot,
        end_slot,
        entries,
    };

    // 3. Serialize
    let serialized = wincode::serialize(&log)
        .map_err(|e| SnapshotError::Serialization(e.to_string()))?;

    info!(
        epoch = epoch.as_u64(),
        bytes = serialized.len(),
        "Snapshot serialized"
    );

    // 4. Outer-RS-encode → 50 chunks
    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let chunks = outer
        .encode(&serialized)
        .map_err(|e| SnapshotError::Encode(format!("outer encode: {:?}", e)))?;

    assert_eq!(chunks.len(), SPOOL_GROUP_COUNT);

    // 5. Inner-Clay-encode each chunk → 20 slices, compute commitments
    let our_spools = ctx.control_plane.get_our_spools();
    let mut commitments = Vec::with_capacity(SPOOL_GROUP_COUNT);
    let mut chunk_sizes = Vec::with_capacity(SPOOL_GROUP_COUNT);
    let mut group_slices: Vec<Option<Vec<Vec<u8>>>> = vec![None; SPOOL_GROUP_COUNT];

    for (group_idx, chunk) in chunks.iter().enumerate() {
        chunk_sizes.push(chunk.len());

        let clay_params = ClayParams::default();
        let coder = ClayCoder::from_params(clay_params);
        let mut slicer = Slicer::new(coder);

        let slices = slicer
            .encode(chunk)
            .map_err(|e| SnapshotError::Encode(format!("inner encode group {}: {:?}", group_idx, e)))?;

        assert_eq!(slices.len(), SPOOL_GROUP_SIZE);

        // Compute commitment (merkle root over inner slices)
        let commitment = blob_merkle_root(&slices);
        commitments.push(commitment);

        // Check if we own any spools in this group
        let we_own_group = our_spools.iter().any(|&s| group_for_spool(s) == group_idx as u64);

        // Persist commitment for this chunk (needed by sign endpoint)
        ctx.storage.store.set_snapshot_commitment(epoch, ChunkIndex(group_idx as u64), commitment)?;

        if we_own_group {
            // Derive a deterministic track address for snapshot slice storage
            let (pda, _) = snapshot_pda(epoch, commitment);
            let track_address: StorePubkey = pda.into();

            // Store slices for owned spools
            for &spool in &our_spools {
                if group_for_spool(spool) == group_idx as u64 {
                    let slice_idx = (spool as usize) % SPOOL_GROUP_SIZE;
                    if let Some(slice_data) = slices.get(slice_idx) {
                        ctx.storage.store.put_slice(spool, track_address, slice_data.clone())?;
                    }
                }
            }
            group_slices[group_idx] = Some(slices);
        }
    }

    info!(
        epoch = epoch.as_u64(),
        chunks = SPOOL_GROUP_COUNT,
        "Snapshot two-level encoded"
    );

    Ok(SnapshotBuildResult {
        commitments,
        chunk_sizes,
        group_slices,
    })
}

/// Build and certify a snapshot for the given epoch.
///
/// Called after AdvanceEpoch when the node is in the committee.
pub async fn build_and_certify<S: Store>(
    ctx: Arc<NodeContext<S>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> Result<(), SnapshotError> {
    let result = build_epoch_snapshot(&ctx, epoch).await?;

    info!(
        epoch = epoch.as_u64(),
        "Snapshot built, starting certification"
    );

    // Certify all 50 snapshot tracks
    if let Err(e) = super::certifier::certify_snapshot_tracks(&ctx, epoch, &result, &cancel).await {
        warn!(
            epoch = epoch.as_u64(),
            error = %e,
            "Snapshot certification failed"
        );
        return Err(e);
    }

    // GC: delete event log after successful certification
    ctx.storage.store.delete_epoch_events(epoch)?;

    info!(
        epoch = epoch.as_u64(),
        "Snapshot build and certify complete"
    );

    Ok(())
}

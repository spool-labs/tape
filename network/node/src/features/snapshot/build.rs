//! Local snapshot artifact building
//!
//! Reads the replay event log for a sealed epoch, serializes it into a
//! deterministic SnapshotLog, erasure-codes it across all spool groups
//! (outer RS + inner Clay), and persists the resulting slices and metadata
//! to the snapshot store columns.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::info::{
    SnapshotGroupInfo, SnapshotGroupStatus, SnapshotInfo, SnapshotStatus,
};
use tape_core::snapshot::types::SnapshotLog;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkIndex, EpochNumber, SlotNumber, StorageUnits, StripeCount};
use tape_crypto::hash::Hash;
use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
use tape_protocol::Api;
use tape_slicer::outer::{OuterCoder, DEFAULT_K_OUTER};
use tape_slicer::{num_stripes, ErasureCoder, Slicer};
use tape_store::ops::{EventLogOps, SnapshotOps};
use tracing::debug;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Build local snapshot artifacts for a sealed epoch
///
/// Reads the event log, serializes it, erasure-codes across all spool
/// groups, and persists slices and metadata to the snapshot store.
pub async fn build_snapshot_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    // Skip if already past Built
    if let Some(existing) = context
        .store
        .get_snapshot_info(epoch)
        .map_err(|e| NodeError::Store(format!("get_snapshot_info({epoch}): {e}")))?
    {
        match existing.status {
            SnapshotStatus::Initialized
            | SnapshotStatus::PartiallyCertified
            | SnapshotStatus::Finalized => {
                debug!(epoch = epoch.0, status = ?existing.status, "snapshot already past Built");
                return Ok(());
            }
            SnapshotStatus::Pending | SnapshotStatus::Built => {}
        }
    }

    let entries = context
        .store
        .get_epoch_events(epoch)
        .map_err(|e| NodeError::Store(format!("get_epoch_events({epoch}): {e}")))?;

    let start_slot = entries.first().map(|e| e.slot).unwrap_or(SlotNumber(0));
    let end_slot = entries.last().map(|e| e.slot).unwrap_or(SlotNumber(0));

    let log = SnapshotLog {
        epoch,
        start_slot,
        end_slot,
        entries,
    };

    let serialized = log
        .to_bytes()
        .map_err(|e| NodeError::Store(format!("snapshot to_bytes({epoch}): {e}")))?;

    let mut outer = OuterCoder::new(DEFAULT_K_OUTER);
    let chunks = outer
        .encode(&serialized)
        .map_err(|e| NodeError::Store(format!("outer encode({epoch}): {e}")))?;

    let mut built_groups = Vec::with_capacity(chunks.len());
    for (group_index, chunk) in chunks.iter().enumerate() {
        let group = SpoolGroup(group_index as u64);
        let group_info = encode_group(context.store.as_ref(), epoch, group, chunk)?;
        built_groups.push((group, group_info));
    }

    let mut snapshot = context
        .store
        .get_snapshot_info(epoch)
        .map_err(|e| NodeError::Store(format!("get_snapshot_info({epoch}): {e}")))?
        .unwrap_or_else(|| SnapshotInfo::new(SnapshotStatus::Pending));

    for (group, built_group) in built_groups {
        let existing_group = snapshot.group(group);
        let certified = existing_group.status == SnapshotGroupStatus::CertifiedOnChain;
        let track_number = existing_group.track_number;

        *snapshot.group_mut(group) = built_group;
        if certified {
            let group_info = snapshot.group_mut(group);
            group_info.status = SnapshotGroupStatus::CertifiedOnChain;
            group_info.track_number = track_number;
        }
    }

    match snapshot.status {
        SnapshotStatus::Pending | SnapshotStatus::Built => {
            snapshot.status = SnapshotStatus::Built;
        }
        SnapshotStatus::Initialized
        | SnapshotStatus::PartiallyCertified
        | SnapshotStatus::Finalized => {
            debug!(epoch = epoch.0, status = ?snapshot.status, "status advanced during build");
        }
    }

    context
        .store
        .put_snapshot_info(epoch, snapshot)
        .map_err(|e| NodeError::Store(format!("put_snapshot_info({epoch}): {e}")))?;

    debug!(
        node_id = context.node_id().0,
        epoch = epoch.0,
        groups = SPOOL_GROUP_COUNT,
        serialized_bytes = serialized.len(),
        "snapshot epoch built"
    );

    Ok(())
}

/// Encode one outer chunk into slices and persist artifacts for a single spool group
fn encode_group<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: &[u8],
) -> Result<SnapshotGroupInfo, NodeError> {
    let mut slicer = Slicer::clay_default();
    slicer.set_chunk_index(ChunkIndex(group.0));

    let slices = slicer
        .encode(chunk)
        .map_err(|e| NodeError::Store(format!("inner encode({epoch}, group {group}): {e}")))?;

    let leaves: [Hash; SPOOL_GROUP_SIZE] = core::array::from_fn(|i| hash_leaf(&slices[i]));
    let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

    let stripe_size = slicer.stripe_size();
    let stripe_count = num_stripes(chunk.len(), stripe_size);

    let blob = BlobInfo {
        size: StorageUnits::from_bytes(chunk.len() as u64),
        commitment,
        profile: slicer.profile(),
        stripe_size: StorageUnits::from_bytes(stripe_size as u64),
        stripe_count: StripeCount(stripe_count as u64),
        leaves,
    };

    for (spool_index, slice) in slices.into_iter().enumerate() {
        store
            .put_snapshot_slice(epoch, group, spool_index as u16, slice)
            .map_err(|e| NodeError::Store(format!(
                "put_snapshot_slice({epoch}, group {group}, spool {spool_index}): {e}"
            )))?;
    }

    Ok(SnapshotGroupInfo {
        status: SnapshotGroupStatus::Built,
        blob,
        track_number: None,
    })
}

#[cfg(test)]
mod tests {
    use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::info::{SnapshotGroupStatus, SnapshotInfo, SnapshotStatus};
    use tape_core::snapshot::types::ReplayableEvent;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_crypto::hash::Hash;
    use tape_store::ops::{EventLogOps, SnapshotOps};

    use super::build_snapshot_epoch;
    use crate::context::test_utils::test_context;

    // empty event log produces valid artifacts for all groups
    #[tokio::test]
    async fn empty_epoch() {
        let ctx = test_context();
        let epoch = EpochNumber(5);

        ctx.store
            .put_snapshot_info(epoch, SnapshotInfo::new(SnapshotStatus::Pending))
            .expect("put_snapshot_info");

        build_snapshot_epoch(&ctx, epoch).await.expect("build");

        let info = ctx.store.get_snapshot_info(epoch).expect("get").expect("exists");
        assert_eq!(info.status, SnapshotStatus::Built);

        for g in 0..SPOOL_GROUP_COUNT as u64 {
            let group = info.group(SpoolGroup(g));
            assert_eq!(group.status, SnapshotGroupStatus::Built);
            assert_ne!(group.blob.commitment, Hash::default());

            for s in 0..SPOOL_GROUP_SIZE as u16 {
                assert!(
                    ctx.store
                        .get_snapshot_slice(epoch, SpoolGroup(g), s)
                        .expect("get")
                        .is_some(),
                    "missing slice group={g} spool={s}"
                );
            }
        }
    }

    // populated log produces artifacts with distinct per-group commitments
    #[tokio::test]
    async fn with_events() {
        let ctx = test_context();
        let epoch = EpochNumber(3);

        ctx.store
            .append_event(
                epoch,
                SlotNumber(100),
                &ReplayableEvent::AdvanceEpoch {
                    old_epoch: EpochNumber(2),
                    new_epoch: EpochNumber(3),
                },
            )
            .expect("append");

        ctx.store
            .put_snapshot_info(epoch, SnapshotInfo::new(SnapshotStatus::Pending))
            .expect("put_snapshot_info");

        build_snapshot_epoch(&ctx, epoch).await.expect("build");

        let info = ctx.store.get_snapshot_info(epoch).expect("get").expect("exists");
        assert_eq!(info.status, SnapshotStatus::Built);

        let c0 = info.group(SpoolGroup(0)).blob.commitment;
        let c1 = info.group(SpoolGroup(1)).blob.commitment;

        // chunk_index differentiates commitments across groups
        assert_ne!(c0, c1);
    }

    // build skips epochs already past Built status
    #[tokio::test]
    async fn skip_initialized() {
        let ctx = test_context();
        let epoch = EpochNumber(5);

        ctx.store
            .put_snapshot_info(epoch, SnapshotInfo::new(SnapshotStatus::Initialized))
            .expect("put_snapshot_info");

        build_snapshot_epoch(&ctx, epoch).await.expect("build");

        let info = ctx.store.get_snapshot_info(epoch).expect("get").expect("exists");
        assert_eq!(info.status, SnapshotStatus::Initialized);
        assert_eq!(info.group(SpoolGroup(0)).status, SnapshotGroupStatus::Missing);
    }

    // rebuilding produces identical commitments
    #[tokio::test]
    async fn idempotent() {
        let ctx = test_context();
        let epoch = EpochNumber(5);

        ctx.store
            .put_snapshot_info(epoch, SnapshotInfo::new(SnapshotStatus::Pending))
            .expect("put_snapshot_info");

        build_snapshot_epoch(&ctx, epoch).await.expect("first build");

        let first = ctx
            .store
            .get_snapshot_info(epoch)
            .expect("get")
            .expect("exists")
            .group(SpoolGroup(0))
            .blob
            .commitment;

        let mut snapshot = ctx.store.get_snapshot_info(epoch).expect("get").expect("exists");
        snapshot.status = SnapshotStatus::Pending;
        ctx.store
            .put_snapshot_info(epoch, snapshot)
            .expect("put_snapshot_info");

        build_snapshot_epoch(&ctx, epoch).await.expect("second build");

        let second = ctx
            .store
            .get_snapshot_info(epoch)
            .expect("get")
            .expect("exists")
            .group(SpoolGroup(0))
            .blob
            .commitment;

        assert_eq!(first, second);
    }
}

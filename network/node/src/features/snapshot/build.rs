//! Build the snapshot for one epoch: outer-RS encode the event log into
//! per-group symbols, then Clay-encode each symbol into slices.
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio_util::sync::CancellationToken;
use tape_core::erasure::{SLICE_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::chunk::{pack_segment, SnapshotChunkPayload, SEGMENT_HEADER_SIZE};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount};
use tape_crypto::hash::Hash;
use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
use tape_protocol::Api;
use tape_slicer::{
    num_stripes, ErasureCoder, OuterCoder, Slicer, MAX_CHUNK_BYTES, SNAPSHOT_K_OUTER,
};
use tape_store::ops::{EventLogOps, SnapshotOps};
use tape_store::types::SnapshotArtifact;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// Maximum compressed bytes carried by a single outer RS round's segment.
pub const MAX_SEGMENT_BYTES: usize = SNAPSHOT_K_OUTER * MAX_CHUNK_BYTES - SEGMENT_HEADER_SIZE;

/// One encoded snapshot chunk, in memory between build and persistence.
#[derive(Debug, Clone)]
pub struct BuiltChunk {
    pub group: SpoolGroup,
    pub chunk: ChunkNumber,
    pub blob: BlobInfo,
    pub slices: [Vec<u8>; SPOOL_GROUP_SIZE],
}

#[derive(Debug, Default)]
pub struct BuildSummary {
    pub groups: usize,
    pub chunks: usize,
}

/// Build the snapshot for one epoch and persist this node's local group artifacts.
pub async fn build_snapshot<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<BuildSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_ctx = ctx.clone();
    let task = tokio::task::spawn_blocking(
        move || build_snapshot_blocking(&owned_ctx, epoch)
    );

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("build_snapshot task join: {e}")))?,
        _ = cancel.cancelled() => 
            Err(NodeError::Store(format!( "build_snapshot({epoch}): cancelled"))),
    }
}

fn build_snapshot_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber, // <- this is the previous epoch
) -> Result<BuildSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let me = ctx.node_id();

    let Some((member_index, _)) = state.find_member(me) else {
        return Ok(BuildSummary::default());
    };

    let our_spools = state.member_spools(member_index);
    if our_spools.is_empty() {
        return Ok(BuildSummary::default());
    }

    let entries = ctx.store
        .get_epoch_events(epoch)
        .map_err(store_err("get_epoch_events"))?;

    let start_slot = entries.first().map(|e| e.slot).unwrap_or(SlotNumber(0));
    let end_slot = entries.last().map(|e| e.slot).unwrap_or(SlotNumber(0));

    let snapshot_log = SnapshotLog { epoch, start_slot, end_slot, entries };
    let serialized = snapshot_log
        .to_bytes()
        .map_err(|e| NodeError::Store(format!("snapshot log serialize({epoch}): {e}")))?;

    let compressed = lz4_flex::compress_prepend_size(&serialized);
    let chunk_total = compressed.len().div_ceil(MAX_SEGMENT_BYTES).max(1);
    let chunk_size = compressed.len().div_ceil(chunk_total).max(1);

    let mut outer = OuterCoder::new(SNAPSHOT_K_OUTER);
    let mut chunk_count = 0usize;

    for chunk_index in 0..chunk_total {
        let start = chunk_index * chunk_size;
        let end = start.saturating_add(chunk_size).min(compressed.len());

        let packed = pack_segment(&compressed[start..end]);
        let symbols = outer.encode(&packed).map_err(|e| {
            NodeError::Store(format!("outer encode epoch={epoch} segment={chunk_index}: {e}"))
        })?;

        let chunk = ChunkNumber(chunk_index as u64);

        for &spool_index in &our_spools {
            let group = SpoolGroup::of(spool_index);
            let bitmap_index = (spool_index - group.base()) as u16;
            let built = encode_chunk(epoch, group, chunk, &symbols[group.0 as usize])?;

            let artifact = SnapshotArtifact {
                spool_index,
                blob: built.blob,
                slice: built.slices[bitmap_index as usize].clone(),
            };

            ctx.store
                .put_snapshot_artifact(epoch, group, chunk, &artifact)
                .map_err(store_err("put_snapshot_artifact"))?;

            chunk_count += 1;
        }
    }

    Ok(BuildSummary {
        groups: our_spools.len(),
        chunks: chunk_count,
    })
}

/// Clay-encode one outer symbol into its 20 spool-member slices and package
/// the result with derived `BlobInfo`.
pub(crate) fn encode_chunk(
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    symbol: &[u8],
) -> Result<BuiltChunk, NodeError> {
    let payload = SnapshotChunkPayload {
        chunk,
        data: symbol.to_vec(),
    };
    let packed = payload.pack();

    let mut slicer = Slicer::clay_default();
    let slices = slicer.encode(&packed).map_err(|e| {
        NodeError::Store(format!(
            "clay encode epoch={epoch} group={group} chunk={chunk}: {e}"
        ))
    })?;

    let slices: [Vec<u8>; SPOOL_GROUP_SIZE] = slices.try_into().map_err(|v: Vec<Vec<u8>>| {
        NodeError::Store(format!(
            "clay encode produced {} slices, expected {}",
            v.len(),
            SPOOL_GROUP_SIZE,
        ))
    })?;

    let leaves: [Hash; SPOOL_GROUP_SIZE] = core::array::from_fn(|i| hash_leaf(&slices[i]));
    let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

    let stripe_size = slicer.stripe_size();
    let stripe_count = num_stripes(symbol.len(), stripe_size);

    let blob = BlobInfo {
        size: StorageUnits::from_bytes(symbol.len() as u64),
        commitment,
        profile: slicer.profile(),
        stripe_size: StorageUnits::from_bytes(stripe_size as u64),
        stripe_count: StripeCount(stripe_count as u64),
        leaves,
    };

    Ok(BuiltChunk {
        group,
        chunk,
        blob,
        slices,
    })
}

fn store_err<E: std::fmt::Display>(op: &'static str) -> impl FnOnce(E) -> NodeError {
    move |e| NodeError::Store(format!("{op}: {e}"))
}

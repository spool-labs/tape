//! Build the snapshot for one epoch: outer-RS encode the event log into
//! per-group symbols, then Clay-encode each symbol into slices.
use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tokio_util::sync::CancellationToken;
use tape_core::cert::{SnapshotSignMessage, SnapshotWriteMessage};
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
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
use tape_store::types::{SnapshotArtifact, SnapshotFinalizeVote, SnapshotWriteVote};
use tape_store::TapeStore;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::snapshot::utils::{bitmap_index_in_group, local_groups};

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

/// Encode every snapshot chunk for `epoch` that belongs to a group in wanted_groups.
pub fn build_snapshot_chunks<Db: Store>(
    store: &TapeStore<Db>,
    epoch: EpochNumber,
    wanted_groups: &HashSet<SpoolGroup>,
) -> Result<Vec<BuiltChunk>, NodeError> {
    if wanted_groups.is_empty() {
        return Ok(Vec::new());
    }

    let entries = store
        .get_epoch_events(epoch)
        .map_err(store_err("get_epoch_events"))?;

    let start_slot = entries
        .first()
        .map(|e| e.slot)
        .unwrap_or(SlotNumber(0));
    let end_slot = entries
        .last()
        .map(|e| e.slot)
        .unwrap_or(SlotNumber(0));

    let snapshot_log = SnapshotLog {
        epoch,
        start_slot,
        end_slot,
        entries,
    };

    let serialized = snapshot_log
        .to_bytes()
        .map_err(|e| NodeError::Store(format!("snapshot log serialize({epoch}): {e}")))?;

    let compressed = lz4_flex::compress_prepend_size(&serialized);

    let segment_count = compressed.len().div_ceil(MAX_SEGMENT_BYTES).max(1);
    let segment_size = compressed.len().div_ceil(segment_count).max(1);

    let mut outer = OuterCoder::new(SNAPSHOT_K_OUTER);
    let mut chunks = Vec::with_capacity(segment_count * wanted_groups.len());

    for segment_idx in 0..segment_count {
        let start = segment_idx * segment_size;
        let end = start.saturating_add(segment_size).min(compressed.len());
        let segment = &compressed[start..end];
        let packed = pack_segment(segment);

        let symbols = outer.encode(&packed).map_err(|e| {
            NodeError::Store(format!(
                "outer encode epoch={epoch} segment={segment_idx}: {e}"
            ))
        })?;

        let chunk = ChunkNumber(segment_idx as u64);
        for (group_index, symbol) in symbols.into_iter().enumerate() {
            let group = SpoolGroup(group_index as u64);
            if !wanted_groups.contains(&group) {
                continue;
            }
            chunks.push(encode_chunk(epoch, group, chunk, &symbol)?);
        }
    }

    Ok(chunks)
}

/// Build the snapshot for one epoch and persist this node's local group
/// artifacts and self-produced partial signatures.
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
        _ = cancel.cancelled() => Err(NodeError::Store(format!(
            "build_snapshot({epoch}): cancelled"
        ))),
    }
}

fn build_snapshot_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<BuildSummary, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let my_node_id = ctx.node_id();
    let my_groups: HashSet<SpoolGroup> = local_groups(&state, my_node_id)
        .into_iter()
        .collect();

    if my_groups.is_empty() {
        return Ok(BuildSummary::default());
    }

    let chunks = build_snapshot_chunks(ctx.store.as_ref(), epoch, &my_groups)?;

    let mut chunk_count = 0usize;
    for chunk in &chunks {
        // We filtered to local groups above; this lookup cannot fail.
        let bitmap_index = bitmap_index_in_group(&state, chunk.group, my_node_id)
            .expect("local group must have a local bitmap index");

        let artifact = SnapshotArtifact {
            blob: chunk.blob,
            local_slice: chunk.slices[bitmap_index as usize].clone(),
        };

        ctx.store
            .put_snapshot_artifact(epoch, chunk.group, chunk.chunk, &artifact)
            .map_err(store_err("put_snapshot_artifact"))?;

        let write_message =
            SnapshotWriteMessage::new(epoch, chunk.group, chunk.chunk, artifact.blob.get_hash())
                .to_bytes();
        let write_sig = ctx
            .bls_sign(&write_message)
            .map_err(|e| NodeError::Store(format!("write bls_sign: {e:?}")))?;

        ctx.store
            .put_snapshot_write_sig(
                epoch,
                chunk.group,
                chunk.chunk,
                bitmap_index,
                &SnapshotWriteVote {
                    message: write_message,
                    signature: write_sig,
                },
            )
            .map_err(store_err("put_snapshot_write_sig"))?;

        chunk_count += 1;
    }

    // One finalize partial per local group.
    for group in &my_groups {
        let bitmap_index = bitmap_index_in_group(&state, *group, my_node_id)
            .expect("local group must have a local bitmap index");

        let finalize_message = SnapshotSignMessage::new(epoch, *group).to_bytes();
        let finalize_sig = ctx
            .bls_sign(&finalize_message)
            .map_err(|e| NodeError::Store(format!("finalize bls_sign: {e:?}")))?;

        ctx.store
            .put_snapshot_finalize_sig(
                epoch,
                *group,
                bitmap_index,
                &SnapshotFinalizeVote {
                    message: finalize_message,
                    signature: finalize_sig,
                },
            )
            .map_err(store_err("put_snapshot_finalize_sig"))?;
    }

    Ok(BuildSummary {
        groups: my_groups.len(),
        chunks: chunk_count,
    })
}

/// Clay-encode one outer symbol into its 20 spool-member slices and package
/// the result with derived `BlobInfo`.
fn encode_chunk(
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
    let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

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

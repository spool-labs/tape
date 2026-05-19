//! Build the canonical snapshot candidate for one epoch.
//!
//! The candidate is a deterministic snapshot `Tape` plus the local slice
//! artifacts this node owns. The `Tape` hash is what current-epoch groups vote
//! on; local artifacts are promoted into the serving store only after that
//! hash is canonical.

use std::sync::Arc;

use bytemuck::{bytes_of, Zeroable};
use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda, SYSTEM_ADDRESS};
use tape_api::state::Tape;
use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
use tape_core::snapshot::chunk::{pack_segment, snapshot_chunk_key, SnapshotChunkPayload};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::GroupIndex;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{
    ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
};
use tape_crypto::hash::hash as hash_bytes;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
use tape_crypto::Address;
use tape_protocol::Api;
use tape_slicer::{
    num_stripes, snapshot_max_segment_bytes, snapshot_outer_k, ErasureCoder, OuterCoder, Slicer,
};
use tape_store::ops::{
    EventLogOps, ObjectInfoOps, SliceOps, SnapshotOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::types::{ObjectInfo, SnapshotArtifact, TapeInfo};
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;

/// One encoded snapshot chunk, in memory between build and persistence.
#[derive(Debug, Clone)]
pub struct BuiltChunk {
    pub group: GroupIndex,
    pub chunk: ChunkNumber,
    pub blob: BlobInfo,
    pub slices: [Vec<u8>; GROUP_SIZE],
}

/// Canonical snapshot candidate derived from the local event log.
#[derive(Debug, Clone)]
pub struct SnapshotCandidate {
    pub voting_epoch: EpochNumber,
    pub target_epoch: EpochNumber,
    pub end_slot: SlotNumber,
    pub hash: Hash,
    pub tape: Tape,
    pub tracks: Vec<SnapshotTrack>,
}

#[derive(Debug, Clone)]
pub struct SnapshotTrack {
    pub group: GroupIndex,
    pub chunk: ChunkNumber,
    pub track: CompressedTrack,
    pub blob: BlobInfo,
}

/// Build the snapshot for one epoch and persist this node's local slice
/// artifacts. Returns `None` if this node is not a current committee voter.
pub async fn build_snapshot<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<Option<SnapshotCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_ctx = ctx.clone();
    let task = tokio::task::spawn_blocking(move || build_snapshot_blocking(&owned_ctx, epoch));

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("build_snapshot task join: {e}")))?,
        _ = cancel.cancelled() =>
            Ok(None),
    }
}

fn build_snapshot_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<Option<SnapshotCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let state = ctx.state();
    let me = ctx.node_address();

    if state.find_member(me).is_none() {
        return Ok(None);
    }

    let our_spools = state.member_spools(me);
    if our_spools.is_empty() {
        return Ok(None);
    }

    let voting_epoch = state.epoch();
    let total_groups = usize::try_from(state.current.epoch.total_groups)
        .map_err(|_| NodeError::Store("snapshot total_groups overflow".into()))?;
    let outer_k = snapshot_outer_k(total_groups);
    if outer_k == 0 {
        return Err(NodeError::Store("snapshot total_groups must be non-zero".into()));
    }

    let entries = ctx
        .store
        .get_epoch_events(epoch)
        .map_err(store_err("get_epoch_events"))?;

    let start_slot = entries.first().map(|e| e.slot).unwrap_or(SlotNumber(0));
    let end_slot = entries.last().map(|e| e.slot).unwrap_or(SlotNumber(0));

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
    let max_segment_bytes = snapshot_max_segment_bytes(total_groups);
    let chunk_total = compressed.len().div_ceil(max_segment_bytes).max(1);
    let chunk_size = compressed.len().div_ceil(chunk_total).max(1);

    let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
    let mut tape = Tape {
        id: TapeNumber(0),
        authority: SYSTEM_ADDRESS,
        capacity: StorageUnits(u64::MAX),
        active_epoch: epoch,
        expiry_epoch: EpochNumber(u64::MAX),
        ..Tape::zeroed()
    };

    let mut outer = OuterCoder::new(outer_k, total_groups);
    let mut tracks = Vec::with_capacity(chunk_total * total_groups);

    for chunk_index in 0..chunk_total {
        let start = chunk_index * chunk_size;
        let end = start.saturating_add(chunk_size).min(compressed.len());

        let packed = pack_segment(&compressed[start..end]);
        let symbols = outer.encode(&packed).map_err(|e| {
            NodeError::Store(format!("outer encode epoch={epoch} segment={chunk_index}: {e}"))
        })?;

        let chunk = ChunkNumber(chunk_index as u64);

        for (group_index, symbol) in symbols.iter().enumerate() {
            let group = GroupIndex(group_index as u64);
            let built = encode_chunk(epoch, group, chunk, symbol)?;
            let track_number =
                TrackNumber((chunk_index as u64) * (total_groups as u64) + group.0);
            let track = CompressedTrack {
                tape: snapshot_tape,
                track_number,
                key: snapshot_chunk_key(epoch, group, chunk),
                kind: TrackKind::Blob as u64,
                state: TrackState::Certified as u64,
                size: built.blob.size,
                group,
                value_hash: built.blob.get_hash(),
            };

            tape.write_track(&track).map_err(|error| {
                NodeError::Store(format!("snapshot tape write_track: {error:?}"))
            })?;

            if let Some((spool_index, bitmap_index)) = our_spools.iter().find_map(|spool| {
                (GroupIndex::containing(*spool) == group)
                    .then(|| group.position_of(*spool).map(|position| (*spool, position)))
                    .flatten()
            }) {
                let artifact = SnapshotArtifact {
                    spool_index,
                    blob: built.blob,
                    slice: built.slices[bitmap_index].clone(),
                };

                ctx.store
                    .put_snapshot_artifact(epoch, group, chunk, &artifact)
                    .map_err(store_err("put_snapshot_artifact"))?;
            }

            tracks.push(SnapshotTrack {
                group,
                chunk,
                track,
                blob: built.blob,
            });
        }
    }

    let hash = hash_bytes(bytes_of(&tape));

    Ok(Some(SnapshotCandidate {
        voting_epoch,
        target_epoch: epoch,
        end_slot,
        hash,
        tape,
        tracks,
    }))
}

/// Promote the locally built canonical candidate into the serving store.
///
/// This must only be called once `candidate.hash` is known canonical.
pub fn persist_snapshot_candidate<Db, Cluster, Blockchain>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    candidate: &SnapshotCandidate,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let snapshot_tape = Address::from(snapshot_tape_pda(candidate.target_epoch).0);
    ctx.store
        .put_tape(
            snapshot_tape,
            TapeInfo {
                end_epoch: EpochNumber(u64::MAX),
                next_track_number: candidate.tape.tracks.next_number(),
            },
        )
        .map_err(store_err("put_tape"))?;

    for track in &candidate.tracks {
        let track_address = Address::from(track_pda(track.track.tape, track.track.track_number).0);
        ctx.store
            .put_track(track_address, track.track)
            .map_err(store_err("put_track"))?;
        ctx.store
            .put_track_data(track_address, TrackData::Blob(track.blob))
            .map_err(store_err("put_track_data"))?;
        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::Snapshot {
                    track_address,
                    epoch: candidate.target_epoch,
                    slot: candidate.end_slot,
                },
            )
            .map_err(store_err("put_object_info"))?;

        if let Some(artifact) = ctx
            .store
            .get_snapshot_artifact(candidate.target_epoch, track.group, track.chunk)
            .map_err(store_err("get_snapshot_artifact"))?
        {
            if artifact.blob == track.blob {
                ctx.store
                    .put_slice(artifact.spool_index, track_address, artifact.slice)
                    .map_err(store_err("put_slice"))?;
            }
        }
    }

    Ok(())
}

/// Clay-encode one outer symbol into its 20 spool-member slices and package
/// the result with derived `BlobInfo`.
pub(crate) fn encode_chunk(
    epoch: EpochNumber,
    group: GroupIndex,
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

    let slices: [Vec<u8>; GROUP_SIZE] = slices.try_into().map_err(|v: Vec<Vec<u8>>| {
        NodeError::Store(format!(
            "clay encode produced {}, expected {}",
            v.len(),
            GROUP_SIZE,
        ))
    })?;

    let leaves: [Hash; GROUP_SIZE] = core::array::from_fn(|i| hash_leaf(&slices[i]));
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

//! Build the canonical snapshot candidate for one epoch.
//!
//! The candidate is a deterministic snapshot `Tape` plus the local slice
//! artifacts this node owns. The `Tape` hash is what current-epoch groups vote
//! on; local artifacts are promoted into the serving store only after that
//! hash is canonical.

use std::sync::Arc;

use bytemuck::bytes_of;
use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_api::state::Tape;
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::tape::{snapshot_tape_number, TapeFlags};
use tape_core::spooler::GroupIndex;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber, SlotNumber};
use tape_crypto::hash::hash as hash_bytes;
use tape_crypto::hash::Hash;
use tape_crypto::Address;
use tape_protocol::{Api, ProtocolState};
use tape_snapshot::encode_snapshot;
use tape_store::ops::{
    EventLogOps, ObjectInfoOps, SliceOps, SnapshotOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::types::{ObjectInfo, SnapshotArtifact, SystemObjectKind, TapeInfo};
use tokio_util::sync::CancellationToken;

use crate::context::NodeContext;
use crate::core::error::NodeError;

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
    state: Arc<ProtocolState>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<Option<SnapshotCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_ctx = ctx.clone();
    let task =
        tokio::task::spawn_blocking(move || build_snapshot_blocking(&owned_ctx, &state, epoch));

    tokio::select! {
        result = task => result
            .map_err(|e| NodeError::Store(format!("build_snapshot task join: {e}")))?,
        _ = cancel.cancelled() =>
            Ok(None),
    }
}

fn build_snapshot_blocking<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    epoch: EpochNumber,
) -> Result<Option<SnapshotCandidate>, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
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
    if total_groups == 0 {
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

    let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
    let chunks = encode_snapshot(snapshot_tape, epoch, &snapshot_log, total_groups)
        .map_err(|e| NodeError::Store(format!("encode snapshot epoch={}: {e}", epoch.0)))?;

    // Fold every chunk track into the candidate tape (its hash is what the
    // current-epoch groups vote on) and stash the slice for any spool we own.
    let mut tape = Tape::snapshot(epoch);
    let mut tracks = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        tape.write_track(&chunk.track).map_err(|error| {
            NodeError::Store(format!("snapshot tape write_track: {error:?}"))
        })?;

        if let Some((spool_index, bitmap_index)) = our_spools.iter().find_map(|&spool| {
            if GroupIndex::containing(spool) != chunk.group {
                return None;
            }

            let bitmap_index = chunk.group.position_of(spool)?;
            Some((spool, bitmap_index))
        }) {
            let artifact = SnapshotArtifact {
                spool_index,
                blob: chunk.blob,
                slice: chunk.slices[bitmap_index].clone(),
            };

            ctx.store
                .put_snapshot_artifact(epoch, chunk.group, chunk.chunk, &artifact)
                .map_err(store_err("put_snapshot_artifact"))?;
        }

        tracks.push(SnapshotTrack {
            group: chunk.group,
            chunk: chunk.chunk,
            track: chunk.track,
            blob: chunk.blob,
        });
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
                id: snapshot_tape_number(candidate.target_epoch),
                flags: TapeFlags::SYSTEM,
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
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot {
                        epoch: candidate.target_epoch,
                    },
                    track_address,
                    registered_epoch: candidate.target_epoch,
                    certified_epoch: Some(candidate.target_epoch),
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

fn store_err<E: std::fmt::Display>(op: &'static str) -> impl FnOnce(E) -> NodeError {
    move |e| NodeError::Store(format!("{op}: {e}"))
}

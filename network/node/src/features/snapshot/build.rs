//! Build the canonical snapshot candidate for one epoch.
//!
//! The candidate is a deterministic snapshot `Tape` plus the local slice
//! artifacts this node owns. The `Tape` hash is what current-epoch groups vote
//! on; local artifacts are promoted into the serving store only after that
//! hash is canonical.

use std::sync::Arc;

use bytemuck::bytes_of;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, TrackAddress, SYSTEM_ADDRESS};
use tape_api::state::Tape;
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::tape::{snapshot_tape_number, TapeFlags};
use tape_core::prelude::SpoolIndex;
use tape_core::spooler::TrackGroup;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber, SlotNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::hash as hash_bytes;
use tape_crypto::hash::Hash;
use tape_protocol::{Api, ProtocolState};
use tape_snapshot::{encode_snapshot_streaming, SnapshotChunk};
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
    pub group: TrackGroup,
    pub chunk: ChunkNumber,
    pub track: CompressedTrack,
    pub blob: BlobEncoding,
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

    // Fold every chunk track into the candidate tape (its hash is what the
    // current-epoch groups vote on) and stash the slice for any spool we own.
    let snapshot_tape = snapshot_tape_pda(epoch).0;
    let mut tape = Tape::snapshot(epoch);
    let mut tracks: Vec<SnapshotTrack> = Vec::new();

    encode_snapshot_streaming(
        snapshot_tape,
        epoch,
        &snapshot_log,
        total_groups,
        ctx.params().profile,
        |chunk| -> Result<(), NodeError> {
            keep_chunk(ctx, epoch, &our_spools, &mut tape, &mut tracks, chunk)
        },
    )?;

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

/// Fold one chunk's track into the candidate tape and keep only the slice of a spool this node owns
fn keep_chunk<Db, Cluster, Blockchain>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
    our_spools: &[SpoolIndex],
    tape: &mut Tape,
    tracks: &mut Vec<SnapshotTrack>,
    mut chunk: SnapshotChunk,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    tape.write_track(&chunk.track)
        .map_err(|error| NodeError::Store(format!("snapshot tape write_track: {error:?}")))?;

    if let Some((spool_index, bitmap_index)) = owned_position(our_spools, chunk.group) {
        let artifact = SnapshotArtifact {
            spool_index,
            blob: chunk.blob,
            slice: std::mem::take(&mut chunk.slices[bitmap_index]),
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

    Ok(())
}

/// The stored spool this node holds in `group`, with its position in the group.
fn owned_position(our_spools: &[SpoolIndex], group: TrackGroup) -> Option<(SpoolIndex, usize)> {
    for &spool in our_spools {
        match group.position_of(spool) {
            Some(bitmap_index) => return Some((spool, bitmap_index)),
            None => continue,
        }
    }

    None
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
    let snapshot_tape = snapshot_tape_pda(candidate.target_epoch).0;
    ctx.store
        .put_tape(
            snapshot_tape,
            TapeInfo {
                id: snapshot_tape_number(candidate.target_epoch),
                flags: TapeFlags::SYSTEM,
                end_epoch: EpochNumber(u64::MAX),
                next_track_number: candidate.tape.tracks.next_number(),
                authority: SYSTEM_ADDRESS,
                delegate: Address::default(),
                share_nonce: Hash::default(),
            },
        )
        .map_err(store_err("put_tape"))?;

    for track in &candidate.tracks {
        let track_address = track.track.address();
        ctx.store
            .put_track(track_address, track.track)
            .map_err(store_err("put_track"))?;
        ctx.store
            .put_track_data(track_address, BlobData::Coded(track.blob))
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

#[cfg(test)]
mod tests {
    use tape_core::prelude::EpochPhase;
    use tape_core::snapshot::replay::{ReplayRecord, ReplayableEvent};
    use tape_crypto::tx::Txid;
    use tape_snapshot::encode_snapshot;

    use crate::harness::NodeHarness;

    use super::*;

    const EPOCH: EpochNumber = EpochNumber(4);
    const EVENT_SLOT: SlotNumber = SlotNumber(9);
    const NODE_COUNT: usize = 25;
    const GROUP_COUNT: u64 = 3;
    /// Spool assignment gives this member one spool in two of the three groups.
    const OWNER_INDEX: usize = 20;
    const OWNED_SPOOL_COUNT: usize = 2;

    fn record() -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event: ReplayableEvent::CertifyTrack {
                track: Address::from([3u8; 32]),
                epoch: EPOCH,
            },
        }
    }

    // the build keeps one slice per owned spool and stores no other slice
    #[tokio::test]
    async fn keeps_owned_slices() {
        let harness = NodeHarness::builder()
            .nodes(NODE_COUNT)
            .current_committee_size(NODE_COUNT)
            .current_group_count(GROUP_COUNT)
            .epoch(EPOCH)
            .phase(EpochPhase::Snapshot)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");

        let ctx = harness.ctx_for(OWNER_INDEX);
        let our_spools = harness.owned_spools(OWNER_INDEX);
        ctx.store
            .append_record(EPOCH, EVENT_SLOT, None, &record())
            .expect("append record");

        let state = ctx.state();
        let candidate = build_snapshot_blocking(&ctx, &state, EPOCH)
            .expect("build snapshot")
            .expect("snapshot candidate");

        let log = SnapshotLog {
            epoch: EPOCH,
            start_slot: EVENT_SLOT,
            end_slot: EVENT_SLOT,
            entries: ctx.store.get_epoch_events(EPOCH).expect("epoch events"),
        };
        let full = encode_snapshot(
            snapshot_tape_pda(EPOCH).0,
            EPOCH,
            &log,
            GROUP_COUNT as usize,
            ctx.params().profile,
        )
        .expect("full encode");

        let mut expected_tape = Tape::snapshot(EPOCH);
        for chunk in &full {
            expected_tape.write_track(&chunk.track).expect("write track");
        }

        assert_eq!(candidate.hash, hash_bytes(bytes_of(&expected_tape)));
        assert_eq!(candidate.tracks.len(), full.len());
        for (kept, chunk) in candidate.tracks.iter().zip(full.iter()) {
            assert_eq!(kept.group, chunk.group);
            assert_eq!(kept.chunk, chunk.chunk);
            assert_eq!(kept.track, chunk.track);
            assert_eq!(kept.blob, chunk.blob);
        }

        assert_eq!(our_spools.len(), OWNED_SPOOL_COUNT);
        let mut stored = 0;
        for group_number in 0..GROUP_COUNT {
            stored += ctx
                .store
                .iter_snapshot_artifacts(EPOCH, TrackGroup(group_number))
                .expect("iter artifacts")
                .len();
        }
        assert_eq!(stored, OWNED_SPOOL_COUNT);

        for &spool in &our_spools {
            let group = TrackGroup::containing(spool);
            let position = group.position_of(spool).expect("group position");
            let artifact = ctx
                .store
                .get_snapshot_artifact(EPOCH, group, ChunkNumber(0))
                .expect("get artifact")
                .expect("owned artifact");
            let coded = full
                .iter()
                .find(|chunk| chunk.group == group && chunk.chunk == ChunkNumber(0))
                .expect("encoded chunk");

            assert_eq!(artifact.spool_index, spool);
            assert_eq!(artifact.slice, coded.slices[position]);
        }
    }
}

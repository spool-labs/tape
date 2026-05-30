///! Fetch and decode snapshot logs from the network during bootstrap.
///! Inverse of `build.rs`'s encode path.

use std::collections::BTreeMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::GroupIndex;
use tape_core::tape::{snapshot_tape_number, TapeFlags};
use tape_core::types::SpoolIndex;
use tape_core::track::blob::BlobInfo;
use tape_core::track::data::TrackData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_protocol::api::{GetSliceReq, GetTrackDataReq, ListTracksByTapeReq};
use tape_protocol::Api;
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};
use tape_snapshot::{
    assemble_snapshot_log, decode_chunk_payload, snapshot_track_group_count,
    validate_snapshot_track_list, verify_snapshot_track_set, K_INNER,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn, Instrument};

use crate::context::NodeContext;
use crate::core::error::NodeError;

const LIST_TRACKS_LIMIT: u32 = 256;

/// A reconstructed epoch snapshot: the decoded log plus the verified chunk-track
/// list it was decoded from (anchored to the on-chain committed root).
pub struct DecodedSnapshot {
    pub log: SnapshotLog,
    pub tracks: Vec<CompressedTrack>,
}

/// Fetch every chunk for an epoch's snapshot tape from peers, decode, and
/// return the reconstructed `SnapshotLog`.
pub async fn fetch_and_decode_epoch<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<DecodedSnapshot, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let tape = Address::from(snapshot_tape_pda(epoch).0);

    // The committed track-merkle root the canonical snapshot was voted on. Every
    // peer-supplied chunk-track list is verified against this before we trust its
    // metadata or decode its slices.
    let committed = context
        .rpc
        .get_snapshot_tape(epoch)
        .await
        .map_err(NodeError::Rpc)?;

    let candidates = list_snapshot_track_candidates(context, tape).await?;

    let mut last_error = None;
    for (peer, tracks) in candidates {
        if let Err(error) = verify_snapshot_track_set(&tracks, &committed.tracks) {
            warn!(node = %peer, %error, "bootstrap: snapshot track list failed root verification");
            last_error = Some(NodeError::Store(error.to_string()));
            continue;
        }

        match decode_snapshot_tracks(context, epoch, tracks.clone(), cancel).await {
            Ok(log) => return Ok(DecodedSnapshot { log, tracks }),
            Err(error) => {
                warn!(node = %peer, ?error, "bootstrap: candidate snapshot track list failed");
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        NodeError::Store(format!(
            "bootstrap: no usable snapshot track list for epoch {epoch}"
        ))
    }))
}

fn snapshot_err(error: tape_snapshot::SnapshotError) -> NodeError {
    NodeError::Store(error.to_string())
}

/// Materialize the snapshot tape and its chunk-track metadata after a bootstrap
/// replay, so the node takes the same custody entry point a builder would have.
///
/// Mirrors `persist_snapshot_candidate` minus the blob/slice data: the chunk
/// tracks are written as `ObjectInfo::System { Snapshot }` (always certified),
/// and the generic spool sync/repair then fetches the slices for owned spools.
/// `decoded.tracks` was verified against the committed merkle root before decode.
pub fn persist_snapshot_metadata<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    epoch: EpochNumber,
    decoded: &DecodedSnapshot,
) -> Result<(), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
    context
        .store
        .put_tape(
            snapshot_tape,
            TapeInfo {
                id: snapshot_tape_number(epoch),
                flags: TapeFlags::SYSTEM,
                end_epoch: EpochNumber(u64::MAX),
                next_track_number: TrackNumber(decoded.tracks.len() as u64),
            },
        )
        .map_err(store_err)?;

    for track in &decoded.tracks {
        let track_address = Address::from(track_pda(track.tape, track.track_number).0);
        context
            .store
            .put_track(track_address, *track)
            .map_err(store_err)?;
        context
            .store
            .put_object_info(
                track_address,
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot { epoch },
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: decoded.log.end_slot,
                },
            )
            .map_err(store_err)?;
    }

    Ok(())
}

fn store_err(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

async fn decode_snapshot_tracks<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    tracks: Vec<CompressedTrack>,
    cancel: &CancellationToken,
) -> Result<SnapshotLog, NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let tape = Address::from(snapshot_tape_pda(epoch).0);
    validate_snapshot_track_list(epoch, tape, &tracks).map_err(snapshot_err)?;
    let total_groups = snapshot_track_group_count(epoch, &tracks).map_err(snapshot_err)?;

    debug!(
        epoch = epoch.0,
        ?tape,
        tracks = tracks.len(),
        "bootstrap: fetched snapshot track list"
    );

    // Fan out: fetch + Clay-decode every track in parallel. Each task returns
    // the `(group, chunk, outer-symbol)` triple recovered from the Clay
    // payload. Tasks that fail are logged and skipped; outer RS recovers as
    // long as enough groups succeed per segment for this snapshot's group count.
    let mut join = JoinSet::new();
    for track in tracks {
        let context = context.clone();
        let cancel = cancel.clone();
        join.spawn(
            async move { fetch_and_decode_track(&context, epoch, track, &cancel).await }
                .in_current_span(),
        );
    }

    let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
    while let Some(result) = join.join_next().await {
        if cancel.is_cancelled() {
            join.abort_all();
            return Err(NodeError::Store("bootstrap: cancelled".into()));
        }
        match result.map_err(|e| NodeError::Store(format!("bootstrap: decode task join: {e}")))? {
            Ok(Decoded { group, chunk, symbol }) => {
                symbols_by_segment
                    .entry(chunk)
                    .or_default()
                    .push((group.0 as usize, symbol));
            }
            Err(error) => {
                warn!(?error, "bootstrap: track decode failed");
            }
        }
    }

    assemble_snapshot_log(&symbols_by_segment, epoch, total_groups).map_err(snapshot_err)
}

struct Decoded {
    group: GroupIndex,
    chunk: ChunkNumber,
    symbol: Vec<u8>,
}

/// Fetch K_INNER verified slices for one track and Clay-decode them.
async fn fetch_and_decode_track<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    track: CompressedTrack,
    cancel: &CancellationToken,
) -> Result<Decoded, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let group = track.group;
    let track_address = Address::from(track_pda(track.tape, track.track_number).0);

    let peers = context.state().group_peers(group);
    if peers.is_empty() {
        return Err(NodeError::Store(format!(
            "bootstrap: no peers for group {} in current committee",
            group.0
        )));
    }

    let blob = fetch_blob(context.api.as_ref(), &peers, track_address).await?;
    if blob.get_hash() != track.value_hash {
        return Err(NodeError::Store(format!(
            "bootstrap: blob metadata for epoch={} group={} track={} does not match on-chain value_hash",
            epoch.0, group.0, track_address
        )));
    }

    let slices = fetch_verified_slices(
        context.api.as_ref(),
        &peers,
        group,
        track_address,
        &blob,
        cancel,
    )
    .await?;

    let refs: Vec<(usize, &[u8])> = slices.iter().map(|(i, d)| (*i, d.as_slice())).collect();
    let (chunk, symbol) = decode_chunk_payload(&refs).map_err(|e| {
        NodeError::Store(format!(
            "bootstrap: chunk decode epoch={} group={} track={}: {e}",
            epoch.0, group.0, track_address
        ))
    })?;

    Ok(Decoded {
        group,
        chunk,
        symbol,
    })
}

async fn fetch_verified_slices<Cluster: Api>(
    api: &Cluster,
    peers: &[(SpoolIndex, Address)],
    group: GroupIndex,
    track: Address,
    blob: &BlobInfo,
    cancel: &CancellationToken,
) -> Result<Vec<(usize, Vec<u8>)>, NodeError> {
    let mut out: Vec<(usize, Vec<u8>)> = Vec::with_capacity(K_INNER);
    for (spool, peer) in peers {
        if cancel.is_cancelled() {
            return Err(NodeError::Store("bootstrap: cancelled".into()));
        }
        let Some(leaf_idx) = group.position_of(*spool) else {
            continue;
        };
        match api
            .get_slice(*peer, &GetSliceReq { track, spool: *spool })
            .await
        {
            Ok(res) => {
                if !blob.verify_slice(SpoolIndex::from(leaf_idx as u64), &res.data) {
                    warn!(
                        node = %peer,
                        spool = %spool,
                        "bootstrap: slice failed leaf verification"
                    );
                    continue;
                }
                out.push((leaf_idx, res.data));
                if out.len() >= K_INNER {
                    return Ok(out);
                }
            }
            Err(error) => {
                trace!(
                    node = %peer,
                    spool = %spool,
                    ?error,
                    "bootstrap: get_slice failed"
                );
            }
        }
    }
    Err(NodeError::Store(format!(
        "bootstrap: only {}/{} slices for group {}",
        out.len(),
        K_INNER,
        group.0
    )))
}

async fn fetch_blob<Cluster: Api>(
    api: &Cluster,
    peers: &[(SpoolIndex, Address)],
    track: Address,
) -> Result<BlobInfo, NodeError> {
    let mut last_error: Option<NodeError> = None;
    for (_, peer) in peers {
        match api.get_track_data(*peer, &GetTrackDataReq { track }).await {
            Ok(res) => match res.data {
                TrackData::Blob(blob) => return Ok(blob),
                other => {
                    return Err(NodeError::Store(format!(
                        "bootstrap: expected blob track data, got {other:?}"
                    )));
                }
            },
            Err(error) => {
                last_error = Some(NodeError::Store(format!("get_track_data: {error}")));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        NodeError::Store("bootstrap: no peer returned blob metadata".into())
    }))
}

async fn list_snapshot_track_candidates<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    tape: Address,
) -> Result<Vec<(Address, Vec<CompressedTrack>)>, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let peers: Vec<Address> = context
        .state()
        .current
        .committee
        .iter()
        .map(|m| m.node)
        .collect();
    if peers.is_empty() {
        return Err(NodeError::Store(
            "bootstrap: no committee peers available for snapshot track listing".into(),
        ));
    }

    let mut candidates = Vec::new();
    let mut last_error: Option<NodeError> = None;
    for peer in &peers {
        match list_tracks_from_peer(context.api.as_ref(), *peer, tape).await {
            Ok(tracks) if tracks.is_empty() => {
                debug!(
                    node = %peer,
                    ?tape,
                    "bootstrap: peer returned empty snapshot track list"
                );
            }
            Ok(tracks) => candidates.push((*peer, tracks)),
            Err(error) => {
                warn!(node = %peer, ?error, "bootstrap: list_tracks_by_tape failed");
                last_error = Some(error);
            }
        }
    }

    if candidates.is_empty() {
        Err(last_error.unwrap_or_else(|| {
            NodeError::Store(format!(
                "bootstrap: no peer returned snapshot tracks for tape {tape}"
            ))
        }))
    } else {
        Ok(candidates)
    }
}

async fn list_tracks_from_peer<Cluster: Api>(
    api: &Cluster,
    peer: Address,
    tape: Address,
) -> Result<Vec<CompressedTrack>, NodeError> {
    let mut out = Vec::new();
    let mut cursor: Option<TrackNumber> = None;
    loop {
        let res = api
            .list_tracks_by_tape(
                peer,
                &ListTracksByTapeReq {
                    tape,
                    cursor,
                    limit: LIST_TRACKS_LIMIT,
                },
            )
            .await
            .map_err(|error| NodeError::Store(format!("list_tracks_by_tape: {error}")))?;

        out.extend(res.tracks);
        match res.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(out),
        }
    }
}

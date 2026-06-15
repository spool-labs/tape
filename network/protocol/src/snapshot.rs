//! Transport-generic snapshot reader.
//!
//! Reconstructs an epoch's [`SnapshotLog`] from peer-served chunk-track data,
//! verifying everything against the consensus-committed track-merkle root. It is
//! the shared `enumerate -> verify -> fetch -> decode` pipeline used by both the
//! node (bootstrap) and external readers (the epoch explorer): the caller supplies
//! an [`Api`] transport, the current [`ProtocolState`] for routing, and the
//! committed [`TrackArchive`] (from the on-chain snapshot tape). No consensus,
//! Solana, or store deps live here; decode/verify is delegated to `tape-snapshot`.

use std::collections::BTreeMap;
use std::sync::Arc;

use tape_api::program::tapedrive::track_pda;
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::spooler::GroupIndex;
use tape_core::track::archive::TrackArchive;
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber, SpoolIndex, TrackNumber};
use tape_crypto::address::Address;
use tape_snapshot::{
    assemble_snapshot_log, decode_chunk_payload, snapshot_track_group_count,
    validate_snapshot_track_list, verify_snapshot_track_set, SnapshotError, K_INNER,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::api::{GetSliceReq, GetTrackDataReq, ListTracksByTapeReq};
use crate::{Api, ApiError, ProtocolState};

const LIST_TRACKS_LIMIT: u32 = 256;

/// A reconstructed epoch snapshot: the decoded log plus the verified chunk tracks
/// that were fetched and decoded from the committed snapshot tape.
pub struct DecodedSnapshot {
    pub log: SnapshotLog,
    pub tracks: Vec<DecodedSnapshotTrack>,
}

/// A verified snapshot chunk track and the blob metadata used to decode it.
#[derive(Debug, Clone, Copy)]
pub struct DecodedSnapshotTrack {
    pub state: CompressedTrack,
    pub blob: BlobEncoding,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotReaderError {
    #[error("snapshot codec: {0}")]
    Codec(#[from] SnapshotError),

    #[error("transport: {0}")]
    Transport(#[from] ApiError),

    #[error("no committee peers available to list snapshot tracks for tape {0}")]
    NoCandidatePeers(Address),

    #[error("no peer returned snapshot tracks for tape {0}")]
    NoTrackList(Address),

    #[error("no usable snapshot track list for epoch {0}")]
    NoUsableTrackList(u64),

    #[error("no peers for group {0} in committee")]
    NoGroupPeers(u64),

    #[error("blob metadata for group {group} track {track} does not match committed value_hash")]
    BlobHashMismatch { group: u64, track: Address },

    #[error("expected blob track data for track {0}")]
    NonBlobTrackData(Address),

    #[error("no peer returned blob metadata for track {0}")]
    NoBlob(Address),

    #[error("only {got}/{need} verified slices for group {group}")]
    SliceShortfall { group: u64, got: usize, need: usize },

    #[error("snapshot read cancelled")]
    Cancelled,

    #[error("decode task join: {0}")]
    Join(String),
}

/// Fetch chunk tracks for an epoch's snapshot tape from peers, verify the served
/// track list against `committed`, decode, and return the reconstructed
/// [`SnapshotLog`] alongside the verified chunk records used to decode it.
///
/// `state` provides routing (committee peers + group ownership) for the *current*
/// epoch, which holds custody of every historical snapshot's spool data.
pub async fn read_snapshot_epoch<A: Api + 'static>(
    api: &Arc<A>,
    state: &ProtocolState,
    committed: &TrackArchive,
    tape: Address,
    epoch: EpochNumber,
    cancel: &CancellationToken,
) -> Result<DecodedSnapshot, SnapshotReaderError> {
    let candidates = list_track_candidates(api.as_ref(), state, tape).await?;

    let mut last_error = None;
    for (peer, tracks) in candidates {
        if let Err(error) = verify_snapshot_track_set(&tracks, committed) {
            warn!(node = %peer, %error, "snapshot reader: track list failed root verification");
            last_error = Some(SnapshotReaderError::Codec(error));
            continue;
        }

        match decode_snapshot_tracks(api, state, tape, epoch, tracks.clone(), cancel).await {
            Ok(decoded) => return Ok(decoded),
            Err(error) => {
                warn!(node = %peer, ?error, "snapshot reader: candidate track list failed");
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or(SnapshotReaderError::NoUsableTrackList(epoch.0)))
}

async fn decode_snapshot_tracks<A: Api + 'static>(
    api: &Arc<A>,
    state: &ProtocolState,
    tape: Address,
    epoch: EpochNumber,
    tracks: Vec<CompressedTrack>,
    cancel: &CancellationToken,
) -> Result<DecodedSnapshot, SnapshotReaderError> {
    validate_snapshot_track_list(epoch, tape, &tracks)?;
    let total_groups = snapshot_track_group_count(epoch, &tracks)?;

    debug!(
        epoch = epoch.0,
        ?tape,
        tracks = tracks.len(),
        "snapshot reader: decoding chunk-track list"
    );

    // Fan out: fetch + Clay-decode every track in parallel. Each task returns the
    // `(group, chunk, outer-symbol)` triple recovered from the Clay payload. Tasks
    // that fail are logged and skipped; outer RS recovers as long as enough groups
    // succeed per segment for this snapshot's group count.
    let mut join = JoinSet::new();
    for track in tracks {
        let peers = state.group_peers(track.group);
        if peers.is_empty() {
            warn!(group = track.group.0, "snapshot reader: no peers for group");
            continue;
        }
        let api = api.clone();
        let cancel = cancel.clone();
        join.spawn(async move { fetch_and_decode_track(api, epoch, track, peers, cancel).await });
    }

    let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> = BTreeMap::new();
    let mut snapshot_tracks = Vec::new();
    while let Some(result) = join.join_next().await {
        if cancel.is_cancelled() {
            join.abort_all();
            return Err(SnapshotReaderError::Cancelled);
        }
        match result.map_err(|e| SnapshotReaderError::Join(e.to_string()))? {
            Ok(Decoded { group, chunk, symbol, track, blob }) => {
                symbols_by_segment
                    .entry(chunk)
                    .or_default()
                    .push((group.0 as usize, symbol));
                snapshot_tracks.push(DecodedSnapshotTrack { state: track, blob });
            }
            Err(error) => {
                warn!(?error, "snapshot reader: track decode failed");
            }
        }
    }

    snapshot_tracks.sort_by_key(|track| track.state.track_number.0);
    Ok(DecodedSnapshot {
        log: assemble_snapshot_log(&symbols_by_segment, epoch, total_groups)?,
        tracks: snapshot_tracks,
    })
}

struct Decoded {
    group: GroupIndex,
    chunk: ChunkNumber,
    symbol: Vec<u8>,
    track: CompressedTrack,
    blob: BlobEncoding,
}

/// Fetch verified slices for one chunk track and Clay-decode them.
async fn fetch_and_decode_track<A: Api>(
    api: Arc<A>,
    epoch: EpochNumber,
    track: CompressedTrack,
    peers: Vec<(SpoolIndex, Address)>,
    cancel: CancellationToken,
) -> Result<Decoded, SnapshotReaderError> {
    let group = track.group;
    let track_address = Address::from(track_pda(track.tape, track.track_number).0);

    let blob = fetch_blob(api.as_ref(), &peers, track_address).await?;
    if blob.get_hash() != track.value_hash {
        return Err(SnapshotReaderError::BlobHashMismatch {
            group: group.0,
            track: track_address,
        });
    }

    let slices =
        fetch_verified_slices(api.as_ref(), &peers, group, track_address, &blob, &cancel).await?;

    let refs: Vec<(usize, &[u8])> = slices.iter().map(|(i, d)| (*i, d.as_slice())).collect();
    let (chunk, symbol) = decode_chunk_payload(&refs)?;

    debug!(
        epoch = epoch.0,
        group = group.0,
        track = %track_address,
        chunk = chunk.0,
        "snapshot reader: chunk decoded"
    );

    Ok(Decoded {
        group,
        chunk,
        symbol,
        track,
        blob,
    })
}

async fn fetch_verified_slices<A: Api>(
    api: &A,
    peers: &[(SpoolIndex, Address)],
    group: GroupIndex,
    track: Address,
    blob: &BlobEncoding,
    cancel: &CancellationToken,
) -> Result<Vec<(usize, Vec<u8>)>, SnapshotReaderError> {
    let mut out: Vec<(usize, Vec<u8>)> = Vec::with_capacity(K_INNER);
    for (spool, peer) in peers {
        if cancel.is_cancelled() {
            return Err(SnapshotReaderError::Cancelled);
        }
        let Some(leaf_idx) = group.position_of(*spool) else {
            continue;
        };
        match api.get_slice(*peer, &GetSliceReq { track, spool: *spool }).await {
            Ok(res) => {
                if !blob.verify_slice(SpoolIndex::from(leaf_idx as u64), &res.data) {
                    warn!(node = %peer, spool = %spool, "snapshot reader: slice failed leaf verification");
                    continue;
                }
                out.push((leaf_idx, res.data));
                if out.len() >= K_INNER {
                    return Ok(out);
                }
            }
            Err(error) => {
                trace!(node = %peer, spool = %spool, ?error, "snapshot reader: get_slice failed");
            }
        }
    }
    Err(SnapshotReaderError::SliceShortfall {
        group: group.0,
        got: out.len(),
        need: K_INNER,
    })
}

async fn fetch_blob<A: Api>(
    api: &A,
    peers: &[(SpoolIndex, Address)],
    track: Address,
) -> Result<BlobEncoding, SnapshotReaderError> {
    let mut last_error: Option<SnapshotReaderError> = None;
    for (_, peer) in peers {
        match api.get_track_data(*peer, &GetTrackDataReq { track }).await {
            Ok(res) => match res.data {
                BlobData::Coded(blob) => return Ok(blob),
                _ => return Err(SnapshotReaderError::NonBlobTrackData(track)),
            },
            Err(error) => last_error = Some(SnapshotReaderError::Transport(error)),
        }
    }
    Err(last_error.unwrap_or(SnapshotReaderError::NoBlob(track)))
}

async fn list_track_candidates<A: Api>(
    api: &A,
    state: &ProtocolState,
    tape: Address,
) -> Result<Vec<(Address, Vec<CompressedTrack>)>, SnapshotReaderError> {
    let peers: Vec<Address> = state.current.committee.iter().map(|m| m.node).collect();
    if peers.is_empty() {
        return Err(SnapshotReaderError::NoCandidatePeers(tape));
    }

    let mut candidates = Vec::new();
    let mut last_error: Option<SnapshotReaderError> = None;
    for peer in &peers {
        match list_tracks_from_peer(api, *peer, tape).await {
            Ok(tracks) if tracks.is_empty() => {
                debug!(node = %peer, ?tape, "snapshot reader: peer returned empty track list");
            }
            Ok(tracks) => candidates.push((*peer, tracks)),
            Err(error) => {
                warn!(node = %peer, ?error, "snapshot reader: list_tracks_by_tape failed");
                last_error = Some(error);
            }
        }
    }

    if candidates.is_empty() {
        Err(last_error.unwrap_or(SnapshotReaderError::NoTrackList(tape)))
    } else {
        Ok(candidates)
    }
}

async fn list_tracks_from_peer<A: Api>(
    api: &A,
    peer: Address,
    tape: Address,
) -> Result<Vec<CompressedTrack>, SnapshotReaderError> {
    let mut out = Vec::new();
    let mut cursor: Option<TrackNumber> = None;
    loop {
        let res = api
            .list_tracks_by_tape(peer, &ListTracksByTapeReq { tape, cursor, limit: LIST_TRACKS_LIMIT })
            .await?;

        out.extend(res.tracks);
        match res.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(out),
        }
    }
}

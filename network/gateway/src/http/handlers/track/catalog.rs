use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, Uri};
use axum::response::IntoResponse;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::TrackNumber;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_crypto::merkle::{MerkleLeafTree, hash_leaf};
use tape_protocol::Api;
use tape_protocol::api::{
    FindTrackRequest, ListObjectsRequest, ListObjectsResponse, ListTracksByTapeRequest,
    ListTracksByTapeResponse, ObjectListItem, TrackDataResponse, TrackProofResponse,
    TrackResponse,
};
use tape_store::ops::{ObjectListOps, TrackOps, object_has_left, track_has_left};
use tape_store::types::TapeInfo;

use super::{
    parse_address, tape_with_pending, track_address_by_number, track_data_with_pending,
    track_with_pending,
};
use crate::http::error::RouteError;
use crate::http::handlers::share::authorize_signed_read;
use crate::http::handlers::{binary_response, store_error};
use crate::http::state::AppState;

const MAX_TRACK_SCAN_LIMIT: usize = u32::MAX as usize;
const MAX_OBJECT_LIST_LIMIT: usize = 1_000;

pub async fn get_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

pub async fn get_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    // This gateway's own store serves the blob metadata, so an admitted caller needs no node token.
    authorize_signed_read(&state, &headers, &method, uri.path(), &track)?;

    let data = track_data_with_pending(&state, track_addr, track.tape)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackDataResponse { data })
}

pub async fn get_track_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track_addr = parse_address(&track_id, "track id")?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    // A pending rewind or renew moves the tree's start, and a track written
    // after it sits in the new tree.
    let tape = tape_with_pending(&state, track.tape)?.ok_or(RouteError::NotFound)?;

    let pending_tracks = state.context.pending.registered_tracks_by_tape(track.tape);
    let pending_leaf_count = pending_tracks
        .iter()
        .filter(|(_, pending)| !track_has_left(Some(&tape), pending))
        .map(|(_, track)| track.track_number.next().as_usize())
        .max()
        .unwrap_or(0);

    // A tape's tree starts over when it is cleared and the track counter keeps
    // counting, so a track sits at its number less the tree's start. A track
    // from before the start has no leaf.
    let tree_start = tape.tree_start.as_usize();
    let track_index = (track.track_number.0 as usize)
        .checked_sub(tree_start)
        .ok_or(RouteError::NotFound)?;
    let leaf_count = (tape.next_track_number.0 as usize)
        .max(pending_leaf_count)
        .max(track.track_number.next().as_usize())
        .saturating_sub(tree_start);

    if leaf_count == 0 || leaf_count > (1usize << TRACK_TREE_HEIGHT) || track_index >= leaf_count
    {
        return Err(RouteError::Internal("invalid tape track numbering".into()));
    }

    let empty_hash = hash_leaf(&[]);
    let mut leaves = vec![empty_hash; leaf_count];
    let disk_tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(track.tape, tape.id, tape.tree_start.checked_prev(), leaf_count)
        .map_err(store_error)?;

    for tape_track in merge_pending_tape_tracks(&state, Some(&tape), disk_tracks, pending_tracks) {
        let Some(index) = (tape_track.track_number.0 as usize).checked_sub(tree_start) else {
            continue;
        };
        if index < leaf_count {
            leaves[index] = tape_track.get_hash();
        }
    }

    let proof: [Hash; TRACK_TREE_HEIGHT] = MerkleLeafTree::new(&leaves, TRACK_TREE_HEIGHT)
        .and_then(|tree| tree.proof_at_n::<{ TRACK_TREE_HEIGHT }>(track_index))
        .map_err(|_| RouteError::Internal("invalid track proof".into()))?;

    binary_response(&TrackProofResponse {
        proof: CompressedTrackProof {
            state: track,
            proof,
        }
        .pack(),
    })
}

pub async fn get_track_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number)): Path<(String, u64)>,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let track_addr = track_address_by_number(&state, tape, TrackNumber(track_number))?
        .ok_or(RouteError::NotFound)?;
    let track = track_with_pending(&state, track_addr)?.ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

pub async fn find_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let request: FindTrackRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("find track request: {error}")))?;

    let pending_tracks = state.context.pending.registered_tracks_by_tape(tape);
    // The row has the number of the tape at this address now, and the tracks
    // from its tree's start on are the ones the tape holds.
    let tape_row = tape_with_pending(&state, tape)?;
    let disk_tracks = match &tape_row {
        Some(tape_row) => state
            .context
            .store
            .iter_tracks_by_tape_from(
                tape,
                tape_row.id,
                tape_row.tree_start.checked_prev(),
                MAX_TRACK_SCAN_LIMIT,
            )
            .map_err(store_error)?,
        None => Vec::new(),
    };
    let mut matches =
        merge_pending_tape_tracks(&state, tape_row.as_ref(), disk_tracks, pending_tracks)
        .into_iter()
        .filter(|track| track.key == request.key)
        .collect::<Vec<_>>();
    matches.sort_by_key(|track| track.track_number.0);

    let track = match request.version {
        tape_protocol::api::ops::FindTrackVersion::Latest => matches.pop(),
        tape_protocol::api::ops::FindTrackVersion::Number(track_number) => matches
            .into_iter()
            .find(|track| track.track_number == track_number),
    }
    .ok_or(RouteError::NotFound)?;

    binary_response(&TrackResponse {
        track: track.pack(),
    })
}

pub async fn list_tracks_by_tape<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape = parse_address(&tape_id, "tape id")?;
    let request: ListTracksByTapeRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list tracks request: {error}")))?;
    let limit = (request.limit as usize).clamp(1, MAX_TRACK_SCAN_LIMIT);

    let pending_tracks = state.context.pending.registered_tracks_by_tape(tape);
    let disk_limit = limit
        .saturating_add(pending_tracks.len())
        .saturating_add(1)
        .min(MAX_TRACK_SCAN_LIMIT);
    // The row has the number of the tape at this address now, and the tracks
    // from its tree's start on are the ones the tape holds.
    let tape_row = tape_with_pending(&state, tape)?;
    let disk_tracks = match &tape_row {
        Some(tape_row) => {
            let after = request.cursor.max(tape_row.tree_start.checked_prev());
            state
                .context
                .store
                .iter_tracks_by_tape_from(tape, tape_row.id, after, disk_limit)
                .map_err(store_error)?
        }
        None => Vec::new(),
    };

    let mut tracks =
        merge_pending_tape_tracks(&state, tape_row.as_ref(), disk_tracks, pending_tracks);
    if let Some(cursor) = request.cursor {
        tracks.retain(|track| track.track_number > cursor);
    }

    tracks.sort_by_key(|track| track.track_number.0);
    // The cursor is the last track sent, since the next page starts after it.
    let next_cursor = (tracks.len() > limit).then(|| tracks[limit - 1].track_number);
    let tracks = tracks
        .into_iter()
        .take(limit)
        .map(|track| track.pack())
        .collect();

    binary_response(&ListTracksByTapeResponse {
        tracks,
        next_cursor,
    })
}

pub async fn list_objects<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let bucket = parse_address(&tape_id, "tape id")?;
    let request: ListObjectsRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list objects request: {error}")))?;
    let limit = (request.limit as usize).clamp(1, MAX_OBJECT_LIST_LIMIT);
    let delimiter = request
        .delimiter
        .as_deref()
        .filter(|delimiter| !delimiter.is_empty());

    let page = state
        .context
        .store
        .list_objects(
            bucket,
            &request.prefix,
            delimiter,
            request.cursor.as_deref(),
            limit,
        )
        .map_err(store_error)?;

    // The store's listing follows the stored row. A close, rewind or renew
    // that is still pending takes its objects out here.
    let tape_row = tape_with_pending(&state, bucket)?;

    let objects = page
        .objects
        .into_iter()
        .filter(|(_, entry)| !object_has_left(tape_row.as_ref(), entry))
        .map(|(name, entry)| ObjectListItem {
            name,
            size: entry.size,
            etag: entry.etag,
            block_time: entry.block_time,
            slot: entry.slot,
            data_tape: entry.data_tape,
            track_number: entry.track_number,
            track: entry.track,
            kind: entry.kind,
            content_type: entry.content_type,
        })
        .collect();

    binary_response(&ListObjectsResponse {
        objects,
        common_prefixes: page.common_prefixes,
        next_cursor: page.next,
        is_truncated: page.is_truncated,
    })
}

/// A tape's stored tracks with its pending ones folded in. `tape_row` is the
/// row at the tape's address, and a pending track that has left it is dropped:
/// one written before a rewind or a close that is itself still pending.
fn merge_pending_tape_tracks<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape_row: Option<&TapeInfo>,
    disk_tracks: Vec<CompressedTrack>,
    pending_tracks: Vec<(Address, CompressedTrack)>,
) -> Vec<CompressedTrack> {
    let mut by_number = BTreeMap::new();

    for disk_track in disk_tracks {
        let track_addr = disk_track.address();
        if let Some(track) = state
            .context
            .pending
            .apply_to_track(track_addr, Some(disk_track))
        {
            by_number.insert(track.track_number, track);
        }
    }

    for (_, track) in pending_tracks {
        if !track_has_left(tape_row, &track) {
            by_number.insert(track.track_number, track);
        }
    }

    by_number.into_values().collect()
}

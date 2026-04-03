//! Track metadata and proof endpoints

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_crypto::merkle::{create_proof_from_leaf_hashes, hash_leaf};
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, FindTrackRequest, ListTracksByTapeRequest, ListTracksByTapeResponse,
    TrackDataResponse, TrackProofResponse, TrackResponse, ops::FindTrackVersion,
};
use tape_store::ops::{TapeOps, TrackDataOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

const MAX_TRACK_SCAN_LIMIT: usize = u32::MAX as usize;

pub async fn get_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track = state
        .context
        .store
        .get_track(track.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_data<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track = state
        .context
        .store
        .get_track(track.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let protocol = state.context.state();
    let is_owner = protocol
        .group_peers(track.spool_group)
        .into_iter()
        .any(|(_, node_id)| node_id == state.context.node_id());
    if !is_owner {
        return Err(RouteError::NotResponsible);
    }

    let data = state
        .context
        .store
        .get_track_data(track_pda(track.tape, track.track_number).0.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackDataResponse {
        data,
    })
    .map_err(|error| RouteError::Internal(format!("serialize track data response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {
    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track = state
        .context
        .store
        .get_track(track.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let tape = state
        .context
        .store
        .get_tape(track.tape.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let leaf_count = tape.next_track_number.0 as usize;
    let track_index = track.track_number.0 as usize;
    if leaf_count == 0
        || leaf_count > (1usize << TRACK_TREE_HEIGHT)
        || track_index >= leaf_count
    {
        return Err(RouteError::Internal("invalid tape track numbering".into()));
    }

    let empty_hash = hash_leaf(&[]);
    let mut leaves = vec![empty_hash; leaf_count];
    for tape_track in state
        .context
        .store
        .iter_tracks_by_tape_from(track.tape.into(), None, leaf_count)
        .map_err(store_error)?
        .into_iter()
    {
        let index = tape_track.track_number.0 as usize;
        if index < leaf_count {
            leaves[index] = tape_track.get_hash();
        }
    }

    let proof: [Hash; TRACK_TREE_HEIGHT] =
        create_proof_from_leaf_hashes::<{ TRACK_TREE_HEIGHT }>(&leaves, track_index)
            .map_err(|_| RouteError::Internal("invalid track proof".into()))?
            .try_into()
            .map_err(|_| RouteError::Internal("invalid track proof length".into()))?;

    let body = wincode::serialize(&TrackProofResponse {
        proof: CompressedTrackProof { state: track, proof }.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track proof response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn get_track_by_number<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((tape_id, track_number)): Path<(String, u64)>,
) -> Result<impl IntoResponse, RouteError> {
    let tape: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;
    let track = track_pda(tape, TrackNumber(track_number)).0;
    let track = state
        .context
        .store
        .get_track(track.into())
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn find_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;
    let request: FindTrackRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("find track request: {error}")))?;

    let mut matches = state
        .context
        .store
        .iter_tracks_by_tape_from(tape.into(), None, MAX_TRACK_SCAN_LIMIT)
        .map_err(store_error)?
        .into_iter()
        .filter(|track| track.key == request.key)
        .collect::<Vec<_>>();

    matches.sort_by_key(|track| track.track_number.0);

    let track = match request.version {
        FindTrackVersion::Latest => matches.pop(),
        FindTrackVersion::Number(track_number) => {
            matches.into_iter().find(|track| track.track_number == track_number)
        }
    }
    .ok_or(RouteError::NotFound)?;

    let body = wincode::serialize(&TrackResponse {
        track: track.pack(),
    })
    .map_err(|error| RouteError::Internal(format!("serialize track response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

pub async fn list_tracks_by_tape<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(tape_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let tape: Address = tape_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid tape id: {error}")))?;
    let request: ListTracksByTapeRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("list tracks request: {error}")))?;

    let limit = (request.limit as usize).clamp(1, MAX_TRACK_SCAN_LIMIT);
    let tracks = state
        .context
        .store
        .iter_tracks_by_tape_from(tape.into(), request.cursor, limit + 1)
        .map_err(store_error)?
        .into_iter()
        .collect::<Vec<_>>();
    let next_cursor = tracks.get(limit).map(|track| track.track_number);
    let tracks = tracks.into_iter().take(limit).map(|track| track.pack()).collect();

    let body = wincode::serialize(&ListTracksByTapeResponse { tracks, next_cursor })
        .map_err(|error| RouteError::Internal(format!("serialize list tracks response: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

fn store_error(error: impl core::fmt::Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

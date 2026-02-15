//! Inconsistency attestation handler.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use store::Store;
use tape_core::types::EpochNumber;
use tape_node_api::{InconsistencyRequest, BlsInconsistencyResponse, BINARY_CONTENT};
use tape_store::ops::{MetaOps, TrackOps};

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// POST /v1/tracks/:track_id/inconsistency — attest data inconsistency.
pub async fn post_inconsistency<S: Store>(
    State(state): State<AppState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let track_address = super::status::parse_track_address(&track_id)?;

    let request: InconsistencyRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("inconsistency request: {e}")))?;

    // Verify the track exists and the computed root mismatches
    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let local_root = track_info.commitment_root();
    if <[u8; 32]>::from(local_root) == <[u8; 32]>::from(request.computed_root) {
        return Err(ApiError::BadRequest("roots match, no inconsistency".into()));
    }

    let epoch = state
        .context
        .store
        .get_current_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .unwrap_or(EpochNumber(0));

    // BLS sign the inconsistency attestation
    // Use the certify message format with the requester's root
    let msg = tape_core::cert::track::CertifyMessage::new(
        epoch,
        track_address.0,
        request.computed_root.into(),
    );
    let sig = state
        .context
        .bls_keypair
        .sign(&msg.to_bytes())
        .map_err(|e| ApiError::InternalError(format!("bls sign: {e:?}")))?;

    let resp = BlsInconsistencyResponse {
        signature: sig.0 .0,
        node_id: 0,
        member_index: 0,
        epoch: epoch.0,
    };

    let bytes =
        wincode::serialize(&resp).map_err(|e| ApiError::InternalError(e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

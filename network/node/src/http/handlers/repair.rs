//! Repair (sub-chunk extraction) handler.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use tape_protocol::Api;
use store::Store;
use tape_core::encoding::EncodingType;
use tape_protocol::api::{RepairRequest, BINARY_CONTENT};
use tape_slicer::ClayCoder;
use tape_store::ops::{SliceOps, TrackOps};

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// POST /v1/tracks/:track_id/repair — extract sub-chunks for repair.
pub async fn post_repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!(track_id = %track_id, payload_bytes = body.len(), "http post_repair start");
    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::BadRequest(format!("repair request: {e}")))?;

    let track_address = super::status::parse_track_address(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    // Only Clay encoding supports sub-chunk repair
    let profile = track_info.profile();
    let encoding_type = profile
        .encoding_type()
        .ok_or_else(|| ApiError::BadRequest("unknown encoding type".into()))?;

    if encoding_type != EncodingType::Clay {
        return Err(ApiError::BadRequest("repair only supported for Clay encoding".into()));
    }

    let clay_params = profile.clay_params();
    let coder = ClayCoder::from_params(clay_params);
    let alpha = coder.alpha();

    // Load the helper slice
    let helper_spool = request.helper_spool;

    let slice_data = state
        .context
        .store
        .get_slice(helper_spool, track_address)
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    let chunk_size = coder.track_chunk_size(
        track_info.stripe_size as usize,
        track_info.original_size as usize,
    );
    if chunk_size == 0 || alpha == 0 {
        return Err(ApiError::BadRequest("invalid encoding parameters".into()));
    }
    let sub_chunk_size = chunk_size / alpha;

    // Extract requested sub-chunks
    let mut out = Vec::new();

    for stripe_req in &request.stripes {
        let stripe_offset = stripe_req.stripe as usize * chunk_size;
        let chunk = slice_data
            .get(stripe_offset..stripe_offset + chunk_size)
            .ok_or_else(|| ApiError::BadRequest("stripe out of bounds".into()))?;

        for &sc_idx in &stripe_req.sub_chunks {
            let start = sc_idx as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            let sc = chunk
                .get(start..end)
                .ok_or_else(|| ApiError::BadRequest("sub-chunk out of bounds".into()))?;
            out.extend_from_slice(sc);
        }
    }

    tracing::trace!(
        track_id = %track_id,
        output_bytes = out.len(),
        stripes = request.stripes.len(),
        "http post_repair success"
    );

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, BINARY_CONTENT)],
        out,
    ))
}

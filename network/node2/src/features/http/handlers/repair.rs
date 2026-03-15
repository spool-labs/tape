use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::encoding::EncodingType;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, RepairRequest};
use tape_slicer::ClayCoder;
use tape_store::ops::{SliceOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::helpers::{deserialize_body, parse_track_key, store_error};
use crate::features::http::state::AppState;

pub async fn repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: RepairRequest = deserialize_body(&body, "repair request")?;
    let (_, track_key) = parse_track_key(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let profile = track_info.profile();
    let encoding = profile
        .encoding_type()
        .ok_or_else(|| RouteError::BadRequest("unknown encoding type".into()))?;

    if encoding != EncodingType::Clay {
        return Err(RouteError::BadRequest(
            "repair only supported for Clay encoding".into(),
        ));
    }

    let helper_slice = state
        .context
        .store
        .get_slice(request.helper_spool, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let coder = ClayCoder::from_params(profile.clay_params());
    let chunk_size = coder.track_chunk_size(
        track_info.stripe_size as usize,
        track_info.original_size as usize,
    );
    let sub_chunk_size = chunk_size
        .checked_div(coder.alpha())
        .ok_or_else(|| RouteError::BadRequest("invalid repair geometry".into()))?;

    let mut output = Vec::new();
    for stripe in &request.stripes {
        let stripe_offset = stripe.stripe as usize * chunk_size;
        let chunk = helper_slice
            .get(stripe_offset..stripe_offset + chunk_size)
            .ok_or_else(|| RouteError::BadRequest("stripe out of bounds".into()))?;

        for &sub_chunk in &stripe.sub_chunks {
            let start = sub_chunk as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            let data = chunk
                .get(start..end)
                .ok_or_else(|| RouteError::BadRequest("sub-chunk out of bounds".into()))?;
            output.extend_from_slice(data);
        }
    }

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        output,
    ))
}

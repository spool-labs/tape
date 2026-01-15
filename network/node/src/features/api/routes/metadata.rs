//! Track metadata handlers.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_metrics::OperationTimer;

use crate::features::api::ApiError;
use crate::features::storage::service::TrackInfo;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Retrieve track info from storage
    match state.service.get_track_info(track_address) {
        Ok(Some(info)) => {
            let response = serde_json::json!({
                "commitment_hash": hex::encode(info.commitment_hash.0),
                "certified_epoch": info.certified_epoch.0,
                "slice_count": info.slice_count
            });

            state
                .metrics
                .record_request("get_metadata", "success", timer.elapsed_secs());

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&response).unwrap_or_default(),
            )
                .into_response())
        }
        Ok(None) => {
            state
                .metrics
                .record_request("get_metadata", "not_found", timer.elapsed_secs());
            Err(ApiError::TrackNotFound)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track metadata");
            state
                .metrics
                .record_request("get_metadata", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

/// PUT /v1/tracks/:track_id/metadata
pub async fn put_metadata<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Parse JSON body to extract commitment_hash
    let json: serde_json::Value = serde_json::from_slice(&body).map_err(|e| {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid JSON: {}", e))
    })?;

    let commitment_hex = json
        .get("commitment_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            state
                .metrics
                .record_request("put_metadata", "error", timer.elapsed_secs());
            ApiError::InvalidBody("missing commitment_hash field".into())
        })?;

    // Decode hex to bytes
    let commitment_bytes = hex::decode(commitment_hex).map_err(|e| {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid hex in commitment_hash: {}", e))
    })?;

    // Validate length is 32 bytes
    if commitment_bytes.len() != 32 {
        state
            .metrics
            .record_request("put_metadata", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidBody(format!(
            "commitment_hash must be 32 bytes, got {}",
            commitment_bytes.len()
        )));
    }

    // Convert to Hash type
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&commitment_bytes);
    let commitment_hash = Hash(hash_bytes);

    // Create TrackInfo with commitment_hash, certified_epoch=0, slice_count=0
    let info = TrackInfo {
        commitment_hash,
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    };

    // Store track metadata
    match state.service.put_track_info(track_address, info) {
        Ok(()) => {
            state
                .metrics
                .record_request("put_metadata", "success", timer.elapsed_secs());
            Ok(StatusCode::CREATED.into_response())
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to put track metadata");
            state
                .metrics
                .record_request("put_metadata", "error", timer.elapsed_secs());
            Err(ApiError::Storage(e.to_string()))
        }
    }
}

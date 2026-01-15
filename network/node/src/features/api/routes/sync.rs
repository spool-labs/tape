//! Spool synchronization handlers.

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_crypto::Pubkey;
use tape_metrics::OperationTimer;

use crate::features::api::ApiError;
use crate::features::spool_sync::{
    track_id_from_pubkey, SignedSyncRequest, SyncSlice, SyncSpoolRequest, SyncSpoolResponse,
};
use tape_crypto::ed25519::sig_verify;
use tape_store::ops::SliceOps;

use super::ApiState;

/// POST /v1/migrate/sync_spool
///
/// Node-to-node spool synchronization endpoint.
/// Accepts a signed request and returns slices for the requested spool.
pub async fn sync_spool<S: Store>(
    State(state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // 1. Deserialize SignedSyncRequest
    let signed_request: SignedSyncRequest = serde_json::from_slice(&body).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::InvalidBody(format!("invalid sync request: {}", e))
    })?;

    // 2. Verify Ed25519 signature over the serialized request
    let request_bytes = serde_json::to_vec(&signed_request.request).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Internal(format!("request serialization failed: {}", e))
    })?;

    sig_verify(
        &signed_request.signer_pubkey,
        &signed_request.signature,
        &request_bytes,
    )
    .map_err(|_| {
        state
            .metrics
            .record_request("sync_spool", "unauthorized", timer.elapsed_secs());
        ApiError::Unauthorized
    })?;

    // 3. Extract request details
    let (spool_idx, starting_track, batch_size) = match signed_request.request {
        SyncSpoolRequest::V1(ref v1) => {
            (v1.spool_index, v1.starting_track_id.clone(), v1.batch_size)
        }
    };

    // 4. Get all slices for the requested spool
    let all_slices = state.service.store.get_spool_slices(spool_idx).map_err(|e| {
        tracing::error!(spool_idx, error = %e, "Failed to get spool slices");
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Storage(e.to_string())
    })?;

    // 5. Build response with pagination
    let mut result_slices = Vec::new();
    let mut found_start = starting_track.is_empty();

    for (track_pubkey, meta) in all_slices {
        // Convert store Pubkey to base58 track ID
        let track_address = Pubkey::new_from_array(track_pubkey.0);
        let track_id = track_id_from_pubkey(&track_address);

        // Skip until we find the starting track (for pagination)
        if !found_start {
            if track_id == starting_track {
                found_start = true;
            }
            continue;
        }

        // Get the actual slice data
        let (data, _) = state
            .service
            .get_slice(spool_idx, track_address)
            .map_err(|e| {
                tracing::error!(spool_idx, track = %track_id, error = %e, "Failed to get slice data");
                state
                    .metrics
                    .record_request("sync_spool", "error", timer.elapsed_secs());
                ApiError::Storage(e.to_string())
            })?
            .ok_or_else(|| {
                // Slice disappeared between listing and fetching
                state
                    .metrics
                    .record_request("sync_spool", "error", timer.elapsed_secs());
                ApiError::Storage("slice disappeared during enumeration".into())
            })?;

        result_slices.push(SyncSlice {
            track_id,
            slice_index: spool_idx,
            data,
            leaf_hash: meta.leaf_hash,
            merkle_proof: meta.merkle_proof,
        });

        // Respect batch size limit
        if result_slices.len() >= batch_size {
            break;
        }
    }

    // 6. Build and return response
    let response = SyncSpoolResponse::new_v1(result_slices);
    let response_bytes = serde_json::to_vec(&response).map_err(|e| {
        state
            .metrics
            .record_request("sync_spool", "error", timer.elapsed_secs());
        ApiError::Internal(format!("response serialization failed: {}", e))
    })?;

    state
        .metrics
        .record_request("sync_spool", "success", timer.elapsed_secs());

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        response_bytes,
    )
        .into_response())
}

//! Spool synchronization handlers.

use axum::{
    body::Bytes,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_crypto::merkle::hash_leaf;
use tape_node_api::MERKLE_HEIGHT;
use tape_store::ops::SliceOps;
use tracing::{debug, warn};

use crate::features::api::ApiError;
use crate::features::spool_sync::{
    SyncSlice, SyncSpoolRequest, SyncSpoolResponse, track_id_from_pubkey, track_id_to_pubkey,
};
use tape_crypto::Hash;

use super::ApiState;

/// POST /v1/migrate/sync_spool
///
/// Node-to-node spool synchronization endpoint.
/// Accepts a JSON-encoded SyncSpoolRequest and returns slice data for the requested spool.
pub async fn sync_spool<S: Store>(
    State(state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request: SyncSpoolRequest = serde_json::from_slice(&body)
        .map_err(|e| {
            warn!(error = %e, "failed to deserialize sync_spool request");
            ApiError::InvalidBody(e.to_string())
        })?;

    let (spool_index, starting_track_id, batch_size, _epoch) = match &request {
        SyncSpoolRequest::V1(v1) => (
            v1.spool_index,
            &v1.starting_track_id,
            v1.batch_size,
            v1.epoch,
        ),
    };

    debug!(
        spool = spool_index,
        batch_size,
        "sync_spool request"
    );

    if !state.control_plane.owns_spool(spool_index) {
        warn!(spool = spool_index, "sync_spool request for unowned spool");
        return Err(ApiError::NotFound);
    }

    let after_track = if starting_track_id.is_empty() {
        None
    } else {
        Some(
            track_id_to_pubkey(starting_track_id)
                .map(|p| p.into())
                .map_err(|_| ApiError::InvalidTrackId)?,
        )
    };

    let slices_data = state
        .service
        .store
        .iter_slices_by_spool_from(spool_index, after_track, batch_size)
        .map_err(|e| {
            warn!(error = %e, "iter_slices_by_spool_from failed");
            ApiError::Storage(e.to_string())
        })?;

    let slices: Vec<SyncSlice> = slices_data
        .into_iter()
        .map(|(track_pubkey, data)| {
            let leaf = hash_leaf(&data);
            SyncSlice {
                track_id: track_id_from_pubkey(&track_pubkey.into()),
                slice_index: spool_index,
                data,
                leaf_hash: leaf,
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            }
        })
        .collect();

    let response = SyncSpoolResponse::new_v1(slices);
    let response_bytes = serde_json::to_vec(&response).map_err(|e| {
        warn!(error = %e, "failed to serialize sync_spool response");
        ApiError::Serialization(e.to_string())
    })?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        response_bytes,
    )
        .into_response())
}

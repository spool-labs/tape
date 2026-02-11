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
use tape_store::types::Pubkey;
use tracing::{debug, warn};

use crate::features::api::ApiError;
use crate::features::spool_sync::{SyncSlice, SyncSpoolRequest, SyncSpoolResponse};
use tape_crypto::Hash;

use super::ApiState;

/// POST /v1/migrate/sync_spool
///
/// Node-to-node spool synchronization endpoint.
/// Accepts a wincode-encoded SyncSpoolRequest and returns slice data for the requested spool.
pub async fn sync_spool<S: Store>(
    State(state): State<ApiState<S>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request: SyncSpoolRequest = wincode::deserialize(&body)
        .map_err(|e| {
            warn!(error = %e, "failed to deserialize sync_spool request");
            ApiError::InvalidBody(e.to_string())
        })?;

    let (request_spool_index, starting_track, batch_size, request_epoch) = match &request {
        SyncSpoolRequest::V1(v1) => (
            v1.spool_index,
            v1.starting_track,
            v1.batch_size as usize,
            v1.epoch,
        ),
    };

    debug!(
        spool = request_spool_index,
        batch_size,
        "sync_spool request"
    );

    // TODO(auth): Authenticate the requester via TLS certificate pinning.
    //
    // Each committee member registers a TLS public key in NodeInfo.tls_pubkey
    // (tape_store::types::NodeInfo). The planned auth flow:
    // 1. Node-to-node connections use mutual TLS
    // 2. Server extracts the client's TLS public key from the connection
    // 3. Server verifies the key belongs to a current committee member
    // 4. Per-request signatures are unnecessary — TLS provides authentication
    //
    // Until TLS pinning is implemented, this endpoint relies on spool ownership
    // and epoch validation only. Do NOT add per-request signature verification
    // as a stopgap — it would be redundant once TLS pinning lands.

    if !state.control_plane.owns_spool(request_spool_index) {
        warn!(spool = request_spool_index, "sync_spool request for unowned spool");
        return Err(ApiError::NotFound);
    }

    // Validate epoch: must be current or previous
    let current_epoch = state.control_plane.current_epoch();
    let diff = current_epoch.as_u64().saturating_sub(request_epoch.as_u64());
    if request_epoch > current_epoch || diff > 1 {
        warn!(
            spool = %request_spool_index,
            request_epoch = request_epoch.as_u64(),
            current_epoch = current_epoch.as_u64(),
            "sync_spool request with invalid epoch"
        );
        return Err(ApiError::InvalidBody("epoch out of range".into()));
    }

    let after_track: Option<Pubkey> = if starting_track == Pubkey::default() {
        None
    } else {
        Some(starting_track)
    };

    let slices_data = state
        .service
        .store
        .iter_slices_by_spool_from(request_spool_index, after_track, batch_size)
        .map_err(|e| {
            warn!(error = %e, "iter_slices_by_spool_from failed");
            ApiError::Storage(e.to_string())
        })?;

    let slices: Vec<SyncSlice> = slices_data
        .into_iter()
        .map(|(track_pubkey, data)| {
            let leaf = hash_leaf(&data);
            SyncSlice {
                track_address: track_pubkey,
                slice_index: request_spool_index,
                data,
                leaf_hash: leaf,
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            }
        })
        .collect();

    let response = SyncSpoolResponse::new_v1(slices);
    let response_bytes = wincode::serialize(&response).map_err(|e| {
        warn!(error = %e, "failed to serialize sync_spool response");
        ApiError::Serialization(e.to_string())
    })?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        response_bytes,
    )
        .into_response())
}

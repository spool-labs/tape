//! BLS signature handlers for track certification.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_metrics::OperationTimer;
use tape_node_api::SignResponse;

use crate::features::api::ApiError;
use crate::features::storage::service::MERKLE_HEIGHT;
use tape_core::cert::CertifyMessage;
use tape_crypto::merkle::verify_proof;

use super::{parse_track_id, ApiState};

/// GET /v1/tracks/:track_id/sign
///
/// Returns a BLS signature over the track address for certification.
/// Returns 404 if the node doesn't have any slice data for the track.
/// Returns 403 if the node is not in the current committee.
pub async fn get_sign<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Parse track_id to Pubkey (base58)
    let track_address = parse_track_id(&track_id)?;

    // Check if node is in committee
    if !state.control_plane.is_in_committee() {
        state
            .metrics
            .record_request("get_sign", "forbidden", timer.elapsed_secs());
        return Err(ApiError::Unauthorized);
    }

    // Get our member index for the bitmap
    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system.committee.index_of(&node_id)
        .ok_or_else(|| {
            state
                .metrics
                .record_request("get_sign", "error", timer.elapsed_secs());
            ApiError::Internal("Node is in committee but index_of failed".to_string())
        })? as u8;

    // Get track metadata (contains commitment_hash for verification)
    let track_info = match state.service.get_track_info(track_address) {
        Ok(Some(info)) => info,
        Ok(None) => {
            state
                .metrics
                .record_request("get_sign", "not_found", timer.elapsed_secs());
            return Err(ApiError::TrackNotFound);
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to get track info for signing");
            state
                .metrics
                .record_request("get_sign", "error", timer.elapsed_secs());
            return Err(ApiError::Storage(e.to_string()));
        }
    };

    // Get our assigned spools and verify we have valid slice data for each
    let our_spools = state.control_plane.get_our_spools();
    for spool_idx in &our_spools {
        // Check if we have this slice
        let (data, meta) = match state.service.get_slice(*spool_idx, track_address) {
            Ok(Some((d, m))) => (d, m),
            Ok(None) => {
                tracing::warn!(
                    track = %track_id,
                    spool = spool_idx,
                    "Missing slice data for owned spool"
                );
                state
                    .metrics
                    .record_request("get_sign", "incomplete", timer.elapsed_secs());
                return Err(ApiError::IncompleteSliceData);
            }
            Err(e) => {
                tracing::error!(error = %e, spool = spool_idx, "Failed to get slice for signing");
                state
                    .metrics
                    .record_request("get_sign", "error", timer.elapsed_secs());
                return Err(ApiError::Storage(e.to_string()));
            }
        };

        // Verify merkle proof against commitment_hash
        let is_valid = verify_proof(
            &data,
            &track_info.commitment_hash,
            &meta.merkle_proof,
            *spool_idx as u64,
            MERKLE_HEIGHT,
        );
        if !is_valid {
            tracing::warn!(
                track = %track_id,
                spool = spool_idx,
                "Merkle proof verification failed for slice"
            );
            state
                .metrics
                .record_request("get_sign", "merkle_failed", timer.elapsed_secs());
            return Err(ApiError::MerkleVerificationFailed);
        }
    }

    // Build certification message with domain separation and epoch binding
    // Format: DOMAIN_TAG (8) || EPOCH (8 LE) || TRACK_ADDRESS (32) = 48 bytes
    let current_epoch = state.control_plane.current_epoch();
    let certify_message = CertifyMessage::new(current_epoch, track_address.to_bytes());
    let message_bytes = certify_message.to_bytes();

    let signature = state.bls_keypair.sign(&message_bytes).map_err(|e| {
        tracing::error!(error = ?e, "BLS signing failed");
        state
            .metrics
            .record_request("get_sign", "error", timer.elapsed_secs());
        ApiError::Internal(format!("BLS signing failed: {:?}", e))
    })?;

    // Build response
    let response = SignResponse {
        signature: signature.0.0,
        node_id: node_id.as_u64(),
        member_index,
        epoch: current_epoch.0,
    };

    state
        .metrics
        .record_request("get_sign", "success", timer.elapsed_secs());

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&response).unwrap_or_default(),
    )
        .into_response())
}

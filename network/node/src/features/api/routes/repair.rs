//! Repair and inconsistency handlers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures::stream::{self, StreamExt};
use store::Store;
use tape_core::cert::invalidate::InvalidateMessage;
use tape_core::erasure::{group_start, SPOOL_GROUP_SIZE};
use tape_core::spooler::SpoolIndex;
use tape_node_api::{InconsistencyRequest, InconsistencyResponse, RepairRequest};
use tape_node_client::{NodeClient, NodeClientBuilder};
use tape_slicer::adaptive::pick_stripe_size;
use tape_slicer::clay::ClayCoder;
use tape_slicer::coder::ErasureCoder;
use tape_slicer::merkle_helpers::blob_merkle_root;
use tape_slicer::slicer::Slicer;
use tape_store::ops::CommitteeOps;
use tracing::{debug, warn};

use crate::features::api::ApiError;

use super::{parse_track_id, ApiState};

/// Number of concurrent slice downloads during inconsistency verification.
const VERIFY_DOWNLOAD_CONCURRENCY: usize = 8;

/// POST /v1/tracks/:track_id/repair
///
/// Helper-side handler for bandwidth-optimal repair.
/// Reads the specified slice, extracts the requested sub-chunks per stripe,
/// and returns concatenated bytes.
pub async fn post_repair<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::InvalidBody(format!("RepairRequest: {}", e)))?;

    // Read our slice from local storage
    let slice_data = state
        .service
        .get_slice(request.helper_spool, track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::NotFound)?;

    // Parse slice metadata suffix to determine chunk/sub-chunk sizes
    let metadata = tape_slicer::SliceMetadata::from_slice(&slice_data)
        .map_err(|e| ApiError::Internal(format!("slice metadata: {}", e)))?;

    let total_data_len = slice_data.len().saturating_sub(tape_slicer::SliceMetadata::SIZE);
    let blob_len = metadata.blob_len();
    let stripe_size = metadata.stripe_size();
    let num_stripes = if blob_len == 0 {
        1
    } else {
        (blob_len + stripe_size - 1) / stripe_size
    };

    if total_data_len == 0 || num_stripes == 0 {
        return Err(ApiError::Internal("invalid slice layout".into()));
    }

    let chunk_size = total_data_len / num_stripes;

    // Repair is only supported for Clay-encoded tracks
    let profile = metadata.profile();
    if !profile.is_clay() {
        return Err(ApiError::InvalidBody(
            "repair only supported for Clay encoding".into(),
        ));
    }

    // Compute alpha from the encoding profile's clay parameters
    let clay_params = profile.clay_params();
    let coder = ClayCoder::new(
        clay_params.n() as usize,
        clay_params.k() as usize,
        clay_params.d() as usize,
    );
    let alpha = coder.alpha();

    if alpha == 0 || chunk_size % alpha != 0 {
        return Err(ApiError::Internal(format!(
            "chunk_size ({chunk_size}) not divisible by alpha ({alpha})"
        )));
    }
    let sub_chunk_size = chunk_size / alpha;

    // Extract requested sub-chunks
    let mut out = Vec::new();

    for stripe_req in &request.stripes {
        let stripe_idx = stripe_req.stripe as usize;
        let chunk_offset = stripe_idx * chunk_size;
        let chunk_end = chunk_offset + chunk_size;

        if chunk_end > total_data_len {
            return Err(ApiError::Internal("stripe index out of bounds".into()));
        }

        let chunk = &slice_data[chunk_offset..chunk_end];

        for &sc_idx in &stripe_req.sub_chunks {
            let start = sc_idx as usize * sub_chunk_size;
            let end = start + sub_chunk_size;
            if end > chunk.len() {
                return Err(ApiError::Internal("sub-chunk index out of bounds".into()));
            }
            out.extend_from_slice(&chunk[start..end]);
        }
    }

    debug!(
        track = %track_address,
        helper_spool = request.helper_spool,
        lost_slice = request.lost_slice,
        response_bytes = out.len(),
        "extracted repair sub-chunks"
    );

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        out,
    )
        .into_response())
}

/// POST /v1/tracks/:track_id/inconsistency
///
/// Independently verify an inconsistency and return a BLS attestation.
///
/// The handler performs full recovery (download k slices, decode, re-encode)
/// to independently verify that the erasure-coded commitment doesn't match.
/// Only signs if independent computation agrees with the requester's claim.
pub async fn post_inconsistency<S: Store>(
    State(state): State<ApiState<S>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<Json<InconsistencyResponse>, ApiError> {
    let track_address = parse_track_id(&track_id)?;

    let request: InconsistencyRequest = wincode::deserialize(&body)
        .map_err(|e| ApiError::InvalidBody(format!("InconsistencyRequest: {}", e)))?;

    // Must be in committee to attest
    if !state.control_plane.is_in_committee() {
        return Err(ApiError::Unauthorized);
    }

    // Load track metadata
    let track_info = state
        .service
        .get_track(track_address)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::TrackNotFound)?;

    let on_chain_root = track_info.commitment_root();

    // Quick reject: no inconsistency if computed_root matches on-chain
    if request.computed_root == on_chain_root {
        return Err(ApiError::InvalidBody(
            "computed_root matches on-chain commitment".into(),
        ));
    }

    // Independent verification: download k slices, decode, re-encode, compute root
    let computed_root = independently_verify(&state, track_address, &track_info).await?;

    // Our computed root must match the requester's claim
    if computed_root != request.computed_root {
        return Err(ApiError::InvalidBody(
            "independent verification produced different root".into(),
        ));
    }

    // Our computed root must differ from on-chain (confirms inconsistency)
    if computed_root == on_chain_root {
        return Err(ApiError::Internal(
            "independent verification matches on-chain commitment".into(),
        ));
    }

    // Build and sign the invalidation message
    let epoch = state.control_plane.current_epoch();
    let invalidate_message = InvalidateMessage::new(
        epoch,
        track_address.to_bytes(),
        computed_root.0,
    );
    let message = invalidate_message.to_bytes();

    let signature = state
        .bls_keypair
        .sign(&message)
        .map_err(|e| ApiError::Internal(format!("BLS signing failed: {:?}", e)))?;

    let node_id = state.control_plane.our_node_id();
    let system = state.control_plane.get_system();
    let member_index = system
        .committee
        .index_of(&node_id)
        .ok_or(ApiError::Unauthorized)?;

    debug!(
        track = %track_address,
        node_id = node_id.as_u64(),
        "signed inconsistency attestation"
    );

    Ok(Json(InconsistencyResponse {
        signature: (signature.0).0,
        node_id: node_id.as_u64(),
        member_index: member_index as u8,
        epoch: epoch.as_u64(),
    }))
}

/// Download k slices, decode, re-encode, and return the computed merkle root.
async fn independently_verify<S: Store>(
    state: &ApiState<S>,
    track_address: tape_crypto::Pubkey,
    track_info: &tape_store::types::TrackInfo,
) -> Result<tape_crypto::Hash, ApiError> {
    let profile = track_info.profile();
    let clay_params = profile.clay_params();
    let k = clay_params.k() as usize;
    let blob_len = track_info.original_size as usize;

    let spool_group = track_info.spool_group;
    let start = group_start(spool_group);

    // Resolve group members from locally-cached committee
    let epoch = state.control_plane.current_epoch();
    let committee = state
        .service
        .store
        .get_committee(epoch)
        .map_err(|e| ApiError::Storage(e.to_string()))?
        .ok_or(ApiError::Internal("no committee available".into()))?;

    let mut spool_to_node: HashMap<SpoolIndex, usize> = HashMap::new();
    for (idx, member) in committee.iter().enumerate() {
        for &spool in &member.spools {
            spool_to_node.insert(spool, idx);
        }
    }

    let mut available: Vec<(usize, SpoolIndex, NodeClient)> = Vec::new();
    for position in 0..SPOOL_GROUP_SIZE {
        let spool_idx = start + position as SpoolIndex;
        if let Some(&member_idx) = spool_to_node.get(&spool_idx) {
            let member = &committee[member_idx];
            let addr = match member.network_address.to_socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };
            match NodeClientBuilder::new()
                .accept_invalid_certs(state.insecure)
                .build(&addr.to_string())
            {
                Ok(client) => available.push((position, spool_idx, client)),
                Err(_) => continue,
            }
        }
    }

    if available.len() < k {
        return Err(ApiError::Internal(format!(
            "not enough helpers: needed {k}, available {}",
            available.len()
        )));
    }

    let track_id_str = track_address.to_string();
    let collected_count = Arc::new(AtomicUsize::new(0));

    let download_results: Vec<(usize, Result<Vec<u8>, String>)> = stream::iter(
        available.into_iter(),
    )
    .map(|(position, spool_idx, client)| {
        let tid = track_id_str.clone();
        let collected = Arc::clone(&collected_count);
        async move {
            if collected.load(Ordering::Relaxed) >= k {
                return (position, Err("skipped".into()));
            }
            let result = client
                .get_slice(&tid, spool_idx)
                .await
                .map_err(|e| e.to_string());
            if result.is_ok() {
                collected.fetch_add(1, Ordering::Relaxed);
            }
            (position, result)
        }
    })
    .buffer_unordered(VERIFY_DOWNLOAD_CONCURRENCY)
    .collect()
    .await;

    let mut collected_slices: Vec<(usize, Vec<u8>)> = Vec::new();
    for (position, result) in download_results {
        match result {
            Ok(data) => {
                if !track_info.commitment.is_empty()
                    && !track_info.verify_slice(position, &data)
                {
                    warn!(position, "downloaded slice failed leaf verification, skipping");
                    continue;
                }
                collected_slices.push((position, data));
                if collected_slices.len() >= k {
                    break;
                }
            }
            Err(_) => continue,
        }
    }

    if collected_slices.len() < k {
        return Err(ApiError::Internal(format!(
            "not enough slices downloaded: needed {k}, got {}",
            collected_slices.len()
        )));
    }

    let stripe_size = pick_stripe_size(blob_len);

    let computed_root = tokio::task::spawn_blocking(move || {
        let coder = ClayCoder::from_params(clay_params);
        let mut slicer = Slicer::with_profile(coder, stripe_size, true, profile);

        let chunks: Vec<(usize, &[u8])> = collected_slices
            .iter()
            .map(|(pos, data)| (*pos, data.as_slice()))
            .collect();

        let original = slicer
            .decode(&chunks)
            .map_err(|e| ApiError::Internal(format!("decode failed: {}", e)))?;

        let all_slices = slicer
            .encode(&original)
            .map_err(|e| ApiError::Internal(format!("re-encode failed: {}", e)))?;

        Ok::<_, ApiError>(blob_merkle_root(&all_slices))
    })
    .await
    .map_err(|e| ApiError::Internal(format!("spawn_blocking panicked: {}", e)))??;

    Ok(computed_root)
}

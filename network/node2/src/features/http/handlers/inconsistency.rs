use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use bytemuck::cast;

use rpc::Rpc;
use store::Store;
use tape_api::program::tapedrive::CommitteeBitmap;
use tape_core::bft::is_supermajority;
use tape_core::cert::InvalidateMessage;
use tape_core::erasure::{SPOOL_GROUP_SIZE, group_for_spool};
use tape_core::types::EpochNumber;
use tape_crypto::Pubkey;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, BlsInconsistencyResponse, InconsistencyProof, InconsistencyRequest,
};
use tape_store::ops::TrackOps;
use tape_store::types::TrackInfo;

use crate::features::http::error::RouteError;
use crate::features::http::helpers::{deserialize_body, parse_track_key, store_error};
use crate::features::http::state::{AppState, current_epoch};

pub async fn invalidate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: InconsistencyRequest = deserialize_body(&body, "inconsistency request")?;
    let epoch = current_epoch(&state)?;
    let (track, track_key) = parse_track_key(&track_id)?;

    let track_info = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    if track_info.commitment_root() == request.proof.observed_root {
        return Err(RouteError::BadRequest("roots match, no inconsistency".into()));
    }

    verify_inconsistency_proof(&state, &track_info, track, epoch, &request.proof)?;

    let message = InvalidateMessage::new(epoch, track.to_bytes(), request.proof.observed_root.into());
    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsInconsistencyResponse {
        signature,
        node_id: state.context.node_id(),
        epoch,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize invalidate response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn verify_inconsistency_proof<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    track_info: &TrackInfo,
    track: Pubkey,
    epoch: EpochNumber,
    proof: &InconsistencyProof,
) -> Result<(), RouteError> {
    let protocol = state.context.state();
    if protocol.epoch != epoch || protocol.committee.is_empty() {
        return Err(RouteError::BadRequest("committee missing".into()));
    }

    let bitmap: CommitteeBitmap = cast(proof.committee_bitmap);
    let signer_indices = bitmap.indices(protocol.committee.len());
    if signer_indices.is_empty() {
        return Err(RouteError::BadRequest(
            "inconsistency proof has no signers".into(),
        ));
    }

    let mut signer_weight = 0u64;
    let mut signer_pubkeys = Vec::with_capacity(signer_indices.len());

    for signer_index in signer_indices {
        let member = protocol
            .committee
            .get(signer_index)
            .ok_or_else(|| RouteError::BadRequest("unknown signer in bitmap".into()))?;

        signer_weight += protocol
            .member_spools(signer_index)
            .iter()
            .filter(|&&spool| group_for_spool(spool) == track_info.spool_group)
            .count() as u64;
        signer_pubkeys.push(member.key);
    }

    if !is_supermajority(signer_weight, SPOOL_GROUP_SIZE as u64) {
        return Err(RouteError::BadRequest(
            "inconsistency proof lacks quorum for spool group".into(),
        ));
    }

    let message = InvalidateMessage::new(epoch, track.to_bytes(), proof.observed_root.into());
    proof
        .signature
        .verify_aggregate(message.to_bytes(), &signer_pubkeys)
        .map_err(|_| RouteError::InvalidSignature)?;

    Ok(())
}

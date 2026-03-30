use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::cert::track::CertifyMessage;
use tape_core::erasure::group_for_spool;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkIndex, EpochNumber};
use tape_crypto::Pubkey;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse, SnapshotSignatureSubmission};
use tape_store::ops::{MetaOps, SliceOps, TrackOps};
use tape_store::types::{Pubkey as StorePubkey, SnapshotPartialSignature};

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn certify<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
) -> Result<impl IntoResponse, RouteError> {

    let epoch = current_epoch(&state)?;
    let track: Pubkey = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track_key: StorePubkey = track.into();

    let track = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    if !track.is_blob() {
        return Err(RouteError::BadRequest(
            "raw tracks do not require certification".into(),
        ));
    }

    let protocol = state.context.state();
    let has_local_slice = protocol
        .group_peers(track.spool_group)
        .into_iter()
        .filter(|(_, node_id)| *node_id == state.context.node_id())
        .any(|(spool_id, _)| {
            state
                .context
                .store
                .has_slice(spool_id, track_key)
                .unwrap_or(false)
        });

    if !has_local_slice {
        return Err(RouteError::NotFound);
    }

    let message = CertifyMessage::new(epoch, track.get_hash().into());

    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsSignResponse {
        signature,
        node_id: state.context.node_id(),
        epoch,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize certify response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

pub async fn put_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((epoch, chunk_index)): Path<(u64, u64)>,
    body: Bytes,
) -> Result<StatusCode, RouteError> {

    let request: SnapshotSignatureSubmission = wincode::deserialize(&body).map_err(|error| {
        RouteError::BadRequest(format!("snapshot signature submission: {error}"))
    })?;
    let epoch = EpochNumber(epoch);

    if request.epoch != epoch {
        return Err(RouteError::BadRequest("epoch mismatch".into()));
    }

    let protocol = state.context.state();
    if protocol.epoch != epoch || protocol.committee.is_empty() {
        return Err(RouteError::NotFound);
    }

    let member_index = request.member_index as usize;
    let member = protocol
        .committee
        .get(member_index)
        .ok_or_else(|| RouteError::BadRequest("unknown member index".into()))?;

    let group = SpoolGroup(chunk_index);
    if !protocol
        .member_spools(member_index)
        .iter()
        .any(|&spool| group_for_spool(spool) == group)
    {
        return Err(RouteError::NotInCommittee);
    }

    let chunk_index = ChunkIndex(chunk_index);
    let commitment = state
        .context
        .store
        .get_snapshot_commitment(epoch, chunk_index)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let message = SnapshotMessage::new(epoch, commitment.into()).to_bytes();
    if request
        .signature
        .verify_aggregate(message, &[member.key])
        .is_err()
    {
        return Err(RouteError::InvalidSignature);
    }

    state
        .context
        .store
        .set_snapshot_partial_signature(
            epoch,
            group.0,
            SnapshotPartialSignature {
                member_index: request.member_index,
                signature: request.signature,
                epoch: epoch.0,
            },
        )
        .map_err(store_error)?;

    Ok(StatusCode::OK)
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

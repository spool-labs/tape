//! Answer a storage challenge for one sample leaf of one slice.
//!
//! The challenger draws the coordinates from a finalized block hash, so this
//! handler is a plain read: prove the leaf at the index it was asked for. It
//! reads the slice bytes rather than any cached digest, which is the point. An
//! owner that discarded the slice cannot answer, and one that kept only a proof
//! cannot either, because the response carries the leaf itself.

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SampleProofPayload};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps};
use tracing::trace;

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn get_sample<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id, sub_leaf)): Path<(String, SpoolIndex, u64)>,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, spool_id = %spool_id, sub_leaf, "http get_sample start");

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let Some(BlobData::Coded(encoding)) = state
        .context
        .store
        .get_track_data(track)
        .map_err(store_error)?
    else {
        // An inline track has no slices to sample, so it is not a coded challenge.
        return Err(RouteError::NotFound);
    };

    let slice = state
        .context
        .store
        .get_slice(spool_id, track)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let sub_leaf = usize::try_from(sub_leaf)
        .map_err(|_| RouteError::BadRequest("sub-leaf index out of range".into()))?;

    let proof = encoding
        .prove_sub_leaf(spool_id, sub_leaf, &slice)
        .ok_or_else(|| RouteError::BadRequest("sub-leaf index past the slice".into()))?;

    let body = wincode::serialize(&SampleProofPayload::from(proof))
        .map_err(|error| RouteError::Internal(format!("encode sample proof: {error}")))?;

    Ok(([(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

fn store_error(error: impl std::fmt::Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

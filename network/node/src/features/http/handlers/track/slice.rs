use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{SLICE_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use tape_core::track::data::TrackData;
use tape_crypto::address::Address;
use tape_crypto::merkle::{hash_leaf, verify_proof};
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SlicePayload};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};
use tracing::{debug, trace};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn get_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
) -> Result<impl IntoResponse, RouteError> {
    trace!(track_id = %track_id, spool_id, "http get_slice start");

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;
    let track_key = track;

    state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let data = state
        .context
        .store
        .get_slice(spool_id, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    state.context.metrics.add_downloaded(data.len() as u64);

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        data,
    ))
}

pub async fn put_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path((track_id, spool_id)): Path<(String, u16)>,
    body: Bytes,
) -> Result<StatusCode, RouteError> {
    trace!(
        track_id = %track_id,
        spool_id,
        payload_bytes = body.len(),
        "http put_slice start"
    );

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    let track_key = track;
    let payload: SlicePayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("slice payload: {error}")))?;

    let in_store = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?;

    let track = state
        .context
        .pending
        .apply_to_track(track_key, in_store)
        .ok_or(RouteError::NotFound)?;

    if !track.is_blob() {
        return Err(RouteError::BadRequest("raw tracks do not accept slices".into()));
    }

    let track_data = match state.context.pending.track_data(track_key) {
        Some(data) => data,
        None => state
            .context
            .store
            .get_track_data(track_key)
            .map_err(store_error)?
            .ok_or(RouteError::NotFound)?,
    };
    let TrackData::Blob(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    if hash_leaf(&payload.data) != payload.leaf_hash {
        return Err(RouteError::BadRequest("leaf hash mismatch".into()));
    }

    let leaf_pos = (spool_id as usize) % SPOOL_GROUP_SIZE;

    if !verify_proof(
        &payload.data,
        &blob.commitment,
        &payload.merkle_proof,
        leaf_pos as u64,
        SLICE_TREE_HEIGHT,
    ) {
        return Err(RouteError::BadRequest("invalid merkle proof".into()));
    }

    let spool_state = state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;
    
    if spool_state.is_locked() {
        return Err(RouteError::NotResponsible);
    }

    let data_len = payload.data.len() as u64;
    state
        .context
        .store
        .put_slice(spool_id, track_key, payload.data)
        .map_err(store_error)?;
    state.context.metrics.add_uploaded(data_len);

    debug!(track_id = %track_id, spool_id, "http put_slice success");

    Ok(StatusCode::OK)
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::prelude::{SpoolState, SpoolStatus};
    use tape_core::snapshot::chunk::snapshot_chunk_key;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_store::ops::{ObjectInfoOps, SpoolOps, TapeOps, TrackDataOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;
    use crate::context::NodeContext;
    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    fn seed_projected_snapshot_track(
        ctx: &NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>,
    ) -> (Address, u16, Vec<u8>) {
        let epoch = EpochNumber(5);

        let group = SpoolGroup(2);
        let track_number = TrackNumber(9);
        let owned_spool = group.spool_at(5);
        let slice_bytes = vec![0xAB; 96];

        let leaves = [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE];
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);
        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_537),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };

        let (snapshot_tape, _) = snapshot_tape_pda(epoch);
        let track_address = track_pda(snapshot_tape, track_number).0;

        ctx.store
            .put_tape(
                snapshot_tape,
                TapeInfo {
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                },
            )
            .expect("seed snapshot tape");

        let track = CompressedTrack {
            tape: snapshot_tape,
            key: snapshot_chunk_key(epoch, group, ChunkNumber(0)),
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");

        ctx.store
            .put_track_data(track_address, TrackData::Blob(blob))
            .expect("seed track data");

        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0),
                },
            )
            .expect("seed object info");
        ctx.store
            .put_slice(owned_spool, track_address, slice_bytes.clone())
            .expect("seed slice");
        ctx.store
            .set_spool_state(
                owned_spool,
                SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            )
            .expect("set spool state");

        (track_address, owned_spool, slice_bytes)
    }

    #[tokio::test]
    async fn serves_slice() {
        let ctx = test_context();
        let (track_address, owned_spool, slice_bytes) = seed_projected_snapshot_track(&ctx);

        let result = get_slice(
            State(AppState {
                context: ctx.clone(),
            }),
            Path((track_address.to_string(), owned_spool)),
        )
        .await;
        let response = match result {
            Ok(response) => response.into_response(),
            Err(_) => panic!("get_slice failed for projected snapshot track"),
        };

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");

        assert_eq!(body.as_ref(), slice_bytes.as_slice());
    }
}

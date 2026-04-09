use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::track::data::TrackData;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, RepairRequest};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;
use crate::features::spool::repair::extract_repair_data;

pub async fn repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Path(track_id): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("repair request: {error}")))?;

    let track: Address = track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))?;

    let track_key = track.into();

    state
        .context
        .store
        .get_spool_state(request.helper_spool)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)?;

    let track = state
        .context
        .store
        .get_track(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    if !track.is_blob() {
        return Err(RouteError::BadRequest("raw tracks do not support repair".into()));
    }

    let track_data = state
        .context
        .store
        .get_track_data(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    let TrackData::Blob(blob) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    let helper_slice = state
        .context
        .store
        .get_slice(request.helper_spool, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let output = extract_repair_data(
        &blob,
        &request.stripes, 
        &helper_slice
    ).map_err(|error| RouteError::BadRequest(error.to_string()))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        output,
    ))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    //! Black-box regression test that runs the repair handler against a
    //! *projected* snapshot track. The slice bytes here come straight out of
    //! the production slicer (matching what the snapshot build path emits),
    //! so this exercises the same Clay metadata + chunk-index path that
    //! `extract_repair_data` walks for normal blob tracks.

    use super::*;
    use axum::body::{to_bytes, Bytes};
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::prelude::{SpoolState, SpoolStatus};
    use tape_core::snapshot::chunk::snapshot_chunk_key;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber,
    };
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_protocol::api::{RepairRequest, StripeSubChunkRequest};
    use tape_slicer::{ErasureCoder, Slicer};
    use tape_store::ops::{ObjectInfoOps, SliceOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    #[tokio::test]
    async fn repairs_projected_snapshot_slice() {
        let ctx = test_context();

        // Build a real Clay-encoded snapshot chunk: 20 slices, each carrying
        // the per-slice metadata suffix that `extract_repair_data` parses.
        let chunk = vec![0xCDu8; 2048];
        let group = SpoolGroup(2);
        let mut slicer = Slicer::clay_default();
        slicer.set_chunk_index(group.0);

        let slices = slicer.encode(&chunk).expect("slicer encode");
        let stripe_size = slicer.stripe_size();
        let stripe_count = (chunk.len() + stripe_size - 1) / stripe_size;

        let leaves: [tape_crypto::Hash; SPOOL_GROUP_SIZE] =
            core::array::from_fn(|i| hash_leaf(&slices[i]));
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        let blob = BlobInfo {
            size: StorageUnits::from_bytes(chunk.len() as u64),
            root: tape_crypto::Hash::from([0x55; 32]),
            commitment,
            profile: slicer.profile(),
            stripe_size: StorageUnits::from_bytes(stripe_size as u64),
            stripe_count: StripeCount(stripe_count as u64),
            leaves,
        };

        // Project the track. We pick spool index 5 within the group as the
        // helper; that maps to slice position 5 inside the slice array.
        let epoch = EpochNumber(5);
        let parent_epoch = EpochNumber(4);
        let track_number = TrackNumber(9);
        let helper_spool = group.spool_at(5);
        let slice_position = group.slice_of(helper_spool).unwrap();
        let helper_slice = slices[slice_position as usize].clone();

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
            .unwrap();

        let track = CompressedTrack {
            tape: snapshot_tape,
            key: snapshot_chunk_key(epoch, group, parent_epoch),
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };

        ctx.store.put_track(track_address, track).unwrap();
        ctx.store
            .put_track_data(track_address, TrackData::Blob(blob))
            .unwrap();

        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0 * 10 + 1),
                },
            )
            .unwrap();
        ctx.store
            .put_slice(helper_spool, track_address, helper_slice)
            .unwrap();
        ctx.store
            .set_spool_state(
                helper_spool,
                SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            )
            .unwrap();

        // Ask for sub-chunk 0 of stripe 0. Any valid (stripe, sub_chunk) pair
        // would do — we just need extract_repair_data to walk the metadata
        // path and return non-empty bytes.
        let request = RepairRequest {
            helper_spool,
            stripes: vec![StripeSubChunkRequest {
                stripe: 0,
                sub_chunks: vec![0],
            }],
        };
        let body = wincode::serialize(&request).unwrap();

        let result = repair(
            State(AppState {
                context: ctx.clone(),
            }),
            Path(track_address.to_string()),
            Bytes::from(body),
        )
        .await;
        let response = match result {
            Ok(response) => response.into_response(),
            Err(_) => panic!("repair failed for projected snapshot track"),
        };

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.expect("body");
        assert!(!bytes.is_empty(), "repair should return at least one sub-chunk");
    }
}

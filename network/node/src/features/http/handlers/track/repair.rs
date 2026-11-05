use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::track::data::BlobData;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, RepairRequest, StripeSubChunkRequest};
use tape_slicer::extract_repair_data as extract_sub_chunks;
use tape_slicer::{
    ClayCoder, HelperRequest, RepairPlan, SliceIndex, SliceMetadata, StripeSubChunks,
};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps, TrackOps};

use crate::features::blacklist::refuses_object;
use crate::features::http::auth::{MaybeActivePeer, StakedPeer};
use crate::features::http::error::RouteError;
use crate::features::http::handlers::track::parse_track;
use crate::features::http::share::check_share_token;
use crate::features::http::peer_identity::PeerIdentity;
use crate::features::http::state::AppState;

pub async fn repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _staked_peer: StakedPeer,
    MaybeActivePeer(active_peer): MaybeActivePeer,
    identity: PeerIdentity,
    Path(track_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {

    let request: RepairRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("repair request: {error}")))?;

    let track_key = parse_track(&track_id)?;

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
    if !track.is_coded() {
        return Err(RouteError::BadRequest("raw tracks do not support repair".into()));
    }

    if refuses_object(
        state.context.store.as_ref(),
        &state.context.blacklist,
        state.context.node_address(),
        state.context.state().epoch(),
        track_key,
        track.tape,
    )
    .map_err(store_error)?
    {
        return Err(RouteError::BlacklistedObject);
    }

    check_share_token(&state, &track, identity, active_peer, &headers)?;

    let track_data = state
        .context
        .store
        .get_track_data(track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;
    let BlobData::Coded(_) = track_data else {
        return Err(RouteError::BadRequest("track data is not blob metadata".into()));
    };

    let helper_slice = state
        .context
        .store
        .get_slice(request.helper_spool, track_key)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    let output = extract_repair_data(
        state.context.params().profile,
        &request.stripes,
        &helper_slice
    ).map_err(|error| RouteError::BadRequest(error.to_string()))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        output,
    ))
}

/// Cut the sub-chunks a repair request asks for out of a helper's slice
pub fn extract_repair_data(
    profile: ErasureCode,
    stripes: &[StripeSubChunkRequest],
    slice_data: &[u8],
) -> Result<Vec<u8>, String> {
    let ErasureCode::Clay(params) = profile else {
        return Err("repair needs a clay network".to_string());
    };
    let coder = ClayCoder::from_params(params);
    let metadata = SliceMetadata::parse(slice_data).map_err(to_text)?;
    let stripe_size = metadata.stripe_size();
    let chunk_size = coder.track_chunk_size(stripe_size, metadata.blob_len());
    let plan = RepairPlan {
        lost: SliceIndex::new(0),
        chunk_size: chunk_size as u64,
        sub_chunk_size: (chunk_size / coder.alpha().max(1)) as u64,
        stripes: Vec::new(),
    };
    let slice = SliceIndex::new(0);
    let request = HelperRequest { slice, stripes: get_sub_chunks(stripes) };
    extract_sub_chunks(slice_data, &plan, &request).map_err(to_text)
}

fn get_sub_chunks(stripes: &[StripeSubChunkRequest]) -> Vec<StripeSubChunks> {
    let mut wanted = Vec::with_capacity(stripes.len());
    for stripe in stripes {
        let sub_chunks = stripe.sub_chunks.clone();
        let stripe = stripe.stripe;
        wanted.push(StripeSubChunks { stripe, sub_chunks });
    }
    wanted
}

fn to_text(error: impl Display) -> String {
    error.to_string()
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Bytes};
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;

    use tape_api::program::tapedrive::{snapshot_tape_pda, TrackAddress};
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};
    use tape_core::erasure::{MAX_GROUP_SIZE, SLICE_TREE_HEIGHT, slice_root};
    use tape_recover::state::SpoolState;
    use tape_snapshot::snapshot_chunk_key;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::{snapshot_tape_number, TapeFlags};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber,
        TrackNumber,
    };
    use tape_crypto::Hash;
    use tape_crypto::address::Address;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::{RepairRequest, StripeSubChunkRequest};
    use tape_slicer::{ErasureCoder, Slicer};
    use tape_store::ops::{ObjectInfoOps, SliceOps, TapeOps, TrackDataOps, TrackOps};
    use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};

    use super::*;
    use crate::features::http::state::AppState;
    use crate::harness::{NodeHarness, TestContext};

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    // repair handler returns sub-chunk bytes for a projected snapshot track
    #[tokio::test]
    async fn returns_sub_chunk() {
        let ctx = test_context().await;

        // Build a real Clay-encoded snapshot chunk: 20 slices, each carrying
        // the per-slice metadata suffix that the repair extractor parses.
        let chunk = vec![0xCDu8; 2048];
        let group = VirtualGroup(2);
        let mut slicer = Slicer::try_from_profile(TAPENET_PROFILE).expect("tapenet slicer");
        slicer.set_chunk_index(ChunkNumber(group.0));

        let slices = slicer.encode(&chunk).expect("slicer encode");
        let stripe_size = slicer.stripe_size();
        let stripe_count = chunk.len().div_ceil(stripe_size);

        let leaves: [Hash; MAX_GROUP_SIZE] =
            core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(chunk.len() as u64),
            commitment,
            stripe_size: StorageUnits::from_bytes(stripe_size as u64),
            stripe_count: StripeCount(stripe_count as u64),
            leaves,
        };

        let epoch = EpochNumber(5);

        let track_number = TrackNumber(9);
        let helper_spool = group.spool_at(5);

        let slice_position = group
            .position_of(helper_spool)
            .expect("helper slice position");
        let helper_slice = slices[slice_position].clone();

        let (snapshot_tape, _) = snapshot_tape_pda(epoch);

        ctx.store
            .put_tape(
                snapshot_tape,
                TapeInfo {
                    id: snapshot_tape_number(epoch),
                    flags: TapeFlags::SYSTEM,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(track_number.0 + 1),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .expect("seed snapshot tape");

        let track = CompressedTrack {
            tape: snapshot_tape,
            tape_number: TapeNumber(1),
            key: snapshot_chunk_key(epoch, group, ChunkNumber(0)),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: blob.size,
            group,
            value_hash: blob.get_hash(),
        };
        let track_address = track.address();

        ctx.store
            .put_track(track_address, track)
            .expect("seed track");

        ctx.store
            .put_track_data(track_address, BlobData::Coded(blob))
            .expect("seed track data");

        ctx.store
            .put_object_info(
                track_address,
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot {
                        epoch,
                    },
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: SlotNumber(epoch.0),
                },
            )
            .expect("seed object info");

        ctx.store
            .put_slice(helper_spool, track_address, helper_slice)
            .expect("seed helper slice");

        ctx.store
            .put_spool_states(&[(helper_spool, SpoolState::new_scanning())])
            .expect("set spool row");

        let request = RepairRequest {
            helper_spool,
            stripes: vec![StripeSubChunkRequest {
                stripe: 0,
                sub_chunks: vec![0],
            }],
        };

        let body = wincode::serialize(&request).expect("serialize repair request");

        let result = repair(
            State(AppState {
                context: ctx.clone(),
            }),
            StakedPeer {
                node: ctx.node_address(),
                tls_pubkey: ctx.tls_pubkey(),
                stake: TAPE(1),
            },
            MaybeActivePeer(None),
            PeerIdentity::default(),
            Path(track_address.to_string()),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await;
        let response = match result {
            Ok(response) => response.into_response(),
            Err(_) => panic!("repair failed for projected snapshot track"),
        };

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        assert!(!bytes.is_empty());
    }

    // a helper slice on a network with no Clay code has nothing to cut
    #[test]
    fn needs_clay() {
        let profile = ErasureCode::ReedSolomon(RSParams::new(3, 1));

        assert!(extract_repair_data(profile, &[], &[0; 64]).is_err());
    }
}

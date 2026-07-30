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
use tape_core::erasure::leaf_position;
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

    // The commitment indexes leaves by position in the group, not by the
    // network-wide spool index the route carries.
    let position = leaf_position(spool_id)
        .ok_or_else(|| RouteError::BadRequest("spool outside the network".into()))?;

    let proof = encoding
        .prove_sub_leaf(position, sub_leaf, &slice)
        .ok_or_else(|| RouteError::BadRequest("sub-leaf index past the slice".into()))?;

    let body = wincode::serialize(&SampleProofPayload::from(proof))
        .map_err(|error| RouteError::Internal(format!("encode sample proof: {error}")))?;

    Ok(([(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

fn store_error(error: impl std::fmt::Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{
        GROUP_SIZE, SLICE_TREE_HEIGHT, SUB_LEAF_BYTES, SUB_TREE_HEIGHT, slice_root,
    };
    use tape_core::prelude::{SpoolState, SpoolStatus};
    use tape_core::track::blob::{BlobEncoding, SubLeafProof};
    use tape_core::types::{EpochNumber, StorageUnits, StripeCount};
    use tape_crypto::hash::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::SampleProofPayload;
    use tape_slicer::{ErasureCoder, Slicer};
    use tape_store::ops::SpoolOps;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    async fn test_context() -> (TestContext, SpoolIndex) {
        let harness = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let spool = harness.owned_spools(0)[0];
        (harness.ctx_for(0), spool)
    }

    /// Encode a track, register its encoding, and store our slice of it.
    fn seed(ctx: &TestContext, spool: SpoolIndex) -> (Address, usize) {
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let payload: Vec<u8> = (0..200_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect();

        let mut slicer = Slicer::clay_default();
        let slices = slicer.encode(&payload).expect("clay encode");
        let leaves: [Hash; GROUP_SIZE] =
            core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));

        let encoding = BlobEncoding {
            size: StorageUnits::from_bytes(payload.len() as u64),
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(slicer.stripe_size() as u64),
            stripe_count: StripeCount(1),
            leaves,
        };

        let position = leaf_position(spool).expect("spool in a group").as_usize();
        let slice_leaves = slices[position].len().div_ceil(SUB_LEAF_BYTES);
        let track = Address::new_unique();

        ctx.store
            .set_spool_state(spool, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .expect("spool state");
        ctx.store
            .put_track_data(track, BlobData::Coded(encoding))
            .expect("track data");
        ctx.store
            .put_slice(spool, track, slices[position].clone())
            .expect("put slice");

        (track, slice_leaves)
    }

    async fn call(
        ctx: &TestContext,
        track: Address,
        spool: SpoolIndex,
        sub_leaf: u64,
    ) -> Result<SampleProofPayload, RouteError> {
        let response = get_sample::<_, _, _>(
            State(AppState {
                context: ctx.clone(),
            }),
            Path((track.to_string(), spool, sub_leaf)),
        )
        .await?
        .into_response();

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        Ok(wincode::deserialize(&body).expect("decode proof"))
    }

    #[tokio::test]
    async fn a_sampled_leaf_verifies_against_the_registered_encoding() {
        let (ctx, spool) = test_context().await;
        let (track, leaves) = seed(&ctx, spool);
        let position = leaf_position(spool).expect("spool in a group");

        let Some(BlobData::Coded(encoding)) = ctx.store.get_track_data(track).unwrap() else {
            panic!("coded track expected");
        };

        for sub_leaf in [0u64, 1, (leaves / 2) as u64, (leaves - 1) as u64] {
            let payload = call(&ctx, track, spool, sub_leaf).await.expect("proof");
            assert!(encoding.verify_sub_leaf(
                position,
                sub_leaf as usize,
                &SubLeafProof::from(payload),
            ));
        }
    }

    #[tokio::test]
    async fn the_response_carries_the_bytes_not_a_hash() {
        // The whole point of sampling the sub-leaf level: a cached digest is not
        // an answer, so the leaf itself has to be on the wire.
        let (ctx, spool) = test_context().await;
        let (track, _) = seed(&ctx, spool);

        let payload = call(&ctx, track, spool, 3).await.expect("proof");
        assert_eq!(payload.sub_leaf.len(), SUB_LEAF_BYTES);
        assert_eq!(payload.sub_proof.len(), SUB_TREE_HEIGHT);
    }

    #[tokio::test]
    async fn a_leaf_past_the_slice_is_refused() {
        let (ctx, spool) = test_context().await;
        let (track, leaves) = seed(&ctx, spool);

        let error = call(&ctx, track, spool, leaves as u64).await.unwrap_err();
        assert!(matches!(error, RouteError::BadRequest(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_track_we_do_not_hold_is_not_found() {
        let (ctx, spool) = test_context().await;
        seed(&ctx, spool);

        let error = call(&ctx, Address::new_unique(), spool, 0).await.unwrap_err();
        assert!(matches!(error, RouteError::NotFound), "{error:?}");
    }

    #[tokio::test]
    async fn a_spool_we_do_not_own_is_refused() {
        let (ctx, spool) = test_context().await;
        let (track, _) = seed(&ctx, spool);

        // A spool with no local state is one this node is not responsible for.
        let foreign = SpoolIndex(spool.as_u64() + GROUP_SIZE as u64);
        let error = call(&ctx, track, foreign, 0).await.unwrap_err();
        assert!(matches!(error, RouteError::NotResponsible), "{error:?}");
    }
}

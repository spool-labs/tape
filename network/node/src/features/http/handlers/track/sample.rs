//! Answer a storage challenge for one sample leaf of one slice.
//!
//! The challenger draws the coordinates from a finalized block hash, so this
//! handler is a plain read: prove the leaf at the index it was asked for. It
//! reads the slice bytes rather than any cached digest, which is the point. An
//! owner that discarded the slice cannot answer, and one that kept only a proof
//! cannot either, because the response carries the leaf itself.
//!
//! The path comes from the sidecar stored beside the slice, so answering costs a
//! hash of one window instead of the whole slice. The paper's premise is that
//! reading retained data is fast, and a response that rehashes megabytes would
//! push the deadline wide enough for a fetching free-rider to sit inside.

use axum::extract::{Path, State};
use axum::http::header;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{
    SUB_LEAF_BYTES, SUB_TREE_HEIGHT, leaf_position, prove_sub_leaf_windowed, sample_window,
    sub_leaf_count, sub_leaf_hashes,
};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::merkle::create_proof_from_leaf_hashes;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, SampleProofPayload};
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps};
use tracing::{trace, warn};

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
        .ok()
        .filter(|sub_leaf| *sub_leaf < sub_leaf_count(slice.len()))
        .ok_or_else(|| RouteError::BadRequest("sub-leaf index past the slice".into()))?;

    // Answer from the sidecar, so the path costs one window rather than a hash of
    // the whole slice. A slice stored before the sidecar existed has none until
    // the startup rebuild reaches it, and pays the old cost until then.
    let sidecar = state
        .context
        .store
        .get_slice_sidecar(spool_id, track)
        .map_err(store_error)?;

    let sub_proof = match &sidecar {
        Some(sidecar) => prove_sub_leaf_windowed(
            sidecar,
            &slice[sample_window(sub_leaf, slice.len())],
            sub_leaf,
        ),
        None => create_proof_from_leaf_hashes::<SUB_TREE_HEIGHT>(
            &sub_leaf_hashes(&slice),
            sub_leaf,
        )
        .ok(),
    }
    .ok_or_else(|| RouteError::Internal("sample path unavailable".into()))?;

    let start = sub_leaf * SUB_LEAF_BYTES;
    let proof = SubLeafProof {
        sub_leaf: slice[start..(start + SUB_LEAF_BYTES).min(slice.len())].to_vec(),
        sub_proof,
    };

    // Check our own answer before sending it. It costs one leaf hash and sixteen
    // pairs, and it means a rejected proof at the challenger is evidence about
    // the data rather than about a sidecar that drifted from it.
    let position = leaf_position(spool_id);
    if !encoding.verify_sub_leaf(position, sub_leaf, &proof) {
        warn!(track = %track, spool = %spool_id, sub_leaf, "sample: local proof does not verify");
        return Err(RouteError::Internal("sample does not match the commitment".into()));
    }

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
    use tape_core::erasure::{GROUP_SIZE, SAMPLE_WINDOW_LEAVES};
    use tape_core::prelude::{SpoolState, SpoolStatus};
    use tape_core::track::blob::SubLeafProof;
    use tape_core::types::EpochNumber;
    use tape_store::ops::SpoolOps;

    use super::*;
    use crate::harness::{NodeHarness, TestContext, coded_track};

    const PAYLOAD_BYTES: usize = 200_000;
    /// Big enough that a slice spans more than one sample window, which is what
    /// makes the sidecar and the full rebuild take different code paths.
    const MULTI_WINDOW_BYTES: usize = 3_000_000;

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

    /// Register an encoding and store our slice of it, returning the track and
    /// how many sample leaves that slice holds.
    fn seed(ctx: &TestContext, spool: SpoolIndex) -> (Address, usize) {
        seed_len(ctx, spool, PAYLOAD_BYTES)
    }

    fn seed_len(ctx: &TestContext, spool: SpoolIndex, len: usize) -> (Address, usize) {
        let (slices, encoding) = coded_track(len, 0x2545_F491_4F6C_DD1D);
        let slice = &slices[leaf_position(spool).as_usize()];
        let leaves = sub_leaf_count(slice.len());
        let track = Address::new_unique();

        ctx.store
            .set_spool_state(spool, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .expect("spool state");
        ctx.store
            .put_track_data(track, BlobData::Coded(encoding))
            .expect("track data");
        ctx.store
            .put_slice(spool, track, slice.clone())
            .expect("put slice");

        (track, leaves)
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
        let position = leaf_position(spool);

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
    async fn a_sidecar_and_a_full_rebuild_answer_identically() {
        // The fallback path a slice stored before the sidecar existed takes has
        // to produce the same proof, or upgrading a node would break its answers.
        let (ctx, spool) = test_context().await;
        let (track, leaves) = seed_len(&ctx, spool, MULTI_WINDOW_BYTES);
        assert!(leaves > SAMPLE_WINDOW_LEAVES, "slice spans one window only");
        assert!(ctx.store.get_slice_sidecar(spool, track).unwrap().is_some());

        // Both ends of a window and both ends of the slice.
        let sampled = [
            0u64,
            SAMPLE_WINDOW_LEAVES as u64 - 1,
            SAMPLE_WINDOW_LEAVES as u64,
            (leaves - 1) as u64,
        ];

        let mut with_sidecar = Vec::new();
        for sub_leaf in sampled {
            with_sidecar.push(call(&ctx, track, spool, sub_leaf).await.expect("proof"));
        }

        ctx.store.delete_slice_sidecar(spool, track).unwrap();
        assert!(ctx.store.get_slice_sidecar(spool, track).unwrap().is_none());

        for (index, sub_leaf) in sampled.into_iter().enumerate() {
            let rebuilt = call(&ctx, track, spool, sub_leaf).await.expect("proof");
            assert_eq!(rebuilt, with_sidecar[index], "leaf {sub_leaf}");
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

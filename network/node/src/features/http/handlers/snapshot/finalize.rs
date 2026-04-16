//! `POST /v1/snapshots/{epoch}/groups/{group}/finalize`
//!
//! Peer asks this node for a BLS signature on `SnapshotSignMessage`. The
//! signature is only produced once every chunk this node built locally for
//! `(epoch, group)` has been observed on-chain — i.e., all cache entries
//! for the group carry a `posted_track`.

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotSignMessage;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse};
use tape_protocol::Api;

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn finalize<Db: Store, Cluster: Api, Blockchain: Rpc>(
    Path((epoch, group)): Path<(u64, u64)>,
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Result<impl IntoResponse, RouteError> {
    let epoch = EpochNumber(epoch);
    let group = SpoolGroup(group);

    let signing_epoch = current_epoch(&state)?;

    let protocol = state.context.state();
    if protocol.find_member(state.context.node_id()).is_none() {
        return Err(RouteError::NotInCommittee);
    }

    let group_is_local = protocol
        .group_peers(group)
        .into_iter()
        .any(|(_, node_id)| node_id == state.context.node_id());
    if !group_is_local {
        return Err(RouteError::NotResponsible);
    }

    let progress = state.context.snapshot_cache.group_progress(epoch, group);
    if progress.is_empty() {
        return Err(RouteError::NotFound);
    }
    if !progress.is_complete() {
        return Err(RouteError::BadRequest(format!(
            "snapshot group not ready: {}/{} chunks posted on-chain",
            progress.posted, progress.built,
        )));
    }

    let message = SnapshotSignMessage::new(epoch, group);
    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsSignResponse {
        signature,
        node_id: state.context.node_id(),
        epoch: signing_epoch,
    };

    let bytes = wincode::serialize(&response).map_err(|error| {
        RouteError::Internal(format!("serialize snapshot finalize response: {error}"))
    })?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, State};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{ChunkNumber, EpochNumber, NodeId, StorageUnits, StripeCount};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::Hash;
    use tape_crypto::address::Address;
    use tape_protocol::ProtocolState;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;
    use crate::features::snapshot::cache::ChunkKey;

    fn sample_blob(commitment: Hash) -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(2_048),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE],
        }
    }

    fn empty_slices() -> [Vec<u8>; SPOOL_GROUP_SIZE] {
        core::array::from_fn(|_| Vec::new())
    }

    fn local_state_for_node_0(responsible: bool) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(11);
        state.committee = vec![
            CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1_000)),
            CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1_000)),
        ];

        let mut spools = [0u8; SPOOL_COUNT];
        if !responsible {
            for spool in &mut spools {
                *spool = 1;
            }
        }
        state.spools = SpoolAssignment::new(spools);
        state
    }

    async fn render(
        state: AppState<
            store_memory::MemoryStore,
            peer_memory::MemoryApi,
            rpc_litesvm::LiteSvmRpc,
        >,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<axum::response::Response, RouteError> {
        finalize(Path((epoch.0, group.0)), State(state))
            .await
            .map(|response| response.into_response())
    }

    #[tokio::test]
    async fn signs_when_all_chunks_posted() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);

        for chunk_index in 0..3 {
            let key = ChunkKey::new(epoch, group, ChunkNumber(chunk_index));
            context.snapshot_cache.insert(
                key,
                sample_blob(Hash::from([chunk_index as u8; 32])),
                empty_slices(),
            );
            context
                .snapshot_cache
                .mark_posted(&key, Address::from([chunk_index as u8; 32]))
                .expect("cache entry present");
        }

        let response = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
        )
        .await
        .expect("handler success");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let decoded: BlsSignResponse = wincode::deserialize(&bytes).unwrap();

        let message = SnapshotSignMessage::new(epoch, group);
        let expected = context.bls_sign(&message.to_bytes()).unwrap();

        assert_eq!(decoded.signature, expected);
        assert_eq!(decoded.node_id, context.node_id());
        assert_eq!(decoded.epoch, EpochNumber(11));
    }

    #[tokio::test]
    async fn rejects_when_chunks_not_posted() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);

        context.snapshot_cache.insert(
            ChunkKey::new(epoch, group, ChunkNumber(0)),
            sample_blob(Hash::from([0xAB; 32])),
            empty_slices(),
        );

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
        )
        .await
        .expect_err("incomplete group should fail");
        assert!(matches!(err, RouteError::BadRequest(_)));
    }

    #[tokio::test]
    async fn rejects_when_group_unbuilt() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            EpochNumber(10),
            SpoolGroup(4),
        )
        .await
        .expect_err("unbuilt group should fail");
        assert!(matches!(err, RouteError::NotFound));
    }

    #[tokio::test]
    async fn rejects_non_responsible_node() {
        let context = test_context();
        context.set_state(local_state_for_node_0(false)).unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            EpochNumber(10),
            SpoolGroup(4),
        )
        .await
        .expect_err("non-responsible node should fail");
        assert!(matches!(err, RouteError::NotResponsible));
    }
}

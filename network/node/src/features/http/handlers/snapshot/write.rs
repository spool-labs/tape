//! `POST /v1/snapshots/{epoch}/groups/{group}/chunks/{chunk}/write`
//!
//! Peer asks this node for a BLS signature on `SnapshotWriteMessage`. The
//! signature is only produced if the peer's claimed `value_hash` matches
//! what this node computed during its own build for the same chunk — the
//! cached `BlobInfo` is the source of truth.

use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotWriteMessage;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse, GetSnapshotWriteSigRequest};
use tape_protocol::Api;

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};
use crate::features::snapshot::cache::ChunkKey;

pub async fn write<Db: Store, Cluster: Api, Blockchain: Rpc>(
    Path((epoch, group, chunk)): Path<(u64, u64, u64)>,
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: GetSnapshotWriteSigRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("snapshot write request: {error}")))?;

    let epoch = EpochNumber(epoch);
    let group = SpoolGroup(group);
    let chunk = ChunkNumber(chunk);

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

    let key = ChunkKey::new(epoch, group, chunk);
    let local_hash = state
        .context
        .snapshot_cache
        .value_hash(&key)
        .ok_or(RouteError::NotFound)?;

    if local_hash != request.value_hash {
        return Err(RouteError::BadRequest(
            "snapshot chunk value_hash mismatch".into(),
        ));
    }

    let message = SnapshotWriteMessage::new(epoch, group, chunk, request.value_hash);
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
        RouteError::Internal(format!("serialize snapshot write response: {error}"))
    })?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], bytes))
}

#[allow(dead_code)]
fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{
        ChunkNumber, EpochNumber, NodeId, StorageUnits, StripeCount,
    };
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::Hash;
    use tape_protocol::ProtocolState;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;
    use crate::features::snapshot::cache::ChunkKey;

    fn request(value_hash: Hash) -> GetSnapshotWriteSigRequest {
        GetSnapshotWriteSigRequest { value_hash }
    }

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
        chunk: ChunkNumber,
        body: GetSnapshotWriteSigRequest,
    ) -> Result<axum::response::Response, RouteError> {
        let bytes = wincode::serialize(&body).unwrap();
        write(
            Path((epoch.0, group.0, chunk.0)),
            State(state),
            Bytes::from(bytes),
        )
        .await
        .map(|response| response.into_response())
    }

    #[tokio::test]
    async fn signs_matching_chunk() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(2);
        let blob = sample_blob(Hash::from([0xAB; 32]));
        let value_hash = blob.get_hash();

        context.snapshot_cache.insert(
            ChunkKey::new(epoch, group, chunk),
            blob,
            empty_slices(),
        );

        let response = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            chunk,
            request(value_hash),
        )
        .await
        .expect("handler success");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let decoded: BlsSignResponse = wincode::deserialize(&bytes).unwrap();

        let message = SnapshotWriteMessage::new(epoch, group, chunk, value_hash);
        let expected = context.bls_sign(&message.to_bytes()).unwrap();

        assert_eq!(decoded.signature, expected);
        assert_eq!(decoded.node_id, context.node_id());
        assert_eq!(decoded.epoch, EpochNumber(11));
    }

    #[tokio::test]
    async fn rejects_mismatched_value_hash() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(0);
        let blob = sample_blob(Hash::from([0xAB; 32]));

        context.snapshot_cache.insert(
            ChunkKey::new(epoch, group, chunk),
            blob,
            empty_slices(),
        );

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            chunk,
            request(Hash::from([0xCD; 32])),
        )
        .await
        .expect_err("mismatched value_hash should fail");
        assert!(matches!(err, RouteError::BadRequest(_)));
    }

    #[tokio::test]
    async fn rejects_unknown_chunk() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(0);

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            chunk,
            request(Hash::from([0xAB; 32])),
        )
        .await
        .expect_err("chunk not in cache should fail");
        assert!(matches!(err, RouteError::NotFound));
    }

    #[tokio::test]
    async fn rejects_non_responsible_node() {
        let context = test_context();
        context.set_state(local_state_for_node_0(false)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(0);
        let blob = sample_blob(Hash::from([0xAB; 32]));
        let value_hash = blob.get_hash();

        context.snapshot_cache.insert(
            ChunkKey::new(epoch, group, chunk),
            blob,
            empty_slices(),
        );

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            chunk,
            request(value_hash),
        )
        .await
        .expect_err("non-responsible node should fail");
        assert!(matches!(err, RouteError::NotResponsible));
    }
}

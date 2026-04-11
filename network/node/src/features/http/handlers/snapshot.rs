use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::snapshot::SnapshotMessage;
use tape_core::snapshot::info::{SnapshotGroupInfo, SnapshotGroupStatus};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse, SignSnapshotRequest};
use tape_store::ops::SnapshotOps;

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn sign_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    Path((epoch, group)): Path<(u64, u64)>,
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SignSnapshotRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("snapshot sign request: {error}")))?;

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

    let snapshot_group = state
        .context
        .store
        .get_group_info(epoch, group)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    validate_snapshot_group(&snapshot_group, &request)?;

    let message = SnapshotMessage::new(epoch, group, request.blob_hash);
    let signature = state
        .context
        .bls_sign(&message.to_bytes())
        .map_err(|error| RouteError::Internal(format!("bls sign: {error:?}")))?;

    let response = BlsSignResponse {
        signature,
        node_id: state.context.node_id(),
        epoch: signing_epoch,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!(
            "serialize snapshot sign response: {error}"
        )))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], bytes))
}

fn validate_snapshot_group(
    snapshot_group: &SnapshotGroupInfo,
    request: &SignSnapshotRequest,
) -> Result<(), RouteError> {
    if matches!(snapshot_group.status, SnapshotGroupStatus::Missing) {
        return Err(RouteError::NotFound);
    }

    if snapshot_group.blob.get_hash() != request.blob_hash {
        return Err(RouteError::BadRequest(
            "snapshot group blob hash mismatch".into(),
        ));
    }

    Ok(())
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use tape_core::cert::snapshot::SnapshotMessage;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::info::SnapshotGroupStatus;
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{
        EpochNumber, NodeId, StorageUnits, StripeCount,
    };
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::Hash;
    use tape_protocol::ProtocolState;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    fn request(blob_hash: Hash) -> SignSnapshotRequest {
        SignSnapshotRequest {
            blob_hash,
        }
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

    fn snapshot_group_state(blob: BlobInfo) -> SnapshotGroupInfo {
        SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            blob,
            track_number: None,
        }
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
        body: SignSnapshotRequest,
    ) -> Result<axum::response::Response, RouteError> {
        let bytes = wincode::serialize(&body).unwrap();
        sign_snapshot(
            Path((epoch.0, group.0)),
            State(state),
            Bytes::from(bytes),
        )
            .await
            .map(|response| response.into_response())
    }

    #[tokio::test]
    async fn signs_snapshot_group() {
        let context = test_context();
        let state = local_state_for_node_0(true);
        context.set_state(state).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let blob = sample_blob(commitment);
        let blob_hash = blob.get_hash();

        context
            .store
            .put_group_info(epoch, group, snapshot_group_state(blob))
            .unwrap();

        let response = match render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            request(blob_hash),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => panic!("handler success"),
        };

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let decoded: BlsSignResponse = wincode::deserialize(&bytes).unwrap();

        let message = SnapshotMessage::new(epoch, group, blob_hash);
        let expected = context.bls_sign(&message.to_bytes()).unwrap();

        assert_eq!(decoded.signature, expected);
        assert_eq!(decoded.node_id, context.node_id());
        assert_eq!(decoded.epoch, EpochNumber(11));
    }

    #[tokio::test]
    async fn rejects_missing_local_group() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let request = request(Hash::from([0xAB; 32]));

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            request,
        )
        .await
        .expect_err("missing group should fail");
        assert!(matches!(err, RouteError::NotFound));
    }

    #[tokio::test]
    async fn rejects_non_responsible_node() {
        let context = test_context();
        context.set_state(local_state_for_node_0(false)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let blob = sample_blob(commitment);
        let blob_hash = blob.get_hash();

        context
            .store
            .put_group_info(epoch, group, snapshot_group_state(blob))
            .unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            request(blob_hash),
        )
        .await
        .expect_err("non-responsible node should fail");
        assert!(matches!(err, RouteError::NotResponsible));
    }

    #[tokio::test]
    async fn rejects_mismatched_request() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let blob = sample_blob(commitment);

        context
            .store
            .put_group_info(epoch, group, snapshot_group_state(blob))
            .unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            epoch,
            group,
            request(Hash::from([0xCD; 32])),
        )
        .await
        .expect_err("mismatched blob hash should fail");
        assert!(matches!(err, RouteError::BadRequest(_)));
    }
}

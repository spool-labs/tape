use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::snapshot::{SnapshotGroupInfo, SnapshotGroupStatus, SnapshotMessage};
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_protocol::api::{BINARY_CONTENT, BlsSignResponse, SignSnapshotRequest};
use tape_store::ops::SnapshotOps;

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};

pub async fn sign_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    Path((snapshot_epoch, group)): Path<(u64, u64)>,
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SignSnapshotRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("snapshot sign request: {error}")))?;
    let snapshot_epoch = EpochNumber(snapshot_epoch);
    let group = SpoolGroup(group);

    let epoch = current_epoch(&state)?;
    if request.signing_epoch != epoch {
        return Err(RouteError::BadRequest(
            "snapshot signing epoch does not match current epoch".into(),
        ));
    }

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

    let group_info = state
        .context
        .store
        .get_group_info(snapshot_epoch, group)
        .map_err(store_error)?
        .ok_or(RouteError::NotFound)?;

    validate_group_info(&group_info, &request)?;

    let message = SnapshotMessage::new(
        snapshot_epoch,
        request.signing_epoch,
        group,
        request.commitment,
        request.parent_epoch,
    );
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
        .map_err(|error| RouteError::Internal(format!(
            "serialize snapshot sign response: {error}"
        )))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], bytes))
}

fn validate_group_info(
    info: &SnapshotGroupInfo,
    request: &SignSnapshotRequest,
) -> Result<(), RouteError> {
    if matches!(info.status, SnapshotGroupStatus::Missing) {
        return Err(RouteError::NotFound);
    }

    if info.parent_epoch != request.parent_epoch {
        return Err(RouteError::BadRequest(
            "snapshot group parent epoch mismatch".into(),
        ));
    }

    if info.meta.commitment != request.commitment {
        return Err(RouteError::BadRequest(
            "snapshot group commitment mismatch".into(),
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
    use tape_core::bls::BlsPrivateKey;
    use tape_core::bls::BlsSignature;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{MEMBER_COUNT, SPOOL_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::{
        CommitteeBitmap, SnapshotChunkMeta, SnapshotGroupStatus, SnapshotMessage,
    };
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::types::{EpochNumber, NodeId, StorageUnits, StripeCount};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::Hash;
    use tape_protocol::ProtocolState;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    fn snapshot_signature(message: &[u8]) -> BlsSignature {
        BlsPrivateKey::from_random().sign(message).unwrap()
    }

    fn request(
        signing_epoch: EpochNumber,
        commitment: Hash,
        parent_epoch: EpochNumber,
    ) -> SignSnapshotRequest {
        SignSnapshotRequest {
            signing_epoch,
            commitment,
            parent_epoch,
        }
    }

    fn group_info(
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        commitment: Hash,
        parent_epoch: EpochNumber,
    ) -> SnapshotGroupInfo {
        SnapshotGroupInfo {
            epoch: snapshot_epoch,
            parent_epoch,
            group,
            status: SnapshotGroupStatus::Built,
            meta: SnapshotChunkMeta {
                commitment,
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(512),
                stripe_count: StripeCount(4),
            },
            leaves: [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE],
            bitmap: CommitteeBitmap::from_indices(&[0], MEMBER_COUNT),
            signature: snapshot_signature(b"group-info"),
            track: None,
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
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        body: SignSnapshotRequest,
    ) -> Result<axum::response::Response, RouteError> {
        let bytes = wincode::serialize(&body).unwrap();
        sign_snapshot(
            Path((snapshot_epoch.0, group.0)),
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

        let snapshot_epoch = EpochNumber(10);
        let signing_epoch = EpochNumber(11);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let parent_epoch = EpochNumber(9);

        context
            .store
            .put_group_info(group_info(snapshot_epoch, group, commitment, parent_epoch))
            .unwrap();

        let response = match render(
            AppState {
                context: context.clone(),
            },
            snapshot_epoch,
            group,
            request(signing_epoch, commitment, parent_epoch),
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

        let message = SnapshotMessage::new(snapshot_epoch, signing_epoch, group, commitment, parent_epoch);
        let expected = context.bls_sign(&message.to_bytes()).unwrap();

        assert_eq!(decoded.signature, expected);
        assert_eq!(decoded.node_id, context.node_id());
        assert_eq!(decoded.epoch, signing_epoch);
    }

    #[tokio::test]
    async fn rejects_missing_local_group() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let snapshot_epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let request = request(EpochNumber(11), Hash::from([0xAB; 32]), EpochNumber(9));

        let err = render(
            AppState {
                context: context.clone(),
            },
            snapshot_epoch,
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

        let snapshot_epoch = EpochNumber(10);
        let signing_epoch = EpochNumber(11);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let parent_epoch = EpochNumber(9);

        context
            .store
            .put_group_info(group_info(snapshot_epoch, group, commitment, parent_epoch))
            .unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            snapshot_epoch,
            group,
            request(signing_epoch, commitment, parent_epoch),
        )
        .await
        .expect_err("non-responsible node should fail");
        assert!(matches!(err, RouteError::NotResponsible));
    }

    #[tokio::test]
    async fn rejects_mismatched_request() {
        let context = test_context();
        context.set_state(local_state_for_node_0(true)).unwrap();

        let snapshot_epoch = EpochNumber(10);
        let signing_epoch = EpochNumber(11);
        let group = SpoolGroup(4);
        let commitment = Hash::from([0xAB; 32]);
        let parent_epoch = EpochNumber(9);

        context
            .store
            .put_group_info(group_info(snapshot_epoch, group, commitment, parent_epoch))
            .unwrap();

        let err = render(
            AppState {
                context: context.clone(),
            },
            snapshot_epoch,
            group,
            request(signing_epoch, Hash::from([0xCD; 32]), parent_epoch),
        )
        .await
        .expect_err("mismatched commitment should fail");
        assert!(matches!(err, RouteError::BadRequest(_)));
    }
}

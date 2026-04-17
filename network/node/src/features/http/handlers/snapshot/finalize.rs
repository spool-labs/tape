//! `POST /v1/snapshots/finalize`
//!
//! Accept one pushed partial signature for `SnapshotSignMessage`.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotSignMessage;
use tape_protocol::api::PushSnapshotFinalizeSigRequest;
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tape_store::types::SnapshotFinalizeVote;

use crate::features::http::error::RouteError;
use crate::features::http::state::{AppState, current_epoch};
use crate::features::snapshot::quorum::{
    bitmap_index_in_group, group_peer_by_index, is_current_snapshot_epoch, verify_partial,
};

pub async fn finalize<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: PushSnapshotFinalizeSigRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("snapshot finalize request: {error}")))?;

    current_epoch(&state)?;
    let protocol = state.context.state();
    let message = SnapshotSignMessage::from_bytes(&request.message)
        .ok_or_else(|| RouteError::BadRequest("invalid snapshot finalize message".into()))?;

    if !is_current_snapshot_epoch(&protocol, message.epoch) {
        return Err(RouteError::BadRequest(format!(
            "snapshot epoch {} does not match local epoch {}",
            message.epoch.0,
            protocol.epoch.0
        )));
    }

    if protocol.find_member(state.context.node_id()).is_none() {
        return Err(RouteError::NotInCommittee);
    }

    if bitmap_index_in_group(&protocol, message.group, state.context.node_id()).is_none() {
        return Err(RouteError::NotResponsible);
    }

    let signer_index =
        bitmap_index_in_group(&protocol, message.group, request.node_id).ok_or(RouteError::NotInCommittee)?;
    let signer =
        group_peer_by_index(&protocol, message.group, signer_index).ok_or(RouteError::NotInCommittee)?;

    if !verify_partial(&signer.pubkey, &request.message, &request.signature) {
        return Err(RouteError::InvalidSignature);
    }

    state
        .context
        .store
        .put_snapshot_finalize_sig(
            message.epoch,
            message.group,
            signer_index,
            &SnapshotFinalizeVote {
                message: request.message,
                signature: request.signature.clone(),
            },
        )
        .map_err(|error| RouteError::Internal(format!("put_snapshot_finalize_sig: {error}")))?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::State;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::CommitteeMember;
    use tape_core::types::{EpochNumber, NodeId};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_protocol::ProtocolState;
    use tape_store::ops::SnapshotOps;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    fn local_state() -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(11);
        state.committee = vec![
            CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1_000)),
            CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1_000)),
        ];
        let mut spools = [1u8; SPOOL_COUNT];
        spools[80] = 0;
        spools[81] = 1;
        state.spools = SpoolAssignment::new(spools);
        state
    }

    async fn render(
        state: AppState<
            store_memory::MemoryStore,
            peer_memory::MemoryApi,
            rpc_litesvm::LiteSvmRpc,
        >,
        request: PushSnapshotFinalizeSigRequest,
    ) -> Result<axum::response::Response, RouteError> {
        let bytes = wincode::serialize(&request).unwrap();
        finalize(State(state), Bytes::from(bytes))
            .await
            .map(|response| response.into_response())
    }

    #[tokio::test]
    async fn stores_valid_partial() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);

        let signer = BlsPrivateKey::from_random();
        let signer_pubkey = signer.public_key().unwrap();
        let mut state = local_state();
        state.committee[1].key = signer_pubkey;
        context.set_state(state).unwrap();

        let message = SnapshotSignMessage::new(epoch, group);
        let request = PushSnapshotFinalizeSigRequest {
            node_id: NodeId(1),
            message: message.to_bytes(),
            signature: signer.sign(&message.to_bytes()).unwrap(),
        };

        let response = render(
            AppState {
                context: context.clone(),
            },
            request,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            context
                .store
                .count_snapshot_finalize_sigs(epoch, group)
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn rejects_invalid_message() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let err = render(
            AppState { context },
            PushSnapshotFinalizeSigRequest {
                node_id: NodeId(1),
                message: [0u8; tape_core::cert::SNAPSHOT_SIGN_MESSAGE_SIZE],
                signature: BlsPrivateKey::from_random().sign(b"bad").unwrap(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, RouteError::BadRequest(_)));
    }
}

//! Snapshot endpoint for snapshot partial BLS signatures.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::cert::{SnapshotSignMessage, SnapshotWriteMessage};
use tape_protocol::api::{SignatureKind, SnapshotSigRequest};
use tape_protocol::Api;
use tape_store::ops::SnapshotOps;
use tape_store::types::{SnapshotFinalizeVote, SnapshotWriteVote};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;
use crate::features::snapshot::utils::{
    bitmap_index_in_group, group_peer_by_index, verify_partial,
};

pub async fn sig<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SnapshotSigRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("snapshot sig request: {error}")))?;

    let protocol = state.context.state();

    match request.kind {
        SignatureKind::Write => handle_write(&state, &protocol, &request),
        SignatureKind::Finalize => handle_finalize(&state, &protocol, &request),
    }
}

fn handle_write<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &tape_protocol::ProtocolState,
    request: &SnapshotSigRequest,
) -> Result<StatusCode, RouteError> {
    let message = SnapshotWriteMessage::from_bytes(&request.message)
        .ok_or_else(|| RouteError::BadRequest("invalid snapshot write message".into()))?;

    let bitmap_index = preflight(protocol, message.epoch, message.group, request)?;

    let vote = SnapshotWriteVote {
        message: request.message.as_slice().try_into().map_err(|_| {
            RouteError::BadRequest("invalid snapshot write message length".into())
        })?,
        signature: request.signature,
    };

    state
        .context
        .store
        .put_snapshot_write_sig(message.epoch, message.group, message.chunk, bitmap_index, &vote)
        .map_err(|error| RouteError::Internal(format!("put_snapshot_write_sig: {error}")))?;

    Ok(StatusCode::OK)
}

fn handle_finalize<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &tape_protocol::ProtocolState,
    request: &SnapshotSigRequest,
) -> Result<StatusCode, RouteError> {
    let message = SnapshotSignMessage::from_bytes(&request.message)
        .ok_or_else(|| RouteError::BadRequest("invalid snapshot finalize message".into()))?;

    let bitmap_index = preflight(protocol, message.epoch, message.group, request)?;

    let vote = SnapshotFinalizeVote {
        message: request.message.as_slice().try_into().map_err(|_| {
            RouteError::BadRequest("invalid snapshot finalize message length".into())
        })?,
        signature: request.signature,
    };

    state
        .context
        .store
        .put_snapshot_finalize_sig(message.epoch, message.group, bitmap_index, &vote)
        .map_err(|error| RouteError::Internal(format!("put_snapshot_finalize_sig: {error}")))?;

    Ok(StatusCode::OK)
}

/// Verify that the signature in the request is valid for the given message and signer, and return
/// the bitmap index of the signer if valid.
fn preflight(
    protocol: &tape_protocol::ProtocolState,
    message_epoch: tape_core::types::EpochNumber,
    message_group: tape_core::spooler::SpoolGroup,
    request: &SnapshotSigRequest,
) -> Result<u16, RouteError> {
    if protocol.epoch.0 == 0 || message_epoch.0 != protocol.epoch.0 - 1 {
        return Err(RouteError::BadRequest(format!(
            "snapshot epoch {} does not match local epoch {}",
            message_epoch.0, protocol.epoch.0
        )));
    }

    if protocol.find_member(request.node_id).is_none() {
        return Err(RouteError::NotInCommittee);
    }

    let bitmap_index = bitmap_index_in_group(protocol, message_group, request.node_id)
        .ok_or(RouteError::NotResponsible)?;

    let signer = group_peer_by_index(protocol, message_group, bitmap_index)
        .ok_or(RouteError::NotInCommittee)?;

    if !verify_partial(&signer.key, &request.message, &request.signature) {
        return Err(RouteError::InvalidSignature);
    }

    Ok(bitmap_index)
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
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::{ChunkNumber, EpochNumber, NodeId};
    use tape_protocol::ProtocolState;
    use tape_store::ops::SnapshotOps;

    use crate::context::test_utils::test_context;
    use crate::features::http::state::AppState;

    /// Build a committee where NodeId(0) owns spool group 4 slot 0 and
    /// NodeId(1) owns group 4 slot 1 (all other spools are owned by NodeId(1)).
    fn local_state() -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EpochNumber(11);
        state.committee = vec![
            CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1_000)),
            CommitteeMember::new(NodeId(1), Coin::<TAPE>::new(1_000)),
        ];
        let mut spools = [1u8; SPOOL_COUNT];
        spools[80] = 0; // group 4 slot 0 → NodeId(0)
        spools[81] = 1; // group 4 slot 1 → NodeId(1)
        state.spools = SpoolAssignment::new(spools);
        state
    }

    async fn render(
        state: AppState<
            store_memory::MemoryStore,
            peer_memory::MemoryApi,
            rpc_litesvm::LiteSvmRpc,
        >,
        request: SnapshotSigRequest,
    ) -> Result<axum::response::Response, RouteError> {
        let bytes = wincode::serialize(&request).unwrap();
        sig(State(state), Bytes::from(bytes))
            .await
            .map(|response| response.into_response())
    }

    fn configure_signer(
        context: &std::sync::Arc<
            crate::context::NodeContext<
                store_memory::MemoryStore,
                peer_memory::MemoryApi,
                rpc_litesvm::LiteSvmRpc,
            >,
        >,
        signer: &BlsPrivateKey,
    ) {
        let mut state = local_state();
        state.committee[1].key = signer.public_key().unwrap();
        context.set_state(state).unwrap();
    }

    #[tokio::test]
    async fn stores_valid_write_partial() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let chunk = ChunkNumber(2);

        let signer = BlsPrivateKey::from_random();
        configure_signer(&context, &signer);

        let message = SnapshotWriteMessage::new(epoch, group, chunk, tape_crypto::Hash::from([0xAB; 32]));
        let message_bytes = message.to_bytes();
        let request = SnapshotSigRequest {
            node_id: NodeId(1),
            kind: SignatureKind::Write,
            message: message_bytes.to_vec(),
            signature: signer.sign(&message_bytes).unwrap(),
        };

        let response = render(AppState { context: context.clone() }, request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let groups = context.store.iter_snapshot_write_sigs(epoch, group).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].chunk, chunk);
        assert_eq!(groups[0].votes.len(), 1);
    }

    #[tokio::test]
    async fn stores_valid_finalize_partial() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);

        let signer = BlsPrivateKey::from_random();
        configure_signer(&context, &signer);

        let message = SnapshotSignMessage::new(epoch, group);
        let message_bytes = message.to_bytes();
        let request = SnapshotSigRequest {
            node_id: NodeId(1),
            kind: SignatureKind::Finalize,
            message: message_bytes.to_vec(),
            signature: signer.sign(&message_bytes).unwrap(),
        };

        let response = render(AppState { context: context.clone() }, request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            context.store.iter_snapshot_finalize_sigs(epoch, group).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn rejects_wrong_epoch() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let message = SnapshotWriteMessage::new(
            EpochNumber(9),
            SpoolGroup(4),
            ChunkNumber(0),
            tape_crypto::Hash::from([0xCD; 32]),
        );
        let request = SnapshotSigRequest {
            node_id: NodeId(1),
            kind: SignatureKind::Write,
            message: message.to_bytes().to_vec(),
            signature: BlsPrivateKey::from_random().sign(b"bad").unwrap(),
        };

        let err = render(AppState { context }, request).await.unwrap_err();
        assert!(matches!(err, RouteError::BadRequest(_)));
    }

    #[tokio::test]
    async fn rejects_signer_not_in_committee() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let signer = BlsPrivateKey::from_random();
        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let message = SnapshotSignMessage::new(epoch, group);

        let request = SnapshotSigRequest {
            node_id: NodeId(42), // not in committee
            kind: SignatureKind::Finalize,
            message: message.to_bytes().to_vec(),
            signature: signer.sign(&message.to_bytes()).unwrap(),
        };

        let err = render(AppState { context }, request).await.unwrap_err();
        assert!(matches!(err, RouteError::NotInCommittee));
    }

    #[tokio::test]
    async fn rejects_signer_not_in_group() {
        let context = test_context();
        let mut state = local_state();
        // add NodeId(2) to the committee but don't assign any group-4 spool.
        state.committee.push(CommitteeMember::new(NodeId(2), Coin::<TAPE>::new(1_000)));
        context.set_state(state).unwrap();

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let message = SnapshotSignMessage::new(epoch, group);
        let signer = BlsPrivateKey::from_random();

        let request = SnapshotSigRequest {
            node_id: NodeId(2),
            kind: SignatureKind::Finalize,
            message: message.to_bytes().to_vec(),
            signature: signer.sign(&message.to_bytes()).unwrap(),
        };

        let err = render(AppState { context }, request).await.unwrap_err();
        assert!(matches!(err, RouteError::NotResponsible));
    }

    #[tokio::test]
    async fn rejects_invalid_signature() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let signer = BlsPrivateKey::from_random();
        configure_signer(&context, &signer);

        let epoch = EpochNumber(10);
        let group = SpoolGroup(4);
        let message = SnapshotSignMessage::new(epoch, group);

        let request = SnapshotSigRequest {
            node_id: NodeId(1),
            kind: SignatureKind::Finalize,
            message: message.to_bytes().to_vec(),
            // Sign a different message so the signature doesn't match.
            signature: signer.sign(b"wrong-message").unwrap(),
        };

        let err = render(AppState { context }, request).await.unwrap_err();
        assert!(matches!(err, RouteError::InvalidSignature));
    }

    #[tokio::test]
    async fn rejects_malformed_message_bytes() {
        let context = test_context();
        context.set_state(local_state()).unwrap();

        let request = SnapshotSigRequest {
            node_id: NodeId(1),
            kind: SignatureKind::Finalize,
            message: vec![0u8; tape_core::cert::SNAPSHOT_SIGN_MESSAGE_SIZE],
            signature: BlsPrivateKey::from_random().sign(b"bad").unwrap(),
        };

        let err = render(AppState { context }, request).await.unwrap_err();
        assert!(matches!(err, RouteError::BadRequest(_)));
    }
}

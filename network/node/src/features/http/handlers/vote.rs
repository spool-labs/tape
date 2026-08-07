//! Generic peer vote endpoint.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::bls::BlsPubkey;
use tape_core::cert::{AssignmentVoteMessage, NodeEvictMessage, SnapshotSignMessage, eviction_vote_node};
use tape_core::system::{VoteCandidate, VoteKind};
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_protocol::api::VoteRequest;
use tape_store::ops::VoteOps;

use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn vote<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: VoteRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("vote request: {error}")))?;

    if active_peer.node != request.signer {
        return Err(RouteError::Forbidden("vote signer does not match peer identity".into()));
    }

    let protocol = state.context.state();
    let message = validate_candidate(&protocol, request.candidate)?;
    let signer_pubkey = validate_group_signer(&protocol, &request)?;
    request
        .signature
        .verify_aggregate(message, &[signer_pubkey])
        .map_err(|_| RouteError::InvalidSignature)?;

    state
        .context
        .store
        .put_vote_sig(
            request.candidate,
            request.group,
            request.signer,
            &request.signature,
        )
        .map_err(|error| RouteError::Internal(format!("put_vote_sig: {error}")))?;

    Ok(StatusCode::OK)
}

fn validate_candidate(
    protocol: &tape_protocol::ProtocolState,
    candidate: VoteCandidate,
) -> Result<Vec<u8>, RouteError> {
    if candidate.hash == Hash::default() {
        return Err(RouteError::BadRequest("vote candidate hash is empty".into()));
    }

    let current_epoch = protocol.epoch();
    if candidate.voting_epoch != current_epoch {
        return Err(RouteError::BadRequest(format!(
            "vote voting epoch {} does not match local epoch {}",
            candidate.voting_epoch.0, current_epoch.0
        )));
    }

    match candidate.kind {
        VoteKind::Unknown => Err(RouteError::BadRequest("unknown vote kind".into())),
        VoteKind::Snapshot => {
            if current_epoch.is_zero()
                || candidate.target_epoch.0.checked_add(1) != Some(current_epoch.0)
            {
                return Err(RouteError::BadRequest(format!(
                    "snapshot vote target epoch {} does not match local epoch {}",
                    candidate.target_epoch.0, current_epoch.0
                )));
            }
            Ok(SnapshotSignMessage::new(candidate.target_epoch, candidate.hash)
                .to_bytes()
                .to_vec())
        }
        VoteKind::Assignment => {
            let Some(next_epoch) = protocol.next_epoch.as_ref() else {
                return Err(RouteError::BadRequest("assignment target epoch missing".into()));
            };
            if next_epoch.id != candidate.target_epoch {
                return Err(RouteError::BadRequest(format!(
                    "assignment vote target epoch {} does not match next epoch {}",
                    candidate.target_epoch.0, next_epoch.id.0
                )));
            }
            Ok(AssignmentVoteMessage::new(
                candidate.target_epoch,
                next_epoch.nonce,
                candidate.hash,
            )
            .to_bytes()
            .to_vec())
        }
        VoteKind::Eviction => {
            let Some(next_epoch) = protocol.next_epoch.as_ref() else {
                return Err(RouteError::BadRequest("eviction target epoch missing".into()));
            };
            if next_epoch.id != candidate.target_epoch {
                return Err(RouteError::BadRequest(format!(
                    "eviction vote target epoch {} does not match next epoch {}",
                    candidate.target_epoch.0, next_epoch.id.0
                )));
            }
            let node = eviction_vote_node(candidate.hash);
            Ok(NodeEvictMessage::new(candidate.target_epoch, next_epoch.nonce, node)
                .to_bytes()
                .to_vec())
        }
    }
}

fn validate_group_signer(
    protocol: &tape_protocol::ProtocolState,
    request: &VoteRequest,
) -> Result<BlsPubkey, RouteError> {
    if protocol.find_member(request.signer).is_none() {
        return Err(RouteError::NotInCommittee);
    }

    group_signer_pubkey(protocol, request.candidate.voting_epoch, request.group, request.signer)
        .ok_or(RouteError::NotResponsible)
}

fn group_signer_pubkey(
    protocol: &tape_protocol::ProtocolState,
    voting_epoch: EpochNumber,
    group: tape_core::spooler::GroupIndex,
    signer: tape_crypto::Address,
) -> Option<BlsPubkey> {
    (protocol.epoch() == voting_epoch).then_some(())?;

    protocol
        .spool_for_node_in_group(group, signer)
        .map(|(_, spool)| spool.bls_pubkey)
}

//! Receive a group-mate's answer, and the attestations others sign over it.
//!
//! Both are pushed rather than polled, so these are the only places an answer or
//! an attestation enters this node. Everything is checked here, before it reaches
//! the round buffer: a proof against the question this node derived for itself,
//! an attestation against the signer's registered key. Nothing unverified is
//! stored, so a certificate that forms is a certificate over checked evidence.

use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_protocol::Api;
use tape_protocol::api::{AttestationPayload, ProofOfAccessPayload};
use tape_store::ops::ChallengeOps;
use tracing::{debug, trace};

use crate::features::challenge::rounds::RoundKey;
use crate::features::challenge::witness::{
    Round, accept_answer, attest_message, group_members, relay_and_attest, round_of,
};
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

/// Take in one owner's broadcast answer for a round.
pub async fn proof_of_access<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let payload: ProofOfAccessPayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("decode proof: {error}")))?;
    let answer: ProofOfAccess = payload.into();

    let protocol = state.context.state();
    let round = round_of(&answer);
    let key = round.key(answer.spool);

    // A relayed duplicate is the common case, not an error: every peer that
    // accepted the answer forwards it, so most copies arrive after the first.
    if state.context.round_buffer.answer(key).is_some() {
        return Ok(StatusCode::OK);
    }

    let Some(mine) = protocol
        .member_spools(state.context.node_address())
        .first()
        .copied()
    else {
        return Err(RouteError::NotResponsible);
    };

    if !accept_answer(&state.context, &protocol, &answer, mine, true) {
        return Err(RouteError::BadRequest("proof of access refused".into()));
    }

    if !state.context.round_buffer.accept_answer(key, answer.clone()) {
        return Ok(StatusCode::OK);
    }

    trace!(spool = %answer.spool, round = answer.round.0, "challenge: answer accepted");
    relay_and_attest(&state.context, &protocol, &answer).await;
    certify_if_ready(&state, &protocol, &round, key);

    Ok(StatusCode::OK)
}

/// Take in one observer's attestation that it accepted a round's answer.
pub async fn attest<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let payload: AttestationPayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("decode attestation: {error}")))?;

    let protocol = state.context.state();
    let round = Round {
        epoch: payload.epoch,
        group: payload.group,
        round: payload.round,
        block: payload.block,
    };
    let key = round.key(payload.spool);

    // The challenged owner may contribute one signature but cannot certify
    // itself, so its own attestation is refused outright.
    if protocol.spool_owner(payload.spool) == Some(payload.signer) {
        return Err(RouteError::BadRequest("a spool cannot certify itself".into()));
    }

    let Some(peer) = protocol.peer(payload.signer) else {
        return Err(RouteError::BadRequest("unknown signer".into()));
    };
    if payload
        .signature
        .verify_aggregate(
            attest_message(&round, payload.spool).to_bytes(),
            core::slice::from_ref(&peer.bls_pubkey),
        )
        .is_err()
    {
        return Err(RouteError::BadRequest("attestation does not verify".into()));
    }

    state
        .context
        .round_buffer
        .accept_attestation(key, payload.signer, payload.signature);
    certify_if_ready(&state, &protocol, &round, key);

    Ok(StatusCode::OK)
}

/// Record a round the moment it reaches a quorum, once.
///
/// The threshold is a supermajority of the group's live positions, which is what
/// makes a certificate mean at least `q - f` honest signers agreed.
fn certify_if_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &tape_protocol::ProtocolState,
    round: &Round,
    key: RoundKey,
) {
    let threshold = agreement_threshold(group_members(protocol, round.group).len());
    if !state.context.round_buffer.claim_certificate(key, threshold) {
        return;
    }

    let Some(owner) = protocol.spool_owner(key.spool) else {
        return;
    };
    if owner == state.context.node_address() {
        return;
    }

    // Aggregate and check before recording anything. A quorum of accepted
    // attestations should always combine, so a failure here means our own view
    // is inconsistent and the round is put back rather than recorded.
    let attestations = state.context.round_buffer.attestations(key);
    let certificate = SuccessCertificate::aggregate(
        round.epoch,
        round.group,
        round.round,
        key.spool,
        round.block,
        attestations,
    );
    let stands = certificate.as_ref().is_some_and(|certificate| {
        certificate
            .verify(threshold, owner, |signer| {
                protocol.peer(signer).map(|peer| peer.bls_pubkey)
            })
            .inspect_err(|rejection| {
                debug!(spool = %key.spool, ?rejection, "challenge: certificate refused");
            })
            .is_ok()
    });
    if !stands {
        state.context.round_buffer.release_certificate(key);
        return;
    }

    let mut record = state.context.store.peer_record(owner).unwrap_or_default();
    if !record.record(round.epoch, round.round, true) {
        return;
    }
    if let Err(error) = state.context.store.put_peer_record(owner, record) {
        debug!(%error, node = %owner, "challenge: certificate not recorded");
    }
}

/// Signatures a certificate needs, given how many positions the group holds.
///
/// The mechanism's `q` at a full group, scaled down so a partially filled group
/// still certifies rather than stalling every round.
pub fn agreement_threshold(members: usize) -> usize {
    (members * 2 / 3 + 1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::GROUP_SIZE;

    #[test]
    fn the_threshold_is_a_supermajority() {
        // At a full group this is the mechanism's q, and it never drops to a
        // simple majority where two Byzantine signers could carry a round.
        assert_eq!(agreement_threshold(GROUP_SIZE), 14);
        assert!(agreement_threshold(GROUP_SIZE) > GROUP_SIZE / 2);

        // A partially filled group still certifies rather than stalling.
        assert_eq!(agreement_threshold(3), 3);
        assert_eq!(agreement_threshold(1), 1);
        assert_eq!(agreement_threshold(0), 1);
    }
}

use std::collections::BTreeSet;

use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_core::erasure::{GROUP_SIZE, group_for_spool};
use tape_protocol::{Api, ProtocolState};
use tape_protocol::api::{AttestationPayload, ProofOfAccessPayload};
use tracing::{debug, trace};

use crate::features::challenge::audit::{
    Round, accept_answer, attest_message, group_members, round_of, spawn_relay_and_attest,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::rounds::RoundKey;
use crate::features::challenge::trace::MarkKind;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub async fn proof_of_access<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    // A node with the challenge off holds no round to audit against.
    if !state.context.config.challenge.enabled {
        return Err(RouteError::Forbidden("challenge disabled on this node".into()));
    }

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

    // Only a group-mate can audit an answer: nobody else settles the round
    // or holds a stake in it.
    let holds_a_spool = protocol
        .member_spools(state.context.node_address())
        .into_iter()
        .any(|spool| group_for_spool(spool) == answer.group);
    if !holds_a_spool {
        return Err(RouteError::NotResponsible);
    }

    // Timeliness is left to the round, not judged per response: an answer that
    // has not certified by the time the next round opens is settled a miss
    // whenever it arrived, and no schedulable sub-round deadline separates an
    // adversary worth the honest nodes it evicts (see docs/whirlwind.md).
    if !accept_answer(&state.context, &protocol, &answer, true) {
        state
            .context
            .challenge_counters
            .answers_refused
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // No owner named: the payload was refused because it did not verify, so
        // this node has no evidence about who sent it. Attributing it to the
        // spool's on-chain owner would be a guess wearing an observation's face.
        state.context.round_traces.mark(
            round.epoch,
            round.round,
            round.group,
            key.spool,
            MarkKind::AnswerRefused,
            None,
        );
        return Err(RouteError::BadRequest("proof of access refused".into()));
    }

    if !state.context.round_buffer.accept_answer(key, answer.clone()) {
        return Ok(StatusCode::OK);
    }

    mark(&state, &protocol, &round, key, MarkKind::AnswerIn);
    trace!(spool = %answer.spool, round = answer.round.0, "challenge: answer accepted");
    spawn_relay_and_attest(&state.context, &protocol, &answer);
    certify_if_ready(&state, &protocol, &round, key);

    Ok(StatusCode::OK)
}

pub async fn attest<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    if !state.context.config.challenge.enabled {
        return Err(RouteError::Forbidden("challenge disabled on this node".into()));
    }

    let payload: AttestationPayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("decode attestation: {error}")))?;

    let protocol = state.context.state();
    let round = Round {
        epoch: payload.epoch,
        group: payload.group,
        round: payload.round,
        block: payload.block,
    };
    let Some(peer) = protocol.peer(payload.signer) else {
        return Err(RouteError::BadRequest("unknown signer".into()));
    };

    // A round has one signature per spool in the group, so anything longer is
    // not a batch this node asked for, and a spool twice is not a retry.
    if payload.attests.len() > GROUP_SIZE {
        return Err(RouteError::BadRequest("attestation batch too large".into()));
    }
    let mut seen = BTreeSet::new();
    if !payload.attests.iter().all(|attest| seen.insert(attest.spool)) {
        return Err(RouteError::BadRequest("attestation batch repeats a spool".into()));
    }

    // One round's signatures arrive together. Each still stands for its own
    // spool, so each is checked against the message that spool's attesters sign.
    // Every one is verified: entries feed a shared aggregate, so a single
    // unchecked signature fails the whole round and charges the answering node.
    for attest in &payload.attests {
        let key = round.key(attest.spool);
        if attest
            .signature
            .verify_aggregate(
                attest_message(&round, attest.spool).to_bytes(),
                core::slice::from_ref(&peer.bls_pubkey),
            )
            .is_err()
        {
            return Err(RouteError::BadRequest("attestation does not verify".into()));
        }

        // Only a signature this node had not already counted, so a peer
        // replaying one leaves a single mark on the timeline rather than a row.
        if state
            .context
            .round_buffer
            .accept_attestation(key, payload.signer, attest.signature)
        {
            state.context.round_traces.mark(
                round.epoch,
                round.round,
                round.group,
                attest.spool,
                MarkKind::AttestIn,
                Some(payload.signer),
            );
            certify_if_ready(&state, &protocol, &round, key);
        }
    }

    Ok(StatusCode::OK)
}

fn certify_if_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
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

    // Folded now rather than waiting for the block to finalize. A certificate
    // under a candidate that loses records a success the owner may not have
    // earned, which is the harmless direction. Waiting instead would lose the
    // late certificate that replaces a recorded miss, and a miss is what
    // evicts. `settle_previous` refuses to charge a miss for a round that never
    // finalized, which is the half that has teeth.
    fold_outcome(&state.context.store, owner, key.spool, round.epoch, round.round, true);
    state.context.round_traces.mark(
        round.epoch,
        round.round,
        round.group,
        key.spool,
        MarkKind::Certified,
        Some(owner),
    );
}

/// Marks an accepted answer against the spool's owner, who owed it.
fn mark<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    round: &Round,
    key: RoundKey,
    kind: MarkKind,
) {
    state.context.round_traces.mark(
        round.epoch,
        round.round,
        round.group,
        key.spool,
        kind,
        protocol.spool_owner(key.spool),
    );
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

    // at a full group the threshold is the mechanism's q, and it never drops to
    // a simple majority where two Byzantine signers could carry a round
    #[test]
    fn supermajority() {
        assert_eq!(agreement_threshold(GROUP_SIZE), 14);
        assert!(agreement_threshold(GROUP_SIZE) > GROUP_SIZE / 2);

        // A partially filled group still certifies rather than stalling.
        assert_eq!(agreement_threshold(3), 3);
        assert_eq!(agreement_threshold(1), 1);
        assert_eq!(agreement_threshold(0), 1);
    }
}

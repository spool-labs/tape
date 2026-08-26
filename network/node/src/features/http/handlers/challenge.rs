use std::collections::BTreeSet;

use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::bls::BlsPubkey;
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_core::erasure::{GROUP_SIZE, group_for_spool};
use tape_crypto::Address;
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
    // Off the executor: a pairing plus the store reads it checks against.
    let accepted = {
        let context = state.context.clone();
        let protocol = protocol.clone();
        let answer = answer.clone();
        tokio::task::spawn_blocking(move || accept_answer(&context, &protocol, &answer, true))
            .await
            .unwrap_or(false)
    };
    if !accepted {
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
    certify_if_ready(&state, &protocol, &round, key).await;

    Ok(StatusCode::OK)
}

pub async fn attest<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
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
    // The key itself is read at certification; this only turns away a signer the
    // node has never heard of.
    if protocol.peer(payload.signer).is_none() {
        return Err(RouteError::BadRequest("unknown signer".into()));
    }

    // A round has one signature per spool in the group, so anything longer is
    // not a batch this node asked for, and a spool twice is not a retry.
    if payload.attests.len() > GROUP_SIZE {
        return Err(RouteError::BadRequest("attestation batch too large".into()));
    }
    let mut seen = BTreeSet::new();
    if !payload.attests.iter().all(|attest| seen.insert(attest.spool)) {
        return Err(RouteError::BadRequest("attestation batch repeats a spool".into()));
    }

    // Signatures are taken on arrival and settled by the quorum aggregate in
    // `certify_if_ready`, which pairs once for a whole certificate. Checking
    // each on arrival paired every signature twice, once alone and again inside
    // the aggregate, which is a pairing per signature per peer per group every
    // round. A forged one still cannot certify: it fails the aggregate, and the
    // scan behind that failure is what names the signer.
    for attest in &payload.attests {
        let key = round.key(attest.spool);
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
            certify_if_ready(&state, &protocol, &round, key).await;
        }
    }

    Ok(StatusCode::OK)
}

async fn certify_if_ready<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(
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

    // Aggregate and check before recording anything. This is the only pairing
    // an honest round pays, so a failure means a signature this node took on
    // trust does not stand, and the scan below is what names the signer.
    //
    // Off the executor: a quorum verify is a pairing product and the fold
    // behind it writes to the store.
    let attestations = state.context.round_buffer.attestations(key);
    let signers: Vec<(Address, BlsPubkey)> = attestations
        .iter()
        .filter_map(|(signer, _)| Some((*signer, protocol.peer(*signer)?.bls_pubkey)))
        .collect();

    let context = state.context.clone();
    let round = *round;
    let stands = tokio::task::spawn_blocking(move || {
        let certificate = SuccessCertificate::aggregate(
            round.epoch,
            round.group,
            round.round,
            key.spool,
            round.block,
            attestations.clone(),
        );
        let stands = certificate.as_ref().is_some_and(|certificate| {
            certificate
                .verify(threshold, owner, |signer| {
                    signers
                        .iter()
                        .find(|(held, _)| *held == signer)
                        .map(|(_, pubkey)| *pubkey)
                })
                .inspect_err(|rejection| {
                    debug!(spool = %key.spool, ?rejection, "challenge: certificate refused");
                })
                .is_ok()
        });
        if !stands {
            // One bad signature fails the whole aggregate, so the quorum is
            // rebuilt without whoever sent it rather than the round being lost.
            let message = attest_message(&round, key.spool).to_bytes();
            for (signer, signature) in &attestations {
                let Some((_, pubkey)) = signers.iter().find(|(held, _)| held == signer) else {
                    continue;
                };
                if signature
                    .verify_aggregate(&message, core::slice::from_ref(pubkey))
                    .is_err()
                {
                    context.round_buffer.drop_attestation(key, *signer);
                    debug!(spool = %key.spool, node = %signer, "challenge: attestation dropped");
                }
            }
            context.round_buffer.release_certificate(key);
            return false;
        }

        // Folded now rather than waiting for the block to finalize. A
        // certificate under a candidate that loses records a success the owner
        // may not have earned, which is the harmless direction. Waiting instead
        // would lose the late certificate that replaces a recorded miss, and a
        // miss is what evicts. `settle_previous` refuses to charge a miss for a
        // round that never finalized, which is the half that has teeth.
        fold_outcome(&context.store, owner, key.spool, round.epoch, round.round, true);
        true
    })
    .await
    .unwrap_or(false);
    if !stands {
        return;
    }
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

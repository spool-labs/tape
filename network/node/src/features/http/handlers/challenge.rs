use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_core::erasure::group_for_spool;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::{Api, ProtocolState};
use tape_protocol::api::{AttestationPayload, ProofOfAccessPayload};
use tracing::{debug, trace, warn};

use crate::features::challenge::audit::{
    Round, accept_answer, attest_message, group_members, round_of, spawn_relay_and_attest,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::refusal::RefusalReason;
use crate::features::challenge::rounds::RoundKey;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;
use crate::features::state::realign::{RealignCause, spawn_realign};

pub async fn proof_of_access<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    // A node with the challenge off holds no round to audit against.
    if !state.context.config.challenge.enabled {
        return Err(RouteError::Forbidden("challenge disabled on this node".into()));
    }
    // A node re-reading its view judges nothing until it has one it trusts.
    if state.context.challenge_tripwire.is_realigning() {
        return Err(RouteError::Unavailable("realigning protocol state".into()));
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
    if let Err(reason) = accept_answer(&state.context, &protocol, &answer, true) {
        return Err(refuse(&state, &answer, reason));
    }

    if !state.context.round_buffer.accept_answer(key, answer.clone()) {
        return Ok(StatusCode::OK);
    }

    trace!(spool = %answer.spool, round = answer.round.0, "challenge: answer accepted");
    spawn_relay_and_attest(&state.context, &protocol, &answer);
    certify_if_ready(&state, &protocol, &round, key);

    Ok(StatusCode::OK)
}

/// Counts a refusal, names it in the log, and names it in the body.
fn refuse<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    answer: &ProofOfAccess,
    reason: RefusalReason,
) -> RouteError {
    state.context.challenge_counters.refusals.record(reason);
    warn!(
        spool = %answer.spool,
        round = answer.round.0,
        epoch = answer.epoch.0,
        reason = reason.label(),
        "challenge: proof of access refused"
    );
    RouteError::BadRequest(format!("proof of access refused: {}", reason.label()))
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
    let key = round.key(payload.spool);

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
    watch_digest(&state, &protocol, payload.signer, payload.epoch, payload.digest);

    // An attestation only ever adds to a quorum, so it is safe to bank while
    // suspended, and banking it is what lets this node's own round certify. What
    // a suspended node must not do is fold an outcome, which writes a record
    // against an owner the suspect view named.
    if !state.context.challenge_tripwire.is_realigning() {
        certify_if_ready(&state, &protocol, &round, key);
    }

    Ok(StatusCode::OK)
}

/// Realigns when the group settles on a view of the epoch that is not ours.
///
/// Read only after the signature stands, so a report costs a group member's key
/// rather than a reachable port.
fn watch_digest<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    signer: Address,
    epoch: EpochNumber,
    digest: Hash,
) {
    if !state.context.epoch_digest.observe(protocol, signer, epoch, digest) {
        return;
    }
    let Some(delay) = state.context.challenge_tripwire.trip() else {
        return;
    };

    warn!(epoch = epoch.0, "challenge: group holds a different view of the epoch");
    spawn_realign(&state.context, delay, RealignCause::Divergence);
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

    use tape_core::spooler::GroupIndex;

    use crate::features::challenge::tripwire::Judgement;
    use crate::harness::{NodeHarness, TestContext};

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

    // a node re-reading its view answers no proof and no attestation, because
    // both would be weighed against the view under suspicion
    #[tokio::test]
    async fn realigning_node_takes_nothing() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        let blank = Judgement { peers: GROUP_SIZE as u64, certified: 0 };
        let rounds = ctx.config.challenge.realign_after_blank_rounds;
        for _ in 0..rounds {
            ctx.challenge_tripwire.record_round(GroupIndex(0), blank);
        }
        assert!(ctx.challenge_tripwire.is_realigning());

        let state = AppState { context: ctx.clone() };
        let proof = proof_of_access(State(state.clone()), Bytes::new()).await;

        assert!(matches!(proof.err(), Some(RouteError::Unavailable(_))));
    }

    // but it keeps taking attestations, because they only ever add to a quorum
    // and its own round needs them to certify while it re-reads
    #[tokio::test]
    async fn realigning_node_still_banks_attestations() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        assert!(ctx.challenge_tripwire.trip().is_some());
        assert!(ctx.challenge_tripwire.is_realigning());

        let state = AppState { context: ctx.clone() };
        let attestation = attest(State(state), Bytes::new()).await;

        // Refused on the body it was handed, not on the suspension.
        let message = match attestation.err() {
            Some(RouteError::BadRequest(message)) => message,
            other => panic!("unexpected refusal while realigning: {other:?}"),
        };
        assert!(message.contains("decode attestation"), "{message}");
    }
}

use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::ProofOfAccess;
use tape_core::erasure::group_for_spool;
use tape_protocol::{Api, ProtocolState};
use tape_protocol::api::{AttestationPayload, ProofOfAccessPayload};
use tracing::{trace, warn};

use crate::features::challenge::audit::{
    Round, accept_answer, attest_message, round_of, spawn_relay_and_attest,
};
use crate::features::challenge::certify::certify_if_ready;
use crate::features::challenge::refusal::RefusalReason;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;
use crate::features::state::digest::Report;

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
    certify_if_ready(&state.context, &protocol, &round, key);

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
    watch_digest(&state, &protocol, &payload);

    // An attestation only ever adds to a quorum, so it is safe to bank while
    // suspended, and banking it is what lets this node's own round certify. What
    // a suspended node must not do is fold an outcome, which writes a record
    // against an owner the suspect view named. The quorum a suspension holds up
    // is swept for when it lifts.
    if !state.context.challenge_tripwire.is_realigning() {
        certify_if_ready(&state.context, &protocol, &round, key);
    }

    Ok(StatusCode::OK)
}

/// Counts a peer's view of the epoch against this node's, and says so.
///
/// Detection only: nothing here suspends or realigns. A truthful report is
/// still not a verdict, and the arguments for acting on one do not hold yet.
/// The counters are what an operator reads, and what a soak has to show before
/// this arm is allowed to do anything.
fn watch_digest<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    payload: &AttestationPayload,
) {
    let report = state.context.epoch_digest.observe(
        protocol,
        payload.signer,
        payload.epoch,
        payload.digest,
        payload.digest_signature,
    );
    match report {
        // Nothing to say: not settled at one end, not this epoch, or unsigned.
        Report::Ignored => {}
        // Traced rather than counted. It is the ordinary case, and an operator
        // reading zero disagreements needs to know the comparison ran at all.
        Report::Agrees => trace!(
            epoch = payload.epoch.0,
            node = %payload.signer,
            "challenge: peer view agrees"
        ),
        Report::Disagrees { signers } => {
            state
                .context
                .challenge_counters
                .divergence_observed
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                epoch = payload.epoch.0,
                signers,
                node = %payload.signer,
                "challenge: a peer holds a different view of the epoch"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::spooler::GroupIndex;

    use super::*;
    use crate::features::challenge::tripwire::Judgement;
    use crate::harness::{NodeHarness, TestContext};

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

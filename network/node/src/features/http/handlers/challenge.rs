use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use rpc::Rpc;
use store::Store;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_core::erasure::{GROUP_SIZE, group_for_spool};
use tape_crypto::Address;
use tape_protocol::{Api, ProtocolState};
use tape_protocol::api::{AttestationPayload, ProofOfAccessPayload};
use tracing::{debug, trace, warn};

use crate::features::challenge::audit::{
    Round, attest_message, round_of, spawn_relay_and_attest,
};
use crate::features::challenge::certify::agreement_threshold;
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::refusal::RefusalReason;
use crate::features::challenge::rounds::RoundKey;
use crate::features::challenge::trace::MarkKind;
use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::context::NodeContext;
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

    // The group feeds the round seed, so a wrong group is a question the owner chose
    if group_for_spool(answer.spool) != answer.group {
        return Err(RouteError::BadRequest("proof spool outside the claimed group".into()));
    }

    let protocol = state.context.state();
    let round = round_of(&answer);
    let key = round.key(answer.spool);

    // A relayed duplicate is the common case, not an error: every peer that
    // accepted the answer forwards it, so most copies arrive after the first.
    if state.context.round_buffer.answer(key).is_some() {
        return Ok(StatusCode::OK);
    }

    // Only a group-mate can audit an answer: nobody else settles the round
    // or holds a stake in it. Checked in the round's epoch, which can have turned.
    if !protocol.is_member_at(answer.epoch, answer.group, state.context.node_address()) {
        return Err(RouteError::NotResponsible);
    }
    let threshold = threshold_at(&protocol, &round)?;

    // Relayed copies land before the first is stored; only one pays the pairing.
    let Some(_verifying) = state.context.round_buffer.begin_verify(key) else {
        return Ok(StatusCode::OK);
    };

    // Timeliness is left to the round, not judged per response: an answer that
    // has not certified by the time the next round opens is settled a miss
    // whenever it arrived, and no schedulable sub-round deadline separates an
    // adversary worth the honest nodes it evicts.
    // Off the executor: a pairing plus the store reads it checks against.
    let accepted = {
        let queued_at = std::time::Instant::now();
        let (reply, verdict) = tokio::sync::oneshot::channel();
        let request = crate::context::VerifyRequest {
            answer: answer.clone(),
            protocol: protocol.clone(),
            reply,
        };
        if state.context.verify_stage().send(request).is_err() {
            return Err(RouteError::Internal("verify stage closed".into()));
        }
        let accepted = verdict
            .await
            .map_err(|_| RouteError::Internal("verify worker dropped verdict".into()))?;
        let waited = queued_at.elapsed();
        if waited.as_millis() as u64 > state.context.config.challenge.ingress_wait_budget_ms {
            debug!(spool = %answer.spool, waited_ms = waited.as_millis(), "challenge: proof past its slot before verifying");
        }
        accepted
    };
    if let Err(reason) = accepted {
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
        return Err(refuse(&state, &answer, reason));
    }

    if !state.context.round_buffer.accept_answer(key, answer.clone()) {
        return Ok(StatusCode::OK);
    }

    mark(&state, &protocol, &round, key, MarkKind::AnswerIn);
    trace!(spool = %answer.spool, round = answer.round.0, "challenge: answer accepted");
    spawn_relay_and_attest(&state.context, &protocol, &answer);
    if let Some(ready) = claim_certificate(&state, &protocol, key, threshold) {
        spawn_certify_batch(&state, &round, vec![ready]);
    }

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
    active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    if !state.context.config.challenge.enabled {
        return Err(RouteError::Forbidden("challenge disabled on this node".into()));
    }

    let payload: AttestationPayload = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("decode attestation: {error}")))?;

    // Nothing here checks the signature, so the connection is what says who
    // signed: a caller naming someone else would seat a signature the aggregate
    // later refuses, and the honest signer's own would arrive as a duplicate.
    if active_peer.node != payload.signer {
        return Err(RouteError::Forbidden("attestation signer is not the calling peer".into()));
    }

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

    // Compare the authenticated signer's state report once per batch. This is
    // detection only; a disagreement never rejects its attestations.
    watch_digest(&state, &protocol, &payload);
    // A signer outside the round's epoch roster would fill a position it never had
    if !protocol.is_member_at(payload.epoch, payload.group, payload.signer) {
        return Err(RouteError::Forbidden("attestation signer is not in the group".into()));
    }
    let threshold = threshold_at(&protocol, &round)?;

    // One signature per spool in the group, each spool once, each in the group
    if payload.attests.len() > GROUP_SIZE {
        return Err(RouteError::BadRequest("attestation batch too large".into()));
    }
    let mut seen = BTreeSet::new();
    for attest in &payload.attests {
        if !seen.insert(attest.spool) {
            return Err(RouteError::BadRequest("attestation batch repeats a spool".into()));
        }
        if group_for_spool(attest.spool) != payload.group {
            return Err(RouteError::BadRequest("attestation spool outside the claimed group".into()));
        }
    }

    let mut claimed: Vec<Claimed> = Vec::new();

    // Signatures are taken on arrival and settled by the quorum aggregate,
    // which pairs once for a whole certificate. Checking
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
            // An attestation only adds to a quorum, so bank it while the view is
            // being refreshed but do not fold an outcome against that view.
            if !state.context.challenge_tripwire.is_realigning() {
                if let Some(ready) = claim_certificate(&state, &protocol, key, threshold) {
                    claimed.push(ready);
                }
            }
        }
    }

    // One pass for the whole batch. A batch crosses quorum for many spools at
    // once, and certifying each on its own put every one of them through the
    // executor and the bound separately: measured at 86-183 ms for a single
    // uncontended trip, plus 13 ms for each one queued ahead of it.
    if !claimed.is_empty() {
        spawn_certify_batch(&state, &round, claimed);
    }

    Ok(StatusCode::OK)
}
/// One spool whose quorum this node claimed, with what verifying it needs.
struct Claimed {
    key: RoundKey,
    owner: Address,
    threshold: usize,
    attestations: Vec<(Address, BlsSignature)>,
    signers: BTreeMap<Address, BlsPubkey>,
}

/// Claims every quorum that filled while realignment suspended certification.
///
/// These attestations were accepted without individual pairing checks, so the
/// sweep must use the same aggregate-and-drop-invalid path as a live arrival.
/// The older single-attestation path assumed each signature had already been
/// verified and could leave a resumed round permanently poisoned.
pub(crate) async fn certify_banked<
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
>(context: &std::sync::Arc<NodeContext<Db, Cluster, Blockchain>>) {
    let state = AppState {
        context: context.clone(),
    };
    let protocol = context.state();
    let mut batches: Vec<(Round, Vec<Claimed>)> = Vec::new();

    for key in context.round_buffer.keys() {
        let round = Round {
            epoch: key.epoch,
            group: group_for_spool(key.spool),
            round: key.round,
            block: key.block,
        };
        let Ok(threshold) = threshold_at(&protocol, &round) else {
            continue;
        };
        let Some(claimed) = claim_certificate(&state, &protocol, key, threshold) else {
            continue;
        };

        match batches.iter_mut().find(|(held, _)| held == &round) {
            Some((_, claims)) => claims.push(claimed),
            None => batches.push((round, vec![claimed])),
        }
    }

    if batches.is_empty() {
        return;
    }

    // Keep the suspension held until every aggregate has either stood or had
    // its refused signatures removed. Otherwise settlement can observe the
    // provisional claim between `claim_certificate` and verification.
    let stage = context.clone();
    let runtime = tokio::runtime::Handle::current();
    let (finished, wait) = tokio::sync::oneshot::channel();
    let job = Box::new(move || {
        for (round, claims) in batches {
            certify_batch(state.clone(), round, claims, runtime.clone());
        }
        let _ = finished.send(());
    });
    if let Err(error) = stage.certify_stage().send(job) {
        // The stage only closes during teardown. Finish inline so provisional
        // claims are still released before the suspension guard is dropped.
        (error.0)();
    }
    let _ = wait.await;
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

/// Signatures a certificate needs, sized by the roster of the round's epoch
fn threshold_at(protocol: &ProtocolState, round: &Round) -> Result<usize, RouteError> {
    protocol
        .group_member_count_at(round.epoch, round.group)
        .map(agreement_threshold)
        .ok_or(RouteError::NotResponsible)
}

/// Takes the right to certify a spool, or nothing if it is not this node's to
/// take. Cheap and synchronous, so a batch can claim every spool it carried
/// before any of them reaches the executor.
fn claim_certificate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    key: RoundKey,
    threshold: usize,
) -> Option<Claimed> {
    if !state.context.round_buffer.claim_certificate(key, threshold) {
        return None;
    }

    let owner = protocol.spool_owner(key.spool)?;
    if owner == state.context.node_address() {
        return None;
    }

    let attestations = state.context.round_buffer.attestations(key);
    // Keyed rather than scanned: the verify asks for a key per signer, and a
    // node holding several groups reaches this for every spool that certifies.
    let signers: BTreeMap<Address, BlsPubkey> = attestations
        .iter()
        .filter_map(|(signer, _)| Some((*signer, protocol.peer(*signer)?.bls_pubkey)))
        .collect();

    Some(Claimed { key, owner, threshold, attestations, signers })
}

/// Certifies everything one batch claimed, on the certify stage rather than
/// the request: handler time is the sender's cycle time
fn spawn_certify_batch<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    state: &AppState<Db, Cluster, Blockchain>,
    round: &Round,
    claimed: Vec<Claimed>,
) {
    let stage = state.context.clone();
    let state = state.clone();
    let round = *round;
    // folds still ride the blocking pool, and the worker is not a runtime thread
    let runtime = tokio::runtime::Handle::current();
    let job = Box::new(move || certify_batch(state, round, claimed, runtime));
    let _ = stage.certify_stage().send(job);
}

/// Verifies and records every quorum claimed from one incoming batch.
fn certify_batch<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    state: AppState<Db, Cluster, Blockchain>,
    round: Round,
    claimed: Vec<Claimed>,
    runtime: tokio::runtime::Handle,
) {
    let mut stood: Vec<(RoundKey, Address)> = Vec::new();
    for claim in claimed {
        if let Some(certified) = certify_claim(&state, &round, claim) {
            stood.push(certified);
        }
    }

    for (key, owner) in stood {
        // The certificate exists once the quorum verifies, so the round is
        // marked here rather than behind the write. Settlement judges a spool
        // on the round buffer, which was claimed before any of this, so a write
        // that fails cannot turn a certified round into a miss.
        state.context.round_traces.mark(
            round.epoch,
            round.round,
            round.group,
            key.spool,
            MarkKind::Certified,
            Some(owner),
        );

        // Folded now rather than waiting for the block to confirm. A certificate
        // under a candidate that loses records a success the owner may not have
        // earned, which is the harmless direction. Waiting instead would lose
        // the late certificate that replaces a recorded miss, and a miss is
        // what evicts.
        let context = state.context.clone();
        runtime.spawn_blocking(move || {
            fold_outcome(&context.store, owner, key.spool, round.epoch, round.round, true);
        });
    }
}

/// What one aggregate pass settled for a spool.
enum Verdict {
    /// The quorum verified.
    Stands,
    /// A refused signature was dropped, so what is left may still certify.
    Dropped,
    /// The aggregate failed with nothing to drop, so a retry fails the same way.
    Stuck,
}

/// Certifies one claim, retrying on the quorum left after a refused signature
/// is dropped. Nothing else re-claims: the honest attestations that arrived
/// while the failed claim was held never triggered a claim of their own, and
/// once the whole group has sent, no further arrival will.
fn certify_claim<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    round: &Round,
    claim: Claimed,
) -> Option<(RoundKey, Address)> {
    let mut claim = claim;
    loop {
        match verify_one(&state.context, round, &claim) {
            Verdict::Stands => return Some((claim.key, claim.owner)),
            Verdict::Stuck => return None,
            Verdict::Dropped => {}
        }

        let protocol = state.context.state();
        claim = claim_certificate(state, &protocol, claim.key, claim.threshold)?;
    }
}

/// Aggregates and checks one spool's quorum. This is the only pairing an honest
/// round pays, so a failure means a signature this node took on trust does not
/// stand, and the scan that follows is what names the signer.
fn verify_one<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    claim: &Claimed,
) -> Verdict {
    if aggregate_stands(round, claim) {
        return Verdict::Stands;
    }

    let has_dropped = drop_refused(context, round, claim);
    context.round_buffer.release_certificate(claim.key);
    if has_dropped {
        return Verdict::Dropped;
    }

    Verdict::Stuck
}

/// Whether the claimed quorum aggregates into a certificate that verifies.
fn aggregate_stands(round: &Round, claim: &Claimed) -> bool {
    let key = claim.key;
    let certificate = SuccessCertificate::aggregate(
        round.epoch,
        round.group,
        round.round,
        key.spool,
        round.block,
        claim.attestations.clone(),
    );
    // the summed path skips re-adding the same quorum's keys for every spool;
    // any failure falls through to the full verify, so the cache cannot refuse
    // what the slow path would accept
    let summed = quorum_key(claim).is_some_and(|quorum| {
        certificate.as_ref().is_some_and(|certificate| {
            certificate.verify_summed(claim.threshold, claim.owner, &quorum).is_ok()
        })
    });

    summed
        || certificate.as_ref().is_some_and(|certificate| {
            certificate
                .verify(claim.threshold, claim.owner, |signer| {
                    claim.signers.get(&signer).copied()
                })
                .inspect_err(|rejection| {
                    debug!(spool = %key.spool, ?rejection, "challenge: certificate refused");
                })
                .is_ok()
        })
}

/// Drops the signatures the aggregate refused, and reports whether any went.
///
/// One bad signature fails the whole aggregate, so the quorum is rebuilt
/// without whoever sent it rather than the round being lost.
fn drop_refused<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    claim: &Claimed,
) -> bool {
    let key = claim.key;
    let message = attest_message(round, key.spool).to_bytes();
    let mut has_dropped = false;
    for (signer, signature) in &claim.attestations {
        // A signer with no key on record cannot verify either, so it goes with
        // the refused rather than leaving the round stuck.
        let is_refused = match claim.signers.get(signer) {
            Some(pubkey) => signature.verify_aggregate(&message, core::slice::from_ref(pubkey)).is_err(),
            None => true,
        };
        if is_refused {
            has_dropped |= context.round_buffer.drop_attestation(key, *signer);
            debug!(spool = %key.spool, node = %signer, "challenge: attestation dropped");
        }
    }

    has_dropped
}

/// The quorum's summed key, cached per signer set since every spool a round
/// certifies shares one.
fn quorum_key(claim: &Claimed) -> Option<tape_core::bls::BlsQuorumKey> {
    use std::hash::{Hash, Hasher};
    thread_local! {
        static SUMS: std::cell::RefCell<std::collections::HashMap<u64, tape_core::bls::BlsQuorumKey>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (signer, _) in &claim.attestations {
        signer.hash(&mut hasher);
    }
    let set = hasher.finish();

    SUMS.with(|sums| {
        let mut sums = sums.borrow_mut();
        if let Some(quorum) = sums.get(&set) {
            return Some(*quorum);
        }
        let keys: Option<Vec<_>> = claim
            .attestations
            .iter()
            .map(|(signer, _)| claim.signers.get(signer).copied())
            .collect();
        let quorum = tape_core::bls::BlsQuorumKey::sum(&keys?).ok()?;
        if sums.len() > 64 {
            sums.clear();
        }
        sums.insert(set, quorum);
        Some(quorum)
    })
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

#[cfg(test)]
mod tests {
    use tape_core::challenge::proof::SampleProof;
    use tape_core::erasure::{GROUP_SIZE, group_for_spool};
    use tape_core::spooler::GroupIndex;
    use tape_core::types::RoundNumber;
    use tape_core::types::tls::NetworkTlsPubkey;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    // a node re-reading its view answers no proof, because it would be weighed
    // against the view under suspicion
    #[tokio::test]
    async fn realigning_node_takes_nothing() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        let rounds = ctx.config.challenge.realign_after_blank_rounds;
        for _ in 0..rounds {
            ctx.challenge_tripwire.record_blank_round(GroupIndex(0));
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
        let active_peer = ActivePeer {
            node: ctx.node_address(),
            tls_pubkey: NetworkTlsPubkey::new([0; 32]),
        };
        let attestation = attest(State(state), active_peer, Bytes::new()).await;

        // Refused on the body it was handed, not on the suspension.
        let message = match attestation.err() {
            Some(RouteError::BadRequest(message)) => message,
            other => panic!("unexpected refusal while realigning: {other:?}"),
        };
        assert!(message.contains("decode attestation"), "{message}");
    }

    // The resume sweep uses the post-#119 aggregate verifier: one forged
    // signature is removed and the honest quorum behind it still certifies.
    #[tokio::test]
    async fn banked_quorum_drops_a_forged_signature() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        let protocol = ctx.state();
        let mine = protocol
            .member_spools(ctx.node_address())
            .first()
            .copied()
            .expect("this node holds a spool");
        let group = group_for_spool(mine);
        let target = protocol
            .group_peers(group)
            .into_iter()
            .map(|(spool, _)| spool)
            .find(|spool| *spool != mine)
            .expect("another spool");
        let round = Round {
            epoch: protocol.epoch(),
            group,
            round: RoundNumber(7),
            block: tape_crypto::hash::Hash([0x53; 32]),
        };
        let key = round.key(target);
        let dummy = ProofOfAccess {
            epoch: round.epoch,
            group,
            round: round.round,
            spool: target,
            block: round.block,
            track: Address::new_unique(),
            proof: SampleProof::Inline { payload: Vec::new() },
            signature: harness.node(0).bls_keypair().sign(b"dummy").expect("sign"),
        };
        assert!(ctx.round_buffer.accept_answer(key, dummy));

        let threshold = agreement_threshold(GROUP_SIZE);
        let message = attest_message(&round, target).to_bytes();
        let members: Vec<Address> = protocol
            .group_peers(group)
            .into_iter()
            .map(|(_, owner)| owner)
            .collect();
        for signer in members.iter().take(threshold) {
            let node = (0..25)
                .find(|index| Address::from(harness.node(*index).node_address.to_bytes()) == *signer)
                .expect("group member in harness");
            let signature = harness
                .node(node)
                .bls_keypair()
                .sign(message)
                .expect("sign attestation");
            assert!(ctx.round_buffer.accept_attestation(key, *signer, signature));
        }

        let forged = members[threshold];
        let forged_node = (0..25)
            .find(|index| Address::from(harness.node(*index).node_address.to_bytes()) == forged)
            .expect("forged signer in harness");
        let bad_signature = harness
            .node(forged_node)
            .bls_keypair()
            .sign(b"another round")
            .expect("sign wrong message");
        assert!(ctx.round_buffer.accept_attestation(key, forged, bad_signature));

        certify_banked(&ctx).await;

        assert!(ctx.round_buffer.is_certified(key));
        assert_eq!(ctx.round_buffer.attestations(key).len(), threshold);
        assert!(!ctx
            .round_buffer
            .attestations(key)
            .iter()
            .any(|(signer, _)| *signer == forged));
    }
}

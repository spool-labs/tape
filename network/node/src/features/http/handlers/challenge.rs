use std::collections::{BTreeMap, BTreeSet};

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
use tracing::{debug, trace};

use crate::features::challenge::audit::{
    Round, accept_answer, attest_message, group_members, round_of, spawn_relay_and_attest,
};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::rounds::RoundKey;
use crate::features::challenge::trace::MarkKind;
use crate::features::http::error::RouteError;
use crate::context::NodeContext;
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
    // adversary worth the honest nodes it evicts.
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
    if let Some(ready) = claim_certificate(&state, &protocol, &round, key) {
        spawn_certify_batch(&state, &round, vec![ready]);
    }

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
    let mut claimed: Vec<Claimed> = Vec::new();

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
            if let Some(ready) = claim_certificate(&state, &protocol, &round, key) {
                claimed.push(ready);
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

/// Takes the right to certify a spool, or nothing if it is not this node's to
/// take. Cheap and synchronous, so a batch can claim every spool it carried
/// before any of them reaches the executor.
fn claim_certificate<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    round: &Round,
    key: RoundKey,
) -> Option<Claimed> {
    let threshold = agreement_threshold(group_members(protocol, round.group).len());
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
    let job = Box::new(move || {
        let stood: Vec<_> = claimed
            .into_iter()
            .filter(|claim| verify_one(&state.context, &round, claim))
            .map(|claim| (claim.key, claim.owner))
            .collect();

        for (key, owner) in stood {
            // The certificate exists once the quorum verifies, so the round is
            // marked here rather than behind the write. Settlement judges a
            // spool on the round buffer, which was claimed before any of this,
            // so a write that fails cannot turn a certified round into a miss.
            state.context.round_traces.mark(
                round.epoch,
                round.round,
                round.group,
                key.spool,
                MarkKind::Certified,
                Some(owner),
            );

            // Folded now rather than waiting for the block to finalize. A
            // certificate under a candidate that loses records a success the
            // owner may not have earned, which is the harmless direction.
            // Waiting instead would lose the late certificate that replaces a
            // recorded miss, and a miss is what evicts. `settle_previous`
            // refuses to charge a miss for a round that never finalized, which
            // is the half that has teeth.
            let context = state.context.clone();
            runtime.spawn_blocking(move || {
                fold_outcome(&context.store, owner, key.spool, round.epoch, round.round, true);
            });
        }
    });
    let _ = stage.certify_stage().send(job);
}

/// Aggregates and checks one spool's quorum. This is the only pairing an honest
/// round pays, so a failure means a signature this node took on trust does not
/// stand, and the scan below is what names the signer.
fn verify_one<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    claim: &Claimed,
) -> bool {
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
    let stands = summed
        || certificate.as_ref().is_some_and(|certificate| {
            certificate
                .verify(claim.threshold, claim.owner, |signer| {
                    claim.signers.get(&signer).copied()
                })
                .inspect_err(|rejection| {
                    debug!(spool = %key.spool, ?rejection, "challenge: certificate refused");
                })
                .is_ok()
        });
    if stands {
        return true;
    }

    // One bad signature fails the whole aggregate, so the quorum is rebuilt
    // without whoever sent it rather than the round being lost.
    let message = attest_message(round, key.spool).to_bytes();
    for (signer, signature) in &claim.attestations {
        let Some(pubkey) = claim.signers.get(signer) else {
            continue;
        };
        if signature.verify_aggregate(&message, core::slice::from_ref(pubkey)).is_err() {
            context.round_buffer.drop_attestation(key, *signer);
            debug!(spool = %key.spool, node = %signer, "challenge: attestation dropped");
        }
    }
    context.round_buffer.release_certificate(key);
    false
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

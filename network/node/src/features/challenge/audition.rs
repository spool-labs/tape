//! Answering a round, and auditioning the answers the rest of the group gave.
//!
//! Every owner derives its own sample, reads the bytes, signs, and broadcasts. An
//! owner that receives someone else's answer derives the same question from
//! replayed state, checks the answer against it, and only then relays it onward
//! and signs an attestation. Relaying is what stops a spool choosing who hears
//! it: a peer it skipped receives the answer from a peer it served.

use std::sync::Arc;

use futures::future::{join, join_all};
use rpc::Rpc;
use store::Store;
use tape_core::cert::challenge::{ChallengeAttestMessage, ChallengeRespondMessage};
use tape_core::challenge::{self, ProofOfAccess, SampleEntry};
use tape_core::challenge::proof::{Registered, SampleProof};
use tape_core::challenge::sample::SampleLeaf;
use tape_core::challenge::sample::Sample;
use tape_core::erasure::{SUB_LEAF_BYTES, prove_sub_leaf_windowed, sample_window};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::{AttestReq, ProofOfAccessReq};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::{SampleOps, SliceOps, TrackDataOps};
use tracing::{debug, trace};

use crate::context::NodeContext;
use crate::features::challenge::manager::challenge_schedule;
use crate::features::challenge::rounds::RoundKey;

/// Peers an accepting owner forwards the answer to.
///
/// The owner already broadcast to the whole group, so relaying exists only to
/// reach members it deliberately skipped. One hop from a handful of accepting
/// peers covers those; flooding every acceptance to every peer turns one round
/// into hundreds of forwards per spool and adds nothing, which the simulation
/// measured before this was built.
const RELAY_FANOUT: usize = 3;

/// One round's coordinates, shared by everyone who takes part in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Round {
    /// Epoch the round belongs to.
    pub epoch: EpochNumber,
    /// Spool group being asked.
    pub group: GroupIndex,
    /// Position of the round within the epoch.
    pub round: RoundNumber,
    /// Hash of the candidate entropy block the samples are drawn from.
    pub block: Hash,
}

impl Round {
    pub fn key(&self, spool: SpoolIndex) -> RoundKey {
        RoundKey {
            epoch: self.epoch,
            round: self.round,
            spool,
            block: self.block,
        }
    }
}

/// The sample a spool owes this round, from replayed state.
///
/// Rows are written when a registration replays, so every owner enumerates the
/// same entries. Reading local holdings diverged instead: a write certifies at
/// q of n, so some members hold no slice and drew a different question.
///
/// Cut at the round window's base slot, on the two slots the chain records.
pub fn expected_sample<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
    spool: SpoolIndex,
) -> Option<(Sample, Hash)> {
    if round.epoch != state.epoch() {
        return None;
    }
    let cutoff = challenge_schedule(state)?.sample_cutoff(round.round);

    // Already in track order: the rows are keyed group-then-track, so the scan
    // arrives in the canonical order the draw is defined over.
    let rows = context
        .store
        .iter_track_samples_by_group(round.group)
        .map_err(|error| debug!(%error, "challenge: sample set unavailable"))
        .ok()?;
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|(_, sample)| sample.in_set_at(cutoff))
        .collect();
    let entries: Vec<SampleEntry> = rows
        .iter()
        .map(|(track, sample)| SampleEntry {
            track: *track,
            kind: sample.kind,
        })
        .collect();

    let seed = challenge::round_seed(&round.block, round.epoch, round.group, round.round, spool);
    let sample = challenge::draw(&seed, &entries)?;

    // The row's own value hash travels with the draw: an inline answer is
    // checked against it, and the track record it came from may be gone.
    let value_hash = rows
        .iter()
        .find(|(track, _)| *track == sample.track)
        .map(|(_, row)| row.value_hash)?;
    Some((sample, value_hash))
}

/// Build this node's own answer for a round.
pub fn build_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
    spool: SpoolIndex,
) -> Option<ProofOfAccess> {
    let (sample, _) = expected_sample(context, state, round, spool)?;
    let proof = match sample.leaf {
        SampleLeaf::Coded { sub_leaf } => {
            let slice = context.store.get_slice(spool, sample.track).ok().flatten()?;
            let sidecar = context.store.get_slice_sidecar(spool, sample.track).ok().flatten()?;

            let start = sub_leaf * SUB_LEAF_BYTES;
            SampleProof::Coded {
                sub_leaf: sub_leaf as u64,
                proof: SubLeafProof {
                    sub_leaf: slice[start..(start + SUB_LEAF_BYTES).min(slice.len())].to_vec(),
                    sub_proof: prove_sub_leaf_windowed(
                        &sidecar,
                        &slice[sample_window(sub_leaf, slice.len())],
                        sub_leaf,
                    )?,
                },
            }
        }
        // An inline write is kept whole by every owner in the group, so the
        // payload is what there is to show. A node that has not caught up on
        // the bytes cannot answer, exactly as it cannot for a slice it lacks.
        SampleLeaf::Inline => {
            let BlobData::Inline(payload) =
                context.store.get_track_data(sample.track).ok().flatten()?
            else {
                debug!(track = %sample.track, "challenge: inline payload not held");
                return None;
            };
            SampleProof::Inline { payload }
        }
    };

    let message = ChallengeRespondMessage::new(
        round.epoch,
        round.group,
        round.round,
        spool,
        round.block,
        proof.signed_leaf(),
    );

    Some(ProofOfAccess {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        spool,
        block: round.block,
        track: sample.track,
        proof,
        signature: context.bls_sign(&message.to_bytes()).ok()?,
    })
}

/// Check an answer someone broadcast, against the question we derived ourselves.
pub fn accept_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
    in_time: bool,
) -> bool {
    // Every refusal below costs the answering owner a miss it may not have
    // earned, so each one says which of them it was.
    let Some((expected, value_hash)) = expected_sample(context, state, &round_of(answer), answer.spool)
    else {
        debug!(spool = %answer.spool, "challenge: no question of our own to check against");
        return false;
    };

    // What the answer is checked against comes from the row the draw came from
    // for an inline track, and from the registered encoding for a coded one. A
    // deleted track keeps its row while its record is gone, so reading the hash
    // back off the record would refuse an honest answer mid-round.
    let coded = context.store.get_track_data(expected.track).ok().flatten();
    let registered = match (expected.leaf, &coded) {
        (SampleLeaf::Coded { .. }, Some(BlobData::Coded(encoding))) => Registered::Coded(encoding),
        (SampleLeaf::Inline, _) => Registered::Inline(value_hash),
        _ => {
            debug!(spool = %answer.spool, track = %expected.track, "challenge: track encoding not held");
            return false;
        }
    };

    let Some(owner) = state.spool_owner(answer.spool) else {
        debug!(spool = %answer.spool, "challenge: spool has no owner in our view");
        return false;
    };
    let Some(peer) = state.peer(owner) else {
        debug!(node = %owner, spool = %answer.spool, "challenge: owner has no registered key");
        return false;
    };

    match answer.verify(&expected, registered, &peer.bls_pubkey, in_time) {
        Ok(()) => true,
        Err(rejection) => {
            debug!(node = %owner, spool = %answer.spool, ?rejection, "challenge: answer refused");
            false
        }
    }
}

/// Relay an accepted answer onward and push our attestation for it.
///
/// Runs off the request path. Every peer that accepts an answer calls this, and
/// each call talks to the rest of the group, so doing it inside the handler makes
/// one delivery wait on a fan-out that is itself triggering more handlers.
pub fn spawn_relay_and_attest<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
) {
    spawn_attest(context, state, answer, true)
}

/// Sign and fan out this node's attestation for an answer it holds.
///
/// The challenged owner calls this for its own round with `relay` off: it has
/// already broadcast to the whole group, and its own signature is one of the q
/// the certificate needs. Excluding it costs a position the threshold cannot
/// spare, since the threshold counts the owner among the members but can never
/// count it among the signers.
pub fn spawn_attest<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
    relay: bool,
) {
    let me = context.node_address();
    let round = round_of(answer);

    let Ok(signature) = context.bls_sign(&attest_message(&round, answer.spool).to_bytes()) else {
        return;
    };
    context
        .round_buffer
        .accept_attestation(round.key(answer.spool), me, signature);

    let peers: Vec<Address> = group_members(state, answer.group)
        .into_iter()
        .filter(|peer| *peer != me)
        .collect();
    let attestation = AttestReq {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        spool: answer.spool,
        block: round.block,
        signer: me,
        signature,
    };
    let context = context.clone();
    let answer = answer.clone();

    tokio::spawn(async move {
        // Relay to a few, attest to all. An attestation is a hundred bytes and
        // every peer needs a quorum of them to certify; the answer is kilobytes
        // and only the skipped need another copy.
        let fanout = if relay { RELAY_FANOUT } else { 0 };
        let relays = peers
            .iter()
            .take(fanout)
            .map(|peer| relay_answer(&context, *peer, answer.clone()));
        let attestations = peers
            .iter()
            .map(|peer| send_attestation(&context, *peer, &attestation));

        join(join_all(relays), join_all(attestations)).await;
    });
}

/// Forward an accepted answer to one peer.
async fn relay_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    peer: Address,
    answer: ProofOfAccess,
) {
    if let Err(error) = context
        .api
        .proof_of_access(peer, &ProofOfAccessReq { answer })
        .await
    {
        trace!(node = %peer, %error, "challenge: relay failed");
    }
}

/// Push this node's attestation for a round to one peer.
async fn send_attestation<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    peer: Address,
    attestation: &AttestReq,
) {
    if let Err(error) = context.api.attest(peer, attestation).await {
        trace!(node = %peer, %error, "challenge: attestation not delivered");
    }
}

/// The message every accepting observer signs for a round.
pub fn attest_message(round: &Round, spool: SpoolIndex) -> ChallengeAttestMessage {
    ChallengeAttestMessage::new(round.epoch, round.group, round.round, spool, round.block)
}

/// The round an answer belongs to.
pub fn round_of(answer: &ProofOfAccess) -> Round {
    Round {
        epoch: answer.epoch,
        group: answer.group,
        round: answer.round,
        block: answer.block,
    }
}

/// Every node holding a spool in a group.
pub fn group_members(state: &ProtocolState, group: GroupIndex) -> Vec<Address> {
    let mut members = Vec::new();
    let Some(spools) = state.spools_in_group(group) else {
        return members;
    };

    for (spool, _) in spools {
        if let Some(owner) = state.spool_owner(spool) {
            if !members.contains(&owner) {
                members.push(owner);
            }
        }
    }

    members
}

//! Answering a round, and witnessing what the rest of the group answered.
//!
//! Every owner derives its own sample, reads the bytes, signs, and broadcasts. An
//! owner that receives someone else's answer derives the same question from its
//! own view, checks the answer against it, and only then relays it onward and
//! signs an attestation. Relaying is what stops a spool choosing who hears it:
//! a peer it skipped receives the answer from a peer it served.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::challenge::{ChallengeAttestMessage, ChallengeRespondMessage};
use tape_core::challenge::{self, ProofOfAccess, SampleEntry};
use tape_core::challenge::sample::Sample;
use tape_core::erasure::{SUB_LEAF_BYTES, prove_sub_leaf_windowed, sample_window};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::hash_leaf;
use tape_protocol::api::{AttestReq, ProofOfAccessReq};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::{ChallengeOps, SliceOps, TrackDataOps};
use tracing::{debug, trace};

use crate::context::NodeContext;
use crate::features::challenge::rounds::RoundKey;

/// One round's coordinates, shared by everyone who takes part in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Round {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
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

/// The sample a spool owes this round, derived from local state alone.
///
/// Every member of a group holds a slice of the same tracks, so any of them
/// reaches the same enumeration and therefore the same question. Nothing an
/// answering owner sends is used to arrive at it.
pub fn expected_sample<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    spool: SpoolIndex,
    from_spool: SpoolIndex,
) -> Option<Sample> {
    let entries: Vec<SampleEntry> = context
        .store
        .iter_slice_sizes_by_spool(from_spool)
        .map_err(|error| debug!(%error, "challenge: sample set unavailable"))
        .ok()?
        .into_iter()
        .map(|(track, slice_len)| SampleEntry { track, slice_len })
        .collect();

    let seed = challenge::round_seed(&round.block, round.epoch, round.group, round.round, spool);
    challenge::draw(&seed, &entries)
}

/// Build this node's own answer for a round.
pub fn build_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    spool: SpoolIndex,
) -> Option<ProofOfAccess> {
    let sample = expected_sample(context, round, spool, spool)?;
    let slice = context.store.get_slice(spool, sample.track).ok().flatten()?;
    let sidecar = context.store.get_slice_sidecar(spool, sample.track).ok().flatten()?;

    let start = sample.sub_leaf * SUB_LEAF_BYTES;
    let proof = SubLeafProof {
        sub_leaf: slice[start..(start + SUB_LEAF_BYTES).min(slice.len())]
            .to_vec(),
        sub_proof: prove_sub_leaf_windowed(
            &sidecar,
            &slice[sample_window(sample.sub_leaf, slice.len())],
            sample.sub_leaf,
        )?,
    };

    let message = ChallengeRespondMessage::new(
        round.epoch,
        round.group,
        round.round,
        spool,
        round.block,
        hash_leaf(&proof.sub_leaf),
    );

    Some(ProofOfAccess {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        spool,
        block: round.block,
        track: sample.track,
        sub_leaf: sample.sub_leaf as u64,
        proof,
        signature: context.bls_sign(&message.to_bytes()).ok()?,
    })
}

/// Check an answer someone broadcast, against the question we derived ourselves.
pub fn accept_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
    mine: SpoolIndex,
    in_time: bool,
) -> bool {
    let Some(expected) = expected_sample(context, &round_of(answer), answer.spool, mine) else {
        return false;
    };

    let Some(BlobData::Coded(encoding)) = context.store.get_track_data(expected.track).ok().flatten()
    else {
        return false;
    };

    let Some(owner) = state.spool_owner(answer.spool) else {
        return false;
    };
    let Some(peer) = state.peer(owner) else {
        return false;
    };

    match answer.verify(&expected, &encoding, &peer.bls_pubkey, in_time) {
        Ok(()) => true,
        Err(rejection) => {
            debug!(node = %owner, spool = %answer.spool, ?rejection, "challenge: answer refused");
            false
        }
    }
}

/// Peers an accepting owner forwards the answer to.
///
/// The owner already broadcast to the whole group, so relaying exists only to
/// reach members it deliberately skipped. One hop from a handful of witnesses
/// covers those; flooding every acceptance to every peer turns one round into
/// hundreds of forwards per spool and adds nothing, which the simulation measured
/// before this was built.
const RELAY_FANOUT: usize = 3;

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
    let spool = answer.spool;
    let context = context.clone();
    let answer = answer.clone();

    tokio::spawn(async move {
        // Relay to a few, attest to all. An attestation is a hundred bytes and
        // every peer needs a quorum of them to certify; the answer is kilobytes
        // and only the skipped need another copy.
        let relays = peers.iter().take(RELAY_FANOUT).map(|peer| {
            let context = context.clone();
            let answer = answer.clone();
            async move {
                if let Err(error) = context
                    .api
                    .proof_of_access(*peer, &ProofOfAccessReq { answer })
                    .await
                {
                    trace!(node = %peer, %error, "challenge: relay failed");
                }
            }
        });

        let attestations = peers.iter().map(|peer| {
            let context = context.clone();
            async move {
                let sent = context
                    .api
                    .attest(
                        *peer,
                        &AttestReq {
                            epoch: round.epoch,
                            group: round.group,
                            round: round.round,
                            spool,
                            block: round.block,
                            signer: me,
                            signature,
                        },
                    )
                    .await;
                if let Err(error) = sent {
                    trace!(node = %peer, %error, "challenge: attestation not delivered");
                }
            }
        });

        futures::future::join(
            futures::future::join_all(relays),
            futures::future::join_all(attestations),
        )
        .await;
    });
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


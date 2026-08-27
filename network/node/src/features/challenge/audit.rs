use std::sync::Arc;

use crate::features::challenge::attest_queue::BatchKey;


use futures::future::join_all;
use rpc::Rpc;
use store::Store;
use tape_core::cert::challenge::{ChallengeAttestMessage, ChallengeRespondMessage};
use tape_core::bls::BlsPubkey;
use tape_core::challenge::{self, ProofOfAccess, SampleEntry};
use tape_core::challenge::proof::{Registered, SampleProof};
use tape_core::challenge::sample::SampleLeaf;
use tape_core::challenge::sample::{Sample, sample_space};
use tape_core::challenge::schedule::{SETTLE_DEADLINE_SLOTS, SLOT_MS};
use tape_core::erasure::{
    SAMPLE_WINDOW_LEAVES, SUB_LEAF_BYTES, prove_sub_leaf_windowed, sample_window_range,
};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::{AttestReq, ProofOfAccessReq, SpoolAttestation};
use tape_protocol::{Api, ProtocolState};
use tape_store::types::TrackSample;
use tape_store::ops::{SampleOps, SliceOps, TrackDataOps};
use tokio::time::timeout;
use tracing::{debug, trace};

use crate::context::NodeContext;
use crate::features::challenge::manager::schedule_for;
use crate::features::challenge::rounds::RoundKey;
use crate::features::challenge::trace::MarkKind;

// Reach peers the owner skipped without amplifying every accepted answer to the
// entire group again.
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

/// Returns whether the group has a non-empty sampling space for this round.
///
/// Empty and unavailable sets void the round instead of charging every owner a
/// miss.
pub fn has_sample_set<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
) -> bool {
    sample_space(&set_entries(context, state, round).1) > 0
}

/// The sample set for one round, shared by every answer drawn against it.
pub type SampleSet = (Vec<(Address, TrackSample)>, Vec<SampleEntry>);

/// One round's sample set, built once and reused.
///
/// Every answer in a round draws from the same set, and the set is a scan of
/// the group's rows. Deriving it per answer meant a receiver walked the group
/// once for each of the twenty answers it verified, which at a round a second
/// across five groups is the same scan run thousands of times a second for one
/// unchanging result.
///
/// Safe to hold because the set cannot move once the round exists. It is the
/// rows registered before a cutoff a finality window behind the round, and a
/// node only takes part while at tip, so it has applied past that cutoff
/// already: a later write carries a slot far above it and a later deletion
/// records one too. A store edited behind the ingest path can outrun this, and
/// says so with `invalidate`.
fn set_entries<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
) -> Arc<SampleSet> {
    let key = (round.epoch, round.round, round.group);
    if let Some(held) = context.sample_sets.get(&key) {
        return held;
    }
    // Read before the store is, so a backdated row landing mid-build is not
    // stamped as though the set already held it.
    let built_at = context.sample_sets.generation();
    let built = Arc::new(build_set_entries(context, state, round));
    context.sample_sets.put(key, built_at, built.clone());
    built
}

fn build_set_entries<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
) -> SampleSet {
    let Some(schedule) = schedule_for(state, round.epoch) else {
        return (Vec::new(), Vec::new());
    };
    let cutoff = schedule.sample_cutoff(round.round);

    // Already in track order: the rows are keyed group-then-track, so the scan
    // arrives in the canonical order the draw is defined over.
    let rows: Vec<(Address, TrackSample)> = context
        .store
        .iter_track_samples_by_group(round.group)
        .map_err(|error| debug!(%error, "challenge: sample set unavailable"))
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, sample)| sample.in_set_at(cutoff))
        .collect();
    let entries = rows
        .iter()
        .map(|(track, sample)| SampleEntry {
            track: *track,
            kind: sample.kind,
        })
        .collect();
    (rows, entries)
}

/// Derives the sample a spool owes from replayed state at the round's cutoff.
///
/// The round's epoch determines the cutoff even when settlement crosses an
/// epoch boundary.
pub fn expected_sample<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
    spool: SpoolIndex,
) -> Option<(Sample, Hash)> {
    let held = set_entries(context, state, round);
    let (rows, entries) = (&held.0, &held.1);

    let seed = challenge::round_seed(&round.block, round.epoch, round.group, round.round, spool);
    let sample = challenge::draw(&seed, &entries)?;

    // The row's own value hash travels with the draw: an inline answer is
    // checked against it, and the track record it came from may be gone.
    let value_hash = rows
        .binary_search_by_key(&sample.track, |(track, _)| *track)
        .ok()
        .map(|index| rows[index].1.value_hash)?;
    Some((sample, value_hash))
}

pub fn build_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    round: &Round,
    spool: SpoolIndex,
) -> Option<ProofOfAccess> {
    let (sample, _) = expected_sample(context, state, round, spool)?;
    let proof = match sample.leaf {
        SampleLeaf::Coded { sub_leaf } => {
            // One window of the slice and the sidecar above it, which is every
            // byte a proof reads. The rest of the slice stays on the device.
            let window = sample_window_range(sub_leaf);
            let (sidecar, bytes) = context
                .store
                .slice_window(spool, sample.track, window.start, window.len())
                .ok()
                .flatten()?;
            if sidecar.is_empty() {
                debug!(track = %sample.track, "challenge: slice has no sidecar");
                return None;
            }

            let at = (sub_leaf % SAMPLE_WINDOW_LEAVES) * SUB_LEAF_BYTES;
            SampleProof::Coded {
                sub_leaf: sub_leaf as u64,
                proof: SubLeafProof {
                    sub_leaf: bytes.get(at..(at + SUB_LEAF_BYTES).min(bytes.len()))?.to_vec(),
                    sub_proof: prove_sub_leaf_windowed(&sidecar, &bytes, sub_leaf)?,
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

/// Everything `accept_answer` checks except the signature, plus the key that
/// signature has to stand against.
///
/// Lets a caller holding a round's worth of answers check their signatures in
/// one pairing product instead of one each.
pub fn answer_signer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
    in_time: bool,
) -> Option<BlsPubkey> {
    let (expected, value_hash) = expected_sample(context, state, &round_of(answer), answer.spool)?;
    let coded = context.store.get_track_data(expected.track).ok().flatten();
    let registered = match (expected.leaf, &coded) {
        (SampleLeaf::Coded { .. }, Some(BlobData::Coded(encoding))) => Registered::Coded(encoding),
        (SampleLeaf::Inline, _) => Registered::Inline(value_hash),
        _ => return None,
    };

    let owner = state.spool_owner(answer.spool)?;
    let peer = state.peer(owner)?;
    answer
        .verify_shape(&expected, registered, in_time)
        .inspect_err(|rejection| {
            debug!(node = %owner, spool = %answer.spool, ?rejection, "challenge: answer refused");
        })
        .ok()?;
    Some(peer.bls_pubkey)
}

pub fn spawn_relay_and_attest<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    answer: &ProofOfAccess,
) {
    spawn_attest(context, state, answer, true)
}

/// Signs and broadcasts an attestation, optionally relaying the answer.
///
/// The challenged owner passes `false` because it has already broadcast its
/// answer. Its own attestation still contributes to the certificate threshold.
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
    context.round_traces.mark(
        round.epoch,
        round.round,
        round.group,
        answer.spool,
        MarkKind::AttestOut,
        Some(me),
    );

    let members = group_members(state, answer.group);
    // Where this node sits in the group, which is what spreads the relay copies.
    let at = members.iter().position(|peer| *peer == me).unwrap_or_default();
    let peers: Vec<Address> = members.into_iter().filter(|peer| *peer != me).collect();

    // One round's signatures ride together. This one joins the batch; the first
    // of a round opens it and owns the sender that carries all of them.
    let batch = BatchKey {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        block: round.block,
    };
    let opened = context.attest_queue.push(batch, answer.spool, signature);

    let context = context.clone();

    // Relays go on their own task. Nothing correct waits on them, and gating the
    // round's signatures behind three kilobyte round-trips put every peer's
    // attestations behind the slowest relay target.
    if relay {
        let context = context.clone();
        let answer = answer.clone();
        let targets = relay_targets(&peers, at, RELAY_FANOUT);
        tokio::spawn(async move {
            let relays = targets
                .into_iter()
                .map(|peer| relay_answer(&context, peer, answer.clone()));
            join_all(relays).await;
        });
    }

    let Some(gathered) = opened else {
        return;
    };

    // One sender per peer: a shared barrier let the slowest response gate every
    // peer's next batch, so signatures left in waves
    for peer in peers {
        let context = context.clone();
        let mut gathered = gathered.clone();
        tokio::spawn(async move {
            let mut sent = 0usize;
            loop {
                // Posts before waiting: the signature that opened the batch is
                // already in it, so the first pass costs no delay of its own.
                let fresh = context.attest_queue.since(&batch, sent);
                if !fresh.is_empty() {
                    sent += fresh.len();
                    let attestation = AttestReq {
                        epoch: batch.epoch,
                        group: batch.group,
                        round: batch.round,
                        block: batch.block,
                        signer: me,
                        attests: fresh
                            .into_iter()
                            .map(|(spool, signature)| SpoolAttestation { spool, signature })
                            .collect(),
                    };
                    send_attestation(&context, peer, &attestation).await;
                }

                if !context.attest_queue.is_open(&batch) {
                    return;
                }

                // A signature landing mid-post moved the count, so this returns
                // at once; an error is the batch retiring
                match timeout(SENDER_IDLE, gathered.changed()).await {
                    Ok(Ok(())) => {}
                    _ => return,
                }
            }
        });
    }
}

/// Which peers this relayer forwards an answer to.
///
/// Taking the first few in group order gave every relayer the same targets, so
/// the copies piled onto members one to three and an owner withholding from
/// anyone else was never healed. Starting at the relayer's own place in the
/// group covers every member exactly `fanout` times over, which is the property
/// the relay exists for.
pub(crate) fn relay_targets(peers: &[Address], at: usize, fanout: usize) -> Vec<Address> {
    if peers.is_empty() || fanout == 0 {
        return Vec::new();
    }
    (0..fanout.min(peers.len()))
        .map(|step| peers[(at + step) % peers.len()])
        .collect()
}

/// Longest a challenge-path post is worth waiting on.
///
/// The client default is thirty seconds, which outlives the round many times
/// over. Sized against settlement rather than the round: a post that lands late
/// still counts if it beats the block rooting, and cutting at the round's own
/// length turned slow-but-certifying spools into misses.
const POST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

/// Longest a round's sender waits before giving up. Retirement normally ends
/// it; this bounds the senders left parked by a node that leaves every group.
const SENDER_IDLE: std::time::Duration =
    std::time::Duration::from_millis(SETTLE_DEADLINE_SLOTS * SLOT_MS);

async fn relay_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    peer: Address,
    answer: ProofOfAccess,
) {
    let sent = tokio::time::timeout(
        POST_TIMEOUT,
        context.api.proof_of_access(peer, &ProofOfAccessReq { answer }),
    )
    .await;
    match sent {
        Ok(Err(error)) => trace!(node = %peer, %error, "challenge: relay failed"),
        Err(_) => trace!(node = %peer, "challenge: relay timed out"),
        Ok(Ok(_)) => {}
    }
}

async fn send_attestation<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    peer: Address,
    attestation: &AttestReq,
) {
    match tokio::time::timeout(POST_TIMEOUT, context.api.attest(peer, attestation)).await {
        Ok(Err(error)) => trace!(node = %peer, %error, "challenge: attestation not delivered"),
        Err(_) => trace!(node = %peer, "challenge: attestation timed out"),
        Ok(Ok(_)) => {}
    }
}

pub fn attest_message(round: &Round, spool: SpoolIndex) -> ChallengeAttestMessage {
    ChallengeAttestMessage::new(round.epoch, round.group, round.round, spool, round.block)
}

pub fn round_of(answer: &ProofOfAccess) -> Round {
    Round {
        epoch: answer.epoch,
        group: answer.group,
        round: answer.round,
        block: answer.block,
    }
}

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

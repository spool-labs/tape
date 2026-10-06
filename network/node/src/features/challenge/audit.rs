use std::sync::Arc;

use crate::features::challenge::attest_queue::BatchKey;
use futures::future::join_all;
use tape_rpc::Rpc;
use tape_store_api::Store;
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
use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SlotNumber, SpoolIndex, VirtualGroup};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::{AttestReq, ProofOfAccessReq, SpoolAttestation};
use tape_protocol::{Api, ProtocolState};
use tape_store::types::TrackSample;
use tape_store::ops::{MetaOps, SampleOps, SliceOps, TrackDataOps};
use tokio::time::timeout;
use tracing::{debug, trace};

use crate::context::NodeContext;
use crate::features::challenge::refusal::RefusalReason;
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

/// Whether this node can still settle the round, so evidence about it can land
pub fn admits_round<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
) -> Result<(), RefusalReason> {
    // No grid for the epoch means this node asked nothing of it and settles
    // none of its rounds.
    let Some(schedule) = context.schedules.get(round.epoch) else {
        return Err(RefusalReason::NoLocalQuestion);
    };

    let progress = context.ingest.progress();
    let behind = schedule
        .round_by(SlotNumber(progress.last_dispatched_slot()))
        .unwrap_or(RoundNumber(0));
    // Ahead comes from the chain tip: a node behind still owes its peers' rounds
    let ahead = schedule
        .round_by(SlotNumber(progress.known_tip().unwrap_or(progress.last_dispatched_slot())))
        .unwrap_or(behind);
    let span = schedule.settling_rounds();
    let window = behind.as_u64().saturating_sub(span)..=ahead.as_u64().saturating_add(span);
    if !window.contains(&round.round.as_u64()) {
        return Err(RefusalReason::OutOfWindow);
    }

    // The entropy block seeds the draw, so a round opened on another block is a
    // different question and no evidence about it reaches the one asked here.
    match context.round_traces.block_for(round.epoch, round.round, round.group) {
        Some(block) if block != round.block => Err(RefusalReason::NoLocalQuestion),
        Some(_) | None => Ok(()),
    }
}

/// Returns whether the group has a non-empty sampling space for this round.
///
/// Empty and unavailable sets void the round instead of charging every owner a
/// miss.
pub fn has_sample_set<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
) -> bool {
    set_entries(context, round).is_some_and(|held| sample_space(&held.1) > 0)
}

/// The sample set for one round, shared by every answer drawn against it.
/// Each row carries the virtual group its track was stamped with.
pub type SampleSet = (Vec<(Address, VirtualGroup, TrackSample)>, Vec<SampleEntry>);

/// One round's sample set, built once and reused, or nothing while this node's
/// store has yet to reach the round's cut.
///
/// Every answer in a round draws from the same set, and the set is a scan of
/// the group's rows. Deriving it per answer meant a receiver walked the group
/// once for each of the twenty answers it verified, which at a round a second
/// across five groups is the same scan run thousands of times a second for one
/// unchanging result.
///
/// The set is the rows registered before a cut behind the round, and it stops
/// moving once the store has applied every block before that cut: a later write
/// lands at a slot past it and a later deletion records one too. A node takes
/// part up to the settlement deadline behind the tip, and a store short of the
/// cut would build a set missing rows, draw a different sample, and refuse
/// honest answers or send a wrong one. So nothing is built or held until the
/// store reaches the cut, and the next ask after that builds the set. A store
/// edited behind the ingest path can outrun this, and says so with
/// `backdated_write`.
fn set_entries<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
) -> Option<Arc<SampleSet>> {
    let key = (round.epoch, round.round, round.group);
    if let Some(held) = context.sample_sets.get(&key) {
        return Some(held);
    }

    // Nothing for an epoch this node never laid a grid for, so it asks no
    // question of that epoch's rounds and answers none of them either.
    let schedule = context.schedules.get(round.epoch)?;
    let cutoff = schedule.sample_cutoff(round.round);
    if !store_reaches(context, cutoff) {
        return None;
    }

    // Read before the store is, so a backdated row landing mid-build is not
    // stamped as though the set already held it.
    let built_at = context.sample_sets.generation();
    let built = Arc::new(build_set_entries(context, round, cutoff)?);
    context.sample_sets.put(key, built_at, built.clone());
    Some(built)
}

/// Whether the store has applied every block before a round's cut, which is
/// what makes the set drawn from it final.
fn store_reaches<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    cutoff: SlotNumber,
) -> bool {
    match context.store.get_sync_cursor() {
        Ok(Some(applied)) => applied.as_u64().saturating_add(1) >= cutoff.as_u64(),
        Ok(None) => false,
        Err(error) => {
            debug!(%error, "challenge: store frontier unreadable");
            false
        }
    }
}

fn build_set_entries<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    cutoff: SlotNumber,
) -> Option<SampleSet> {
    // A live group answers for every virtual group folded onto it in the round's
    // epoch. A state that no longer holds that epoch can't say which, so no set.
    let fold = context.state().fold_at(round.epoch)?;

    // A live group carries hundreds of virtual groups in about a dozen runs of
    // ids, so the set is read one run at a time.
    let mut rows: Vec<(Address, VirtualGroup, TrackSample)> = Vec::new();
    for groups in fold.carried_ranges(round.group) {
        let scanned = context
            .store
            .iter_track_samples_in(groups)
            .map_err(|error| debug!(%error, "challenge: sample set unavailable"))
            .ok()?;

        rows.extend(
            scanned
                .into_iter()
                .filter(|(_, _, sample)| sample.in_set_at(cutoff))
                .map(|(group, track, sample)| (track, group, sample)),
        );
    }

    // Each scan arrives by group and then track. The draw is defined over one
    // track order across every group carried, so the scans are merged into it.
    rows.sort_unstable_by_key(|(track, _, _)| *track);

    let entries = rows
        .iter()
        .map(|(track, _, sample)| SampleEntry {
            track: *track,
            kind: sample.kind,
        })
        .collect();
    Some((rows, entries))
}

/// Derives the sample a spool owes from replayed state at the round's cutoff.
///
/// The round's epoch determines the cutoff even when settlement crosses an
/// epoch boundary.
pub fn expected_sample<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    spool: SpoolIndex,
) -> Option<(Sample, Hash)> {
    drawn_row(context, round, spool).map(|(sample, _, value_hash)| (sample, value_hash))
}

/// The sample a spool owes, with the virtual group and value hash of its track.
fn drawn_row<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    spool: SpoolIndex,
) -> Option<(Sample, VirtualGroup, Hash)> {
    let held = set_entries(context, round)?;
    let (rows, entries) = (&held.0, &held.1);

    let seed = challenge::round_seed(&round.block, round.epoch, round.group, round.round, spool);
    let sample = challenge::draw(&seed, &entries)?;

    // The row's own value hash travels with the draw: an inline answer is
    // checked against it, and the track record it came from may be gone.
    let index = rows
        .binary_search_by_key(&sample.track, |(track, _, _)| *track)
        .ok()?;
    let (_, group, row) = &rows[index];

    Some((sample, *group, row.value_hash))
}

pub fn build_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    round: &Round,
    spool: SpoolIndex,
) -> Option<ProofOfAccess> {
    let (sample, virtual_group, _) = drawn_row(context, round, spool)?;
    let proof = match sample.leaf {
        SampleLeaf::Coded { sub_leaf } => {
            // The slice sits under the track's own group, at this spool's
            // position in the live group that carries it.
            let stored = virtual_group.spool_at(round.group.position_of(spool)?);

            // One window of the slice and the sidecar above it, which is every
            // byte a proof reads. The rest of the slice stays on the device.
            let window = sample_window_range(sub_leaf);
            let (sidecar, bytes) = context
                .store
                .slice_window(stored, sample.track, window.start, window.len())
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
) -> Result<(), RefusalReason> {
    let Some((expected, value_hash)) = expected_sample(context, &round_of(answer), answer.spool)
    else {
        return Err(RefusalReason::NoLocalQuestion);
    };

    // What the answer is checked against comes from the row the draw came from
    // for an inline track, and from the registered encoding for a coded one. A
    // deleted track keeps its row while its record is gone, so reading the hash
    // back off the record would refuse an honest answer mid-round.
    let coded = context.store.get_track_data(expected.track).ok().flatten();
    let registered = match (expected.leaf, &coded) {
        (SampleLeaf::Coded { .. }, Some(BlobData::Coded(encoding))) => Registered::Coded(encoding),
        (SampleLeaf::Inline, _) => Registered::Inline(value_hash),
        _ => return Err(RefusalReason::EncodingNotHeld),
    };

    let Some(owner) = state.spool_owner_at(answer.epoch, answer.spool) else {
        return Err(RefusalReason::UnknownOwner);
    };
    let Some(peer) = state.peer(owner) else {
        return Err(RefusalReason::UnknownKey);
    };

    answer
        .verify(&expected, registered, &peer.bls_pubkey, in_time)
        .map_err(RefusalReason::from)
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
) -> Result<BlsPubkey, RefusalReason> {
    let (expected, value_hash) = expected_sample(context, &round_of(answer), answer.spool)
        .ok_or(RefusalReason::NoLocalQuestion)?;
    let coded = context.store.get_track_data(expected.track).ok().flatten();
    let registered = match (expected.leaf, &coded) {
        (SampleLeaf::Coded { .. }, Some(BlobData::Coded(encoding))) => Registered::Coded(encoding),
        (SampleLeaf::Inline, _) => Registered::Inline(value_hash),
        _ => return Err(RefusalReason::EncodingNotHeld),
    };

    let owner = state
        .spool_owner_at(answer.epoch, answer.spool)
        .ok_or(RefusalReason::UnknownOwner)?;
    let peer = state.peer(owner).ok_or(RefusalReason::UnknownKey)?;
    answer
        .verify_shape(&expected, registered, in_time)
        .map_err(RefusalReason::from)?;
    Ok(peer.bls_pubkey)
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
    let (digest, digest_signature) = context
        .epoch_digest
        .signed(state, me, |message| context.bls_sign(message).ok());
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

    // Nobody to post to, so a batch opened here would hold signatures nothing sends
    let Some(members) = group_members_at(state, answer.epoch, answer.group) else {
        return;
    };
    // Where this node sits in the group, which is what spreads the relay copies.
    let at = members.iter().position(|peer| *peer == me).unwrap_or_default();
    let peers: Vec<Address> = members.into_iter().filter(|peer| *peer != me).collect();
    if peers.is_empty() {
        return;
    }

    // One round's signatures ride together. This one joins the batch; the first
    // of a round opens it and owns the sender that carries all of them.
    let batch = BatchKey {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        block: round.block,
    };
    let opened = context.attest_queue.push(batch, answer.spool, signature, peers.len());

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
                        digest,
                        digest_signature,
                        attests: fresh
                            .into_iter()
                            .map(|(spool, signature)| SpoolAttestation { spool, signature })
                            .collect(),
                    };
                    send_attestation(&context, peer, &attestation).await;
                }

                if !context.attest_queue.is_open(&batch) {
                    break;
                }

                // A signature landing mid-post moved the count, so this returns
                // at once; an error is the batch retiring
                match timeout(SENDER_IDLE, gathered.changed()).await {
                    Ok(Ok(())) => {}
                    _ => break,
                }
            }
            // Counted out so a signature arriving later gets a sender of its own
            context.attest_queue.sender_done(&batch);
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
    state
        .virtual_group(group)
        .map(|own| owners_of(state.group_peers(own)))
        .unwrap_or_default()
}

/// The group's roster in the round's epoch, or nothing once the state no longer holds it
pub fn group_members_at(
    state: &ProtocolState,
    epoch: EpochNumber,
    group: GroupIndex,
) -> Option<Vec<Address>> {
    let own = state.virtual_group_at(epoch, group)?;

    state.group_peers_at(epoch, own).map(owners_of)
}

fn owners_of(peers: Vec<(SpoolIndex, Address)>) -> Vec<Address> {
    let mut members = Vec::new();
    for (_, owner) in peers {
        if !members.contains(&owner) {
            members.push(owner);
        }
    }
    members
}

#[cfg(test)]
mod tests {
    use tape_core::challenge::sample::EntryKind;
    use tape_core::types::StorageUnits;
    use tape_store::ops::{MetaOps, SampleOps};
    use tape_store::types::TrackSample;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    /// Where a live epoch sits, far enough in that a round's cut is past zero.
    const START_SLOT: SlotNumber = SlotNumber(10_000);

    /// A node with one row in a group and a grid laid for its epoch.
    async fn seeded() -> (TestContext, Round) {
        let harness = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx: TestContext = harness.ctx_for(0);
        let mut state = (*ctx.state()).clone();
        state.current.epoch.start_slot = START_SLOT;
        ctx.set_state(state.clone()).expect("publish");
        ctx.schedules.observe(&state, START_SLOT);

        let group = GroupIndex(0);
        let row = TrackSample {
            kind: EntryKind::Coded {
                slice_len: StorageUnits::from_bytes(4_096),
            },
            value_hash: Hash([1; 32]),
            registered_slot: SlotNumber(0),
            deleted_slot: None,
        };
        ctx.store
            .put_track_sample(
                state.virtual_group(group).expect("a live group"),
                Address::new_unique(),
                row,
            )
            .expect("sample row");

        let round = Round {
            epoch: state.epoch(),
            group,
            round: RoundNumber(1),
            block: Hash([2; 32]),
        };
        (ctx, round)
    }

    // a store short of the round's cut holds no set and keeps none, so the set
    // built once the store reaches the cut is the full one
    #[tokio::test]
    async fn store_frontier() {
        let (ctx, round) = seeded().await;
        let cutoff = ctx
            .schedules
            .get(round.epoch)
            .expect("a grid")
            .sample_cutoff(round.round);
        let key = (round.epoch, round.round, round.group);

        ctx.store
            .set_sync_cursor(SlotNumber(cutoff.as_u64() - 2), None)
            .expect("cursor");

        assert!(!has_sample_set(&ctx, &round));
        assert!(ctx.sample_sets.get(&key).is_none());

        ctx.store
            .set_sync_cursor(SlotNumber(cutoff.as_u64() - 1), None)
            .expect("cursor");

        assert!(has_sample_set(&ctx, &round));
        assert!(ctx.sample_sets.get(&key).is_some());
    }
}

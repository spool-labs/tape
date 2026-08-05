//! One challenge round driven end to end through the real functions.
//!
//! Every owner in a group answers its own question, the group verifies, attests,
//! and the attestations aggregate into a certificate. These drive the production
//! path directly rather than over HTTP, so what is under test is the mechanism:
//! what a node derives, what it accepts, and what a quorum then means.

use std::collections::HashMap;

use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::cert::challenge::ChallengeRespondMessage;
use tape_core::challenge::{ProofOfAccess, Sample, SuccessCertificate};
use tape_core::challenge::certificate::CertificateRejection;
use tape_core::erasure::{
    GROUP_SIZE, SUB_LEAF_BYTES, group_for_spool, leaf_position, prove_sub_leaf_windowed,
    sample_window, slice_sidecar, sub_leaf_count,
};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::{
    EpochDuration, EpochNumber, GroupIndex, RoundNumber, SlotNumber, SpoolIndex, StorageUnits,
};
use tape_crypto::Address;
use tape_crypto::hash::{Hash, hash};
use tape_crypto::merkle::hash_leaf;
use tape_protocol::ProtocolState;
use tape_core::challenge::proof::SampleProof;
use tape_core::challenge::sample::{EntryKind, SampleLeaf};
use tape_store::types::TrackSample;
use tape_store::ops::{SampleOps, SliceOps, TrackDataOps};

use crate::features::challenge::fold::holds_spool;
use crate::features::challenge::audition::{
    Round, accept_answer, attest_message, expected_sample,
};
use crate::features::challenge::manager::challenge_schedule;
use crate::features::http::handlers::challenge::agreement_threshold;
use crate::harness::{NodeHarness, TestContext, coded_track};

const PAYLOAD_BYTES: usize = 300_000;
const BLOCK: Hash = Hash([0x7C; 32]);

/// A group whose members all hold known keys, plus the slices they each store.
struct Fixture {
    /// This node, with the store the round reads from.
    ctx: TestContext,
    /// The chain state every derivation in the round runs against.
    state: ProtocolState,
    /// This node's own spool.
    mine: SpoolIndex,
    /// The group all of the fixture's spools sit in.
    group: GroupIndex,
    /// One slice per group position, in position order.
    slices: Vec<Vec<u8>>,
    /// Signing key per spool, so any owner's answer can be forged honestly.
    keys: HashMap<SpoolIndex, BlsPrivateKey>,
}

async fn fixture() -> Fixture {
    let harness = NodeHarness::builder()
        .nodes(25)
        .no_prev_snapshot_tape()
        .build()
        .await
        .expect("build harness");

    let ctx = harness.ctx_for(0);
    let mut state = (*ctx.state()).clone();
    // The harness epoch carries no duration, and the schedule the sample cut
    // derives from refuses an epoch too short to hold a round.
    state.current.epoch.preferences.epoch_duration = EpochDuration(100);
    let mine = state
        .member_spools(ctx.node_address())
        .first()
        .copied()
        .expect("this node holds a spool");
    let group = group_for_spool(mine);

    // Give every owner in the group a key this test can sign with, standing in
    // for the keys they registered on chain.
    let mut keys = HashMap::new();
    for spool in group_spools(&state, group) {
        let key = BlsPrivateKey::from_random();
        if let Some(owner) = state.spool_owner(spool) {
            let pubkey = key.public_key().expect("pubkey");
            set_peer_key(&mut state, owner, pubkey);
        }
        keys.insert(spool, key);
    }

    // One coded track, with this node holding its own slice of it.
    let (slices, encoding) = coded_track(PAYLOAD_BYTES, 0x1234_5678_9ABC_DEF0);
    let track = Address::new_unique();
    ctx.store
        .put_track_data(track, BlobData::Coded(encoding))
        .expect("track data");
    ctx.store
        .put_slice(mine, track, slices[leaf_position(mine).as_usize()].clone())
        .expect("put slice");
    put_sample(&ctx, group, track, slices[0].len(), SlotNumber(0));

    Fixture {
        ctx,
        state,
        mine,
        group,
        slices,
        keys,
    }
}

/// Put a track into a group's sample set, as a replayed registration would.
fn put_sample(
    ctx: &TestContext,
    group: GroupIndex,
    track: Address,
    slice_len: usize,
    registered_slot: SlotNumber,
) {
    ctx.store
        .put_track_sample(
            group,
            track,
            TrackSample {
                kind: EntryKind::Coded {
                    slice_len: StorageUnits::from_bytes(slice_len as u64),
                },
                value_hash: Hash::from([0u8; 32]),
                registered_slot,
                deleted_slot: None,
            },
        )
        .expect("sample row");
}

/// The coded leaf a round asks a spool for, for tests whose fixture is coded.
fn asked_leaf(sample: &Sample) -> usize {
    match sample.leaf {
        SampleLeaf::Coded { sub_leaf } => sub_leaf,
        SampleLeaf::Inline => panic!("the fixture holds coded tracks only"),
    }
}

fn group_spools(state: &ProtocolState, group: GroupIndex) -> Vec<SpoolIndex> {
    state
        .spools_in_group(group)
        .map(|spools| spools.map(|(spool, _)| spool).collect())
        .unwrap_or_default()
}

fn set_peer_key(state: &mut ProtocolState, node: Address, pubkey: BlsPubkey) {
    if let Some(peer) = state.peers.iter_mut().find(|peer| peer.node == node) {
        peer.bls_pubkey = pubkey;
    }
}

// a spool that changes hands leaves the old owner's history behind, and judging
// on it would charge a peer for data it is no longer asked about
#[tokio::test]
async fn a_handed_off_spool_stops_counting() {
    let fixture = fixture().await;
    let spool = fixture.mine;
    let owner = fixture.state.spool_owner(spool).expect("the spool has an owner");
    let stranger = Address::new_unique();

    assert!(holds_spool(&fixture.state, owner, spool));
    assert!(
        !holds_spool(&fixture.state, stranger, spool),
        "a peer that never held the spool was judged on it"
    );

    // The previous epoch still counts, since a round opened before a boundary
    // settles after it.
    let mut rolled = fixture.state.clone();
    rolled.previous = Some(rolled.current.clone());
    rolled.current.groups = Default::default();
    assert!(
        holds_spool(&rolled, owner, spool),
        "a boundary round lost the owner it was opened against"
    );

    // One epoch further on, the history is nobody's business.
    let mut aged = rolled.clone();
    aged.previous = Some(aged.current.clone());
    assert!(
        !holds_spool(&aged, owner, spool),
        "a spool handed on two epochs ago still judged its old owner"
    );
}

// a round opened before the boundary is still answerable after it: the question
// comes off the epoch the round belongs to, whose bundle the state still keeps
#[tokio::test]
async fn a_round_survives_its_epoch_ending() {
    let mut fixture = fixture().await;
    let round = fixture.round();
    let spool = fixture.mine;

    let asked = expected_sample(&fixture.ctx, &fixture.state, &round, spool)
        .expect("a sample while the epoch is current");

    // The epoch turns, carrying the one that just closed into `previous`, which
    // is where settling reads a boundary round from.
    let closing = fixture.state.current.clone();
    fixture.state.previous = Some(closing);
    fixture.state.current.epoch.id = EpochNumber(fixture.state.epoch().as_u64() + 1);

    let after = expected_sample(&fixture.ctx, &fixture.state, &round, spool)
        .expect("the same sample once the epoch has turned");
    assert_eq!(asked.0, after.0, "the question changed under the boundary");
    assert_eq!(asked.1, after.1);
}

impl Fixture {
    fn round(&self) -> Round {
        Round {
            epoch: self.state.epoch(),
            group: self.group,
            round: RoundNumber(3),
            block: BLOCK,
        }
    }

    /// Build the answer one spool owes this round, signed by its own key.
    fn answer_from(&self, spool: SpoolIndex) -> ProofOfAccess {
        self.answer_from_leaf(spool, None)
    }

    /// The same, optionally answering a leaf other than the one it was asked.
    fn answer_from_leaf(&self, spool: SpoolIndex, leaf: Option<usize>) -> ProofOfAccess {
        let round = self.round();
        let (asked, _) =
            expected_sample(&self.ctx, &self.state, &round, spool).expect("a sample");
        let SampleLeaf::Coded { sub_leaf: drawn } = asked.leaf else {
            panic!("the fixture holds coded tracks only");
        };
        let sub_leaf = leaf.unwrap_or(drawn);

        let position = leaf_position(spool);
        let slice = &self.slices[position.as_usize()];
        let start = sub_leaf * SUB_LEAF_BYTES;
        let proof = SubLeafProof {
            sub_leaf: slice[start..(start + SUB_LEAF_BYTES).min(slice.len())].to_vec(),
            sub_proof: prove_sub_leaf_windowed(
                &slice_sidecar(slice).expect("sidecar"),
                &slice[sample_window(sub_leaf, slice.len())],
                sub_leaf,
            )
            .expect("path"),
        };

        let message = ChallengeRespondMessage::new(
            round.epoch,
            round.group,
            round.round,
            spool,
            round.block,
            hash_leaf(&proof.sub_leaf),
        );

        ProofOfAccess {
            epoch: round.epoch,
            group: round.group,
            round: round.round,
            spool,
            block: round.block,
            track: asked.track,
            proof: SampleProof::Coded {
                sub_leaf: sub_leaf as u64,
                proof,
            },
            signature: self.keys[&spool].sign(message.to_bytes()).expect("sign"),
        }
    }

    /// Attestations from `count` owners other than the one being challenged.
    fn attestations(&self, target: SpoolIndex, count: usize) -> Vec<(Address, BlsSignature)> {
        let message = attest_message(&self.round(), target).to_bytes();
        group_spools(&self.state, self.group)
            .into_iter()
            .filter(|spool| *spool != target)
            .take(count)
            .map(|spool| {
                (
                    self.state.spool_owner(spool).expect("owner"),
                    self.keys[&spool].sign(message).expect("sign"),
                )
            })
            .collect()
    }

    fn certify(&self, target: SpoolIndex, signed: Vec<(Address, BlsSignature)>) -> SuccessCertificate {
        let round = self.round();
        SuccessCertificate::aggregate(
            round.epoch,
            round.group,
            round.round,
            target,
            round.block,
            signed,
        )
        .expect("aggregate")
    }

    fn check(&self, target: SpoolIndex, certificate: &SuccessCertificate) -> Result<(), CertificateRejection> {
        let owner = self.state.spool_owner(target).expect("owner");
        certificate.verify(agreement_threshold(GROUP_SIZE), owner, |signer| {
            self.state.peer(signer).map(|peer| peer.bls_pubkey)
        })
    }

    /// A spool in this group that is not ours.
    fn other(&self) -> SpoolIndex {
        group_spools(&self.state, self.group)
            .into_iter()
            .find(|spool| *spool != self.mine)
            .expect("another spool")
    }
}

// the whole path: an owner answers the question the group derived, every other
// owner accepts it, and their attestations combine into a certificate that
// stands against the registered keys
#[tokio::test]
async fn honest_round() {
    let fixture = fixture().await;
    let target = fixture.other();
    let answer = fixture.answer_from(target);

    assert!(accept_answer(
        &fixture.ctx,
        &fixture.state,
        &answer,
        true
    ));

    let signed = fixture.attestations(target, agreement_threshold(GROUP_SIZE));
    let certificate = fixture.certify(target, signed);
    assert_eq!(fixture.check(target, &certificate), Ok(()));
}

// the seed binds the spool, so two owners challenged off one block read
// different data, where otherwise one answer would serve the whole group
#[tokio::test]
async fn distinct_questions() {
    let fixture = fixture().await;
    let round = fixture.round();

    let leaves: Vec<usize> = group_spools(&fixture.state, fixture.group)
        .into_iter()
        .filter_map(|spool| {
            expected_sample(&fixture.ctx, &fixture.state, &round, spool)
        })
        .map(|(sample, _)| asked_leaf(&sample))
        .collect();

    assert_eq!(leaves.len(), GROUP_SIZE);
    let distinct: std::collections::HashSet<usize> = leaves.iter().copied().collect();
    assert!(distinct.len() > 1, "every spool drew the same leaf: {leaves:?}");
}

// a spool that kept only some of its data would answer about a leaf it still
// holds, and the observer derived the question, so that does not pass
#[tokio::test]
async fn easier_leaf() {
    let fixture = fixture().await;
    let target = fixture.other();
    let (asked, _) = expected_sample(&fixture.ctx, &fixture.state, &fixture.round(), target)
        .expect("sample");

    // Wrap, so a draw that lands on the last leaf still names a real neighbour.
    let leaves = sub_leaf_count(fixture.slices[0].len());
    let elsewhere = (asked_leaf(&asked) + 1) % leaves;
    assert_ne!(elsewhere, asked_leaf(&asked));

    // The same fixture accepts the leaf it did ask for, so this is refusing the
    // substitution rather than failing for some unrelated reason.
    assert!(accept_answer(
        &fixture.ctx,
        &fixture.state,
        &fixture.answer_from(target),
        true
    ));
    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &fixture.answer_from_leaf(target, Some(elsewhere)),
        true
    ));
}

// the set is cut at the round window's base slot, so a write finalizing inside
// the round leaves the draw where it was, and observers that ingested it at
// different moments still derive the same question
#[tokio::test]
async fn mid_round_write() {
    let fixture = fixture().await;
    let round = fixture.round();
    let (before, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");

    let cutoff = challenge_schedule(&fixture.state)
        .expect("schedule")
        .sample_cutoff(round.round);
    let late = Address::new_unique();
    put_sample(&fixture.ctx, fixture.group, late, 4 * SUB_LEAF_BYTES, cutoff);

    let (after, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");
    assert_eq!(after.track, before.track, "a mid-round write shifted the draw");
    assert_eq!(asked_leaf(&after), asked_leaf(&before));

    // One slot earlier and the registration is inside the window, which does
    // move the draw.
    put_sample(
        &fixture.ctx,
        fixture.group,
        late,
        4 * SUB_LEAF_BYTES,
        SlotNumber(cutoff.as_u64() - 1),
    );
    let (inside, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");
    assert_ne!(
        (inside.track, asked_leaf(&inside)),
        (before.track, asked_leaf(&before)),
        "a registration before the window stayed out of the set"
    );
}

// the mirror of the registration cut: a track deleted after the window opened
// stays in the set, so an observer that has processed the delete and one that
// has not still draw the same question
#[tokio::test]
async fn mid_round_delete() {
    let fixture = fixture().await;
    let round = fixture.round();
    let cutoff = challenge_schedule(&fixture.state)
        .expect("schedule")
        .sample_cutoff(round.round);

    let extra = Address::new_unique();
    put_sample(&fixture.ctx, fixture.group, extra, 4 * SUB_LEAF_BYTES, SlotNumber(0));
    let (before, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");

    fixture
        .ctx
        .store
        .mark_track_sample_deleted(fixture.group, extra, cutoff)
        .expect("delete at the cut");

    let (after, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");
    assert_eq!(after.track, before.track, "a mid-round deletion shifted the draw");
    assert_eq!(asked_leaf(&after), asked_leaf(&before));

    // Once the deletion predates the window, the entry is out of the set.
    put_sample(&fixture.ctx, fixture.group, extra, 4 * SUB_LEAF_BYTES, SlotNumber(0));
    fixture
        .ctx
        .store
        .mark_track_sample_deleted(fixture.group, extra, SlotNumber(cutoff.as_u64() - 1))
        .expect("delete before the cut");
    let (out, _) = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine)
        .expect("a sample");
    assert_ne!(out.track, extra, "a pre-window deletion stayed in the set");
}

// an inline track is answered with its payload, and the group accepts it
#[tokio::test]
async fn inline_round() {
    let fixture = fixture().await;
    let target = fixture.other();
    let round = fixture.round();

    // A track every owner keeps whole, and a group whose set holds only it, so
    // the draw is bound to land there.
    let payload = b"a small object that rides inside the write".to_vec();
    let track = Address::new_unique();
    fixture
        .ctx
        .store
        .put_track_data(track, BlobData::Inline(payload.clone()))
        .expect("inline data");
    for (held, _) in fixture
        .ctx
        .store
        .iter_track_samples_by_group(fixture.group)
        .expect("rows")
    {
        fixture
            .ctx
            .store
            .mark_track_sample_deleted(fixture.group, held, SlotNumber(0))
            .expect("clear the coded rows");
    }
    fixture
        .ctx
        .store
        .put_track_sample(
            fixture.group,
            track,
            TrackSample {
                kind: EntryKind::Inline,
                value_hash: hash(&payload),
                registered_slot: SlotNumber(0),
                deleted_slot: None,
            },
        )
        .expect("inline row");

    let (asked, _) = expected_sample(&fixture.ctx, &fixture.state, &round, target)
        .expect("a sample");
    assert_eq!(asked.track, track);
    assert_eq!(asked.leaf, SampleLeaf::Inline);

    let proof = SampleProof::Inline {
        payload: payload.clone(),
    };
    let message = ChallengeRespondMessage::new(
        round.epoch,
        round.group,
        round.round,
        target,
        round.block,
        proof.signed_leaf(),
    );
    let answer = ProofOfAccess {
        epoch: round.epoch,
        group: round.group,
        round: round.round,
        spool: target,
        block: round.block,
        track,
        proof,
        signature: fixture.keys[&target].sign(message.to_bytes()).expect("sign"),
    };

    assert!(accept_answer(&fixture.ctx, &fixture.state, &answer, true));

    // A payload that does not hash to the registered value is refused.
    let mut rotten = answer.clone();
    rotten.proof = SampleProof::Inline {
        payload: b"not what was written".to_vec(),
    };
    assert!(!accept_answer(&fixture.ctx, &fixture.state, &rotten, true));
}

// a leaf that does not hash into the track's commitment is refused
#[tokio::test]
async fn forged_leaf() {
    let fixture = fixture().await;
    let target = fixture.other();
    let mut answer = fixture.answer_from(target);
    if let SampleProof::Coded { proof, .. } = &mut answer.proof {
        proof.sub_leaf[0] ^= 0xFF;
    }

    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &answer,
        true
    ));
}

// the signature is checked against the key registered for the spool being
// challenged, so answering on another spool's behalf fails
#[tokio::test]
async fn wrong_owner() {
    let fixture = fixture().await;
    let target = fixture.other();
    let mut answer = fixture.answer_from(target);
    answer.signature = fixture.keys[&fixture.mine]
        .sign(answer.message().to_bytes())
        .expect("sign");

    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &answer,
        true
    ));
}

// an answer that arrives after the round's deadline is refused
#[tokio::test]
async fn late_answer() {
    let fixture = fixture().await;
    let target = fixture.other();
    let answer = fixture.answer_from(target);

    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &answer,
        false
    ));
}

// nothing to attest to means nothing aggregates, which is the one thing the
// mechanism catches: a spool that answers nobody
#[tokio::test]
async fn silent_spool() {
    let fixture = fixture().await;
    let target = fixture.other();

    let short = fixture.attestations(target, agreement_threshold(GROUP_SIZE) - 1);
    let certificate = fixture.certify(target, short);
    assert_eq!(
        fixture.check(target, &certificate),
        Err(CertificateRejection::BelowQuorum)
    );
}

// the challenged owner's own signature counts toward its quorum: the
// threshold counts it among the group's members, so a group at the fault
// bound has thirteen other honest signers against a threshold of fourteen
#[tokio::test]
async fn owner_counts() {
    let fixture = fixture().await;
    let target = fixture.other();
    let owner = fixture.state.spool_owner(target).expect("owner");

    let short = fixture.attestations(target, agreement_threshold(GROUP_SIZE) - 1);
    let certificate = fixture.certify(target, short.clone());
    assert_eq!(
        fixture.check(target, &certificate),
        Err(CertificateRejection::BelowQuorum),
        "one short of the threshold is not a quorum"
    );

    let mut signed = short;
    signed.push((
        owner,
        fixture.keys[&target]
            .sign(attest_message(&fixture.round(), target).to_bytes())
            .expect("sign"),
    ));

    let certificate = fixture.certify(target, signed);
    assert_eq!(fixture.check(target, &certificate), Ok(()));
}

// signers who saw different candidate blocks signed different bytes, so an
// aggregate mixing them cannot claim a quorum agreed on one history
#[tokio::test]
async fn two_branches() {
    let fixture = fixture().await;
    let target = fixture.other();

    let threshold = agreement_threshold(GROUP_SIZE);
    let mut signed = fixture.attestations(target, threshold - 1);

    let other_branch = Round {
        block: Hash([0x7D; 32]),
        ..fixture.round()
    };
    let stray = group_spools(&fixture.state, fixture.group)
        .into_iter()
        .filter(|spool| *spool != target)
        .nth(threshold)
        .expect("another signer");
    signed.push((
        fixture.state.spool_owner(stray).expect("owner"),
        fixture.keys[&stray]
            .sign(attest_message(&other_branch, target).to_bytes())
            .expect("sign"),
    ));

    let certificate = fixture.certify(target, signed);
    assert_eq!(
        fixture.check(target, &certificate),
        Err(CertificateRejection::BadAggregate)
    );
}

// the group is in the round seed, so an owner free to name it could grind the
// draw onto a leaf it kept; the group a spool belongs to is what binds it
#[tokio::test]
async fn stray_group() {
    let fixture = fixture().await;
    let target = fixture.other();
    let mut answer = fixture.answer_from(target);
    answer.group = GroupIndex(fixture.group.as_u64() + 1);

    assert!(!accept_answer(&fixture.ctx, &fixture.state, &answer, true));
}

// acceptance is a function of the answer and this node's own derivation, with
// no notion of a sender in it, so a peer the target skipped accepts the same
// bytes forwarded by a peer it served
#[tokio::test]
async fn any_deliverer() {
    let fixture = fixture().await;
    let target = fixture.other();
    let answer = fixture.answer_from(target);

    // Byte-identical copies, as a relay would forward them.
    let forwarded = answer.clone();
    assert_eq!(forwarded, answer);
    assert!(accept_answer(&fixture.ctx, &fixture.state, &forwarded, true));

    let signed = fixture.attestations(target, agreement_threshold(GROUP_SIZE));
    let certificate = fixture.certify(target, signed);
    assert_eq!(fixture.check(target, &certificate), Ok(()));
}

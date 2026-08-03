//! One challenge round driven end to end through the real functions.
//!
//! Every owner in a group answers its own question, the group verifies, attests,
//! and the attestations aggregate into a certificate. These drive the production
//! path directly rather than over HTTP, so what is under test is the mechanism:
//! what a node derives, what it accepts, and what a quorum then means.

use std::collections::HashMap;

use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::cert::challenge::ChallengeRespondMessage;
use tape_core::challenge::{ProofOfAccess, SuccessCertificate};
use tape_core::challenge::certificate::CertificateRejection;
use tape_core::erasure::{
    GROUP_SIZE, SUB_LEAF_BYTES, group_for_spool, leaf_position, prove_sub_leaf_windowed,
    sample_window, slice_sidecar, sub_leaf_count,
};
use tape_core::track::blob::SubLeafProof;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochDuration, GroupIndex, RoundNumber, SlotNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_crypto::merkle::hash_leaf;
use tape_protocol::ProtocolState;
use tape_store::ops::{SliceOps, TrackDataOps, TrackOps};
use tape_store::types::SliceTombstone;

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

    Fixture {
        ctx,
        state,
        mine,
        group,
        slices,
        keys,
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
        let asked =
            expected_sample(&self.ctx, &self.state, &round, spool, self.mine).expect("a sample");
        let sub_leaf = leaf.unwrap_or(asked.sub_leaf);

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
            sub_leaf: sub_leaf as u64,
            proof,
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
        fixture.mine,
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
            expected_sample(&fixture.ctx, &fixture.state, &round, spool, fixture.mine)
        })
        .map(|sample| sample.sub_leaf)
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
    let asked = expected_sample(&fixture.ctx, &fixture.state, &fixture.round(), target, fixture.mine)
        .expect("sample");

    // Wrap, so a draw that lands on the last leaf still names a real neighbour.
    let leaves = sub_leaf_count(fixture.slices[0].len());
    let elsewhere = (asked.sub_leaf + 1) % leaves;
    assert_ne!(elsewhere, asked.sub_leaf);

    // The same fixture accepts the leaf it did ask for, so this is refusing the
    // substitution rather than failing for some unrelated reason.
    assert!(accept_answer(
        &fixture.ctx,
        &fixture.state,
        &fixture.answer_from(target),
        fixture.mine,
        true
    ));
    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &fixture.answer_from_leaf(target, Some(elsewhere)),
        fixture.mine,
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
    let before = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");

    let cutoff = challenge_schedule(&fixture.state)
        .expect("schedule")
        .base_slot(round.round);
    let late = Address::new_unique();
    fixture
        .ctx
        .store
        .put_slice(fixture.mine, late, vec![0xA5; 4 * SUB_LEAF_BYTES])
        .expect("late slice");
    fixture.ctx.store.put_track_slot(late, cutoff).expect("late slot");

    let after = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");
    assert_eq!(after.track, before.track, "a mid-round write shifted the draw");
    assert_eq!(after.sub_leaf, before.sub_leaf);

    // A registration final before the window stays in the set, and a track
    // with no recorded slot predates the record and is grandfathered.
    fixture
        .ctx
        .store
        .put_track_slot(before.track, SlotNumber(0))
        .expect("early slot");
    let again = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");
    assert_eq!(again.track, before.track);
}

// the mirror of the registration cut: a slice deleted after the window opened
// leaves a tombstone, so the set the draw runs over is the same whether or not
// an observer has processed the delete yet
#[tokio::test]
async fn mid_round_delete() {
    let fixture = fixture().await;
    let round = fixture.round();
    let cutoff = challenge_schedule(&fixture.state)
        .expect("schedule")
        .base_slot(round.round);

    let extra = Address::new_unique();
    fixture
        .ctx
        .store
        .put_slice(fixture.mine, extra, vec![0x5A; 4 * SUB_LEAF_BYTES])
        .expect("extra slice");
    fixture.ctx.store.put_track_slot(extra, SlotNumber(0)).expect("extra slot");
    let before = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");

    let len = fixture
        .ctx
        .store
        .slice_size(fixture.mine, extra)
        .expect("size read")
        .expect("size indexed");
    fixture.ctx.store.delete_slice(fixture.mine, extra).expect("delete");
    fixture
        .ctx
        .store
        .put_slice_tombstone(
            fixture.mine,
            extra,
            SliceTombstone { deleted_slot: cutoff, slice_len: len },
        )
        .expect("tombstone");

    let after = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");
    assert_eq!(after.track, before.track, "a mid-round deletion shifted the draw");
    assert_eq!(after.sub_leaf, before.sub_leaf);

    // Once the deletion predates the window, the entry is out of the set.
    fixture
        .ctx
        .store
        .put_slice_tombstone(
            fixture.mine,
            extra,
            SliceTombstone {
                deleted_slot: SlotNumber(cutoff.as_u64() - 1),
                slice_len: len,
            },
        )
        .expect("tombstone");
    let out = expected_sample(&fixture.ctx, &fixture.state, &round, fixture.mine, fixture.mine)
        .expect("a sample");
    assert_ne!(out.track, extra, "a pre-window deletion stayed in the set");
}

// a leaf that does not hash into the track's commitment is refused
#[tokio::test]
async fn forged_leaf() {
    let fixture = fixture().await;
    let target = fixture.other();
    let mut answer = fixture.answer_from(target);
    answer.proof.sub_leaf[0] ^= 0xFF;

    assert!(!accept_answer(
        &fixture.ctx,
        &fixture.state,
        &answer,
        fixture.mine,
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
        fixture.mine,
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
        fixture.mine,
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

// the challenged owner's own signature does not count toward its quorum
#[tokio::test]
async fn self_certify() {
    let fixture = fixture().await;
    let target = fixture.other();
    let owner = fixture.state.spool_owner(target).expect("owner");

    let mut signed = fixture.attestations(target, agreement_threshold(GROUP_SIZE) - 1);
    signed.push((
        owner,
        fixture.keys[&target]
            .sign(attest_message(&fixture.round(), target).to_bytes())
            .expect("sign"),
    ));

    let certificate = fixture.certify(target, signed);
    assert_eq!(
        fixture.check(target, &certificate),
        Err(CertificateRejection::SelfCertified)
    );
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

    assert!(!accept_answer(&fixture.ctx, &fixture.state, &answer, fixture.mine, true));
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
    assert!(accept_answer(&fixture.ctx, &fixture.state, &forwarded, fixture.mine, true));

    let signed = fixture.attestations(target, agreement_threshold(GROUP_SIZE));
    let certificate = fixture.certify(target, signed);
    assert_eq!(fixture.check(target, &certificate), Ok(()));
}

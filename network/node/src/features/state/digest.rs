use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use bytemuck::Zeroable;

use tape_core::bls::BlsSignature;
use tape_core::cert::challenge::ChallengeDigestMessage;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::ProtocolState;

/// What a peer's report was worth once it was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// Not this node's epoch, not settled at either end, or unsigned.
    Ignored,
    /// A checked report that matches this node's view.
    Agrees,
    /// A checked report that does not, and how many distinct peers now say it.
    Disagrees { signers: usize },
}

/// This node's view digest, and what its peers say theirs is.
///
/// Detection only. Peers advance slots apart, so their digests legitimately
/// disagree across a transition, and the reports arriving here have not been
/// weighed against stake or committee seniority beyond membership. What it
/// produces is a count for an operator to read, never an action.
#[derive(Default)]
pub struct DigestWatch {
    inner: Mutex<WatchState>,
}

#[derive(Default)]
struct WatchState {
    own: Option<Signed>,
    reports: HashMap<Address, Hash>,
}

/// This node's digest for one epoch, and its signature over the report.
#[derive(Clone, Copy)]
struct Signed {
    epoch: EpochNumber,
    digest: Hash,
    signature: Option<BlsSignature>,
}

impl DigestWatch {
    /// This node's digest for the settled view, or nothing mid-transition
    pub fn own(&self, state: &ProtocolState) -> Option<Hash> {
        if state.phase() != EpochPhase::Active {
            return None;
        }

        let epoch = state.epoch();
        let mut inner = self.lock();
        if let Some(signed) = inner.own {
            if signed.epoch == epoch {
                return Some(signed.digest);
            }
        }

        let digest = state.view_digest();
        inner.own = Some(Signed {
            epoch,
            digest,
            signature: None,
        });
        inner.reports.clear();
        Some(digest)
    }

    /// The digest to report, with a signature over it, both cached per epoch
    ///
    /// The bytes signed are constant for the whole epoch, so signing them once
    /// keeps a pairing off the attestation path. The epoch inside the signed
    /// message is the one the digest was taken from, never the round's: a round
    /// that settles across a boundary carries the previous epoch's number, and a
    /// report labelled with it would be read against the wrong view.
    pub fn signed(
        &self,
        state: &ProtocolState,
        me: Address,
        sign: impl FnOnce(&[u8]) -> Option<BlsSignature>,
    ) -> (Hash, BlsSignature) {
        let unsigned = (Hash::default(), BlsSignature::zeroed());
        let Some(digest) = self.own(state) else {
            return unsigned;
        };

        let epoch = state.epoch();
        let mut inner = self.lock();
        if let Some(signed) = inner.own {
            if let (true, Some(signature)) = (signed.epoch == epoch, signed.signature) {
                return (signed.digest, signature);
            }
        }

        let Some(signature) = sign(&ChallengeDigestMessage::new(me, epoch, digest).to_bytes())
        else {
            return unsigned;
        };
        inner.own = Some(Signed {
            epoch,
            digest,
            signature: Some(signature),
        });
        (digest, signature)
    }

    /// Drops the cached digest so the next read comes off freshly published state
    ///
    /// Called from every state publish. A committee join or an eviction rewrites
    /// the hashed inputs without moving the epoch, and a digest cached under the
    /// epoch alone would keep answering with the view that has just been
    /// replaced.
    pub fn invalidate(&self) {
        let mut inner = self.lock();
        inner.own = None;
        inner.reports.clear();
    }

    /// Distinct peers currently reporting one view that is not this node's
    pub fn disagreeing(&self) -> usize {
        let inner = self.lock();
        largest_bloc(&inner.reports)
    }

    /// Records one peer's signed digest and says what it was worth
    ///
    /// A report counts only from a current committee member, only for the epoch
    /// this node calls settled, and only under that member's registered key.
    pub fn observe(
        &self,
        state: &ProtocolState,
        signer: Address,
        epoch: EpochNumber,
        digest: Hash,
        signature: BlsSignature,
    ) -> Report {
        let Some(mine) = self.own(state) else {
            return Report::Ignored;
        };
        if epoch != state.epoch() || digest == Hash::default() {
            return Report::Ignored;
        }

        // The peer directory holds anyone who ever paid rent to register. Only a
        // seat in the committee that is running says a report speaks for the
        // network this node belongs to.
        let Some(peer) = state.find_member(signer).and(state.peer(signer)) else {
            return Report::Ignored;
        };

        // Nothing new from a peer already counted at this digest, so a peer that
        // attests every round costs one pairing rather than one per round.
        let is_known = self.lock().reports.get(&signer) == Some(&digest);
        if !is_known {
            let message = ChallengeDigestMessage::new(signer, epoch, digest).to_bytes();
            if signature
                .verify_aggregate(message, core::slice::from_ref(&peer.bls_pubkey))
                .is_err()
            {
                return Report::Ignored;
            }
        }

        let mut inner = self.lock();
        if digest == mine {
            inner.reports.remove(&signer);
            return Report::Agrees;
        }

        inner.reports.insert(signer, digest);
        Report::Disagrees {
            signers: largest_bloc(&inner.reports),
        }
    }

    fn lock(&self) -> MutexGuard<'_, WatchState> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Distinct peers behind whichever foreign digest the most of them report.
fn largest_bloc(reports: &HashMap<Address, Hash>) -> usize {
    let mut tally: HashMap<Hash, usize> = HashMap::new();
    for reported in reports.values() {
        *tally.entry(*reported).or_default() += 1;
    }
    tally.into_values().max().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use tape_api::state::Epoch;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::system::{EpochState, Member, Peer};
    use tape_core::types::coin::TAPE;
    use tape_protocol::EpochBundle;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    struct Reporter {
        node: Address,
        key: BlsPrivateKey,
    }

    impl Reporter {
        fn new(byte: u8) -> Self {
            let mut bytes = [0u8; 32];
            bytes[0] = byte;
            Self {
                node: Address::new(bytes),
                key: BlsPrivateKey::from_random(),
            }
        }

        fn sign(&self, epoch: EpochNumber, digest: Hash) -> BlsSignature {
            self.key
                .sign(&ChallengeDigestMessage::new(self.node, epoch, digest).to_bytes())
                .expect("sign")
        }
    }

    fn phase(phase: EpochPhase) -> EpochState {
        let mut state = EpochState::zeroed();
        state.phase = phase as u64;
        state
    }

    /// Every reporter both registered and seated, which is what a report needs.
    fn seated(epoch: EpochNumber, reporters: &[Reporter]) -> ProtocolState {
        let mut peers = Vec::new();
        let mut committee = Vec::new();
        for reporter in reporters {
            let mut peer = Peer::new(reporter.node);
            peer.bls_pubkey = reporter.key.public_key().expect("pubkey");
            peers.push(peer);
            committee.push(Member::new(reporter.node, TAPE(1_000)));
        }

        ProtocolState {
            peers,
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    state: phase(EpochPhase::Active),
                    ..Epoch::zeroed()
                },
                committee,
                ..EpochBundle::default()
            },
            ..ProtocolState::default()
        }
    }

    fn reporters(count: u8) -> Vec<Reporter> {
        (1..=count).map(Reporter::new).collect()
    }

    fn theirs() -> Hash {
        Hash([0x5A; 32])
    }

    const EPOCH: EpochNumber = EpochNumber(7);

    fn disagreed(report: Report) -> usize {
        match report {
            Report::Disagrees { signers } => signers,
            Report::Agrees | Report::Ignored => 0,
        }
    }

    // a report nobody signed for is not evidence
    #[test]
    fn unsigned_refused() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let report =
                watch.observe(&state, reporter.node, EPOCH, theirs(), BlsSignature::zeroed());
            assert_eq!(report, Report::Ignored);
        }
        assert_eq!(watch.disagreeing(), 0);
    }

    // nor one signed by a key the reporter does not hold
    #[test]
    fn forged_signer_refused() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();
        let outsider = Reporter::new(200);

        for reporter in &peers {
            let stolen = outsider.sign(EPOCH, theirs());
            assert_eq!(
                watch.observe(&state, reporter.node, EPOCH, theirs(), stolen),
                Report::Ignored
            );
        }
    }

    // one peer's report cannot be replayed under another peer's name
    #[test]
    fn reports_do_not_transfer() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();
        let captured = peers[0].sign(EPOCH, theirs());

        for reporter in peers.iter().skip(1) {
            assert_eq!(
                watch.observe(&state, reporter.node, EPOCH, theirs(), captured),
                Report::Ignored
            );
        }
    }

    // registering costs rent; a seat costs stake. Only the seat speaks.
    #[test]
    fn unseated_peers_refused() {
        let peers = reporters(20);
        let mut state = seated(EPOCH, &peers);
        state.current.committee.clear();
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EPOCH, theirs());
            assert_eq!(
                watch.observe(&state, reporter.node, EPOCH, theirs(), signature),
                Report::Ignored
            );
        }
        assert_eq!(watch.disagreeing(), 0);
    }

    // a node mid-transition has no settled view to compare against
    #[test]
    fn suppressed_in_transition() {
        let peers = reporters(20);
        let mut state = seated(EPOCH, &peers);
        state.current.epoch.state = phase(EpochPhase::Closing);
        let watch = DigestWatch::default();

        assert!(watch.own(&state).is_none());
        for reporter in &peers {
            let signature = reporter.sign(EPOCH, theirs());
            assert_eq!(
                watch.observe(&state, reporter.node, EPOCH, theirs(), signature),
                Report::Ignored
            );
        }
    }

    // peers advance slots apart, so one still speaking for the epoch we left
    // says nothing about the one we are in
    #[test]
    fn other_epochs_ignored() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EpochNumber(6), theirs());
            assert_eq!(
                watch.observe(&state, reporter.node, EpochNumber(6), theirs(), signature),
                Report::Ignored
            );
        }
    }

    // a peer that has not settled reports nothing rather than a wrong answer
    #[test]
    fn unsettled_peers_ignored() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EPOCH, Hash::default());
            assert_eq!(
                watch.observe(&state, reporter.node, EPOCH, Hash::default(), signature),
                Report::Ignored
            );
        }
    }

    // the count is of distinct peers behind one foreign view, so a peer
    // attesting every round is still one of them
    #[test]
    fn counts_distinct_signers() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for _ in 0..8 {
            let signature = peers[0].sign(EPOCH, theirs());
            let report = watch.observe(&state, peers[0].node, EPOCH, theirs(), signature);
            assert_eq!(disagreed(report), 1);
        }

        let signature = peers[1].sign(EPOCH, theirs());
        assert_eq!(
            disagreed(watch.observe(&state, peers[1].node, EPOCH, theirs(), signature)),
            2
        );
        assert_eq!(watch.disagreeing(), 2);
    }

    // peers reading the chain at different slots disagree with each other as
    // well as with us, and the gauge reports the largest bloc, not the total
    #[test]
    fn scattered_reports_stay_small() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for (index, reporter) in peers.iter().enumerate() {
            let scattered = Hash([index as u8 + 1; 32]);
            let signature = reporter.sign(EPOCH, scattered);
            watch.observe(&state, reporter.node, EPOCH, scattered, signature);
        }

        assert_eq!(watch.disagreeing(), 1);
    }

    // a whole group behind one foreign view is counted and nothing more: this
    // watch reports, it does not act
    #[test]
    fn whole_group_only_counts() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EPOCH, theirs());
            watch.observe(&state, reporter.node, EPOCH, theirs(), signature);
        }

        assert_eq!(watch.disagreeing(), 20);
    }

    // a peer that comes back into line stops being counted against us
    #[test]
    fn agreement_clears_a_reporter() {
        let peers = reporters(20);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();
        let mine = watch.own(&state).expect("settled");

        let signature = peers[0].sign(EPOCH, theirs());
        assert_eq!(
            disagreed(watch.observe(&state, peers[0].node, EPOCH, theirs(), signature)),
            1
        );

        let corrected = peers[0].sign(EPOCH, mine);
        assert_eq!(
            watch.observe(&state, peers[0].node, EPOCH, mine, corrected),
            Report::Agrees
        );
        assert_eq!(watch.disagreeing(), 0);
    }

    // the signature is over the digest's own epoch, cached with it, and signed
    // once rather than per attestation
    #[test]
    fn signature_is_cached_per_epoch() {
        let peers = reporters(1);
        let state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();
        let mut calls = 0;

        let (digest, signature) = watch.signed(&state, peers[0].node, |message| {
            calls += 1;
            peers[0].key.sign(message).ok()
        });
        assert_ne!(digest, Hash::default());

        let (again, same) = watch.signed(&state, peers[0].node, |_| {
            calls += 1;
            None
        });
        assert_eq!(calls, 1, "signed the same bytes twice");
        assert_eq!((again, same), (digest, signature));

        // And it is the digest's epoch in the message, not a round's.
        let expected = ChallengeDigestMessage::new(peers[0].node, EPOCH, digest).to_bytes();
        assert!(
            signature
                .verify_aggregate(
                    expected,
                    core::slice::from_ref(&peers[0].key.public_key().expect("pubkey"))
                )
                .is_ok()
        );
    }

    // a key rotation inside the epoch moves the digest, and a cache kept under
    // the epoch number alone would answer with the view that was replaced
    #[test]
    fn cache_follows_the_view() {
        let peers = reporters(20);
        let mut state = seated(EPOCH, &peers);
        let watch = DigestWatch::default();
        let before = watch.own(&state).expect("settled");

        state.peers[0].bls_pubkey = BlsPrivateKey::from_random().public_key().expect("pubkey");
        assert_eq!(watch.own(&state), Some(before), "still cached");

        watch.invalidate();
        assert_ne!(watch.own(&state), Some(before), "recomputed after a publish");
    }

    // and it is the publish that has to drop it: a join or an eviction rewrites
    // what the digest covers without moving the epoch it is keyed under
    #[tokio::test]
    async fn publish_drops_the_cache() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        let mut live = (*ctx.state()).clone();
        live.current.epoch.state = phase(EpochPhase::Active);
        ctx.set_state(live).expect("publish");
        let before = ctx.epoch_digest.own(&ctx.state()).expect("settled");

        let mut rotated = (*ctx.state()).clone();
        rotated.current.groups[0].spools[0].node = Address::new_unique();
        assert_eq!(rotated.epoch(), ctx.state().epoch(), "the epoch must not move");
        ctx.set_state(rotated).expect("publish");

        let after = ctx.epoch_digest.own(&ctx.state()).expect("settled");
        assert_ne!(before, after, "the watch answered with the replaced view");
    }
}

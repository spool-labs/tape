use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tape_core::bls::BlsSignature;
use tape_core::cert::challenge::ChallengeDigestMessage;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::ProtocolState;

/// Distinct peers that must report one view before it outweighs ours.
///
/// The mechanism's own supermajority over a full group. Scaling this to the
/// reports in hand instead would let three peers outvote a group of twenty, and
/// would let anything that clears the reports re-arm the vote from scratch.
pub const DIVERGENCE_QUORUM: usize = GROUP_SIZE * 2 / 3 + 1;

/// This node's view digest, and what its peers say theirs is.
///
/// Peers advance slots apart, so their digests legitimately disagree across a
/// transition. Only epochs both sides call settled are compared.
#[derive(Default)]
pub struct DigestWatch {
    inner: Mutex<WatchState>,
}

#[derive(Default)]
struct WatchState {
    own: Option<(EpochNumber, Hash)>,
    reports: HashMap<Address, Hash>,
}

impl DigestWatch {
    /// This node's digest for the settled view, or nothing mid-transition
    pub fn own(&self, state: &ProtocolState) -> Option<Hash> {
        if state.phase() != EpochPhase::Active {
            return None;
        }

        let epoch = state.epoch();
        let mut inner = self.lock();
        if let Some((cached, digest)) = inner.own {
            if cached == epoch {
                return Some(digest);
            }
        }

        let digest = state.view_digest();
        inner.own = Some((epoch, digest));
        inner.reports.clear();
        Some(digest)
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

    /// Records one peer's signed digest, returning whether the group's view is not ours
    ///
    /// Divergence is a supermajority of distinct peers agreeing on one digest
    /// that is not this node's. Scattered disagreement is the ordinary skew of
    /// peers reading the chain at different slots.
    pub fn observe(
        &self,
        state: &ProtocolState,
        signer: Address,
        epoch: EpochNumber,
        digest: Hash,
        signature: BlsSignature,
    ) -> bool {
        let Some(mine) = self.own(state) else {
            return false;
        };
        if epoch != state.epoch() || digest == Hash::default() || digest == mine {
            return false;
        }

        // Only a report that pushes towards divergence is worth a pairing check,
        // and only a checked one is counted. A quorum fixed to the group's size
        // leaves nothing for a forged agreeing report to buy.
        if !signed_by(state, signer, epoch, digest, signature) {
            return false;
        }

        let mut inner = self.lock();
        inner.reports.insert(signer, digest);

        let mut tally: HashMap<Hash, usize> = HashMap::new();
        for reported in inner.reports.values() {
            *tally.entry(*reported).or_default() += 1;
        }

        let agreeing = tally.into_values().max().unwrap_or_default();
        agreeing >= DIVERGENCE_QUORUM
    }

    fn lock(&self) -> MutexGuard<'_, WatchState> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Whether the signer's registered key stands behind this report.
fn signed_by(
    state: &ProtocolState,
    signer: Address,
    epoch: EpochNumber,
    digest: Hash,
    signature: BlsSignature,
) -> bool {
    let Some(peer) = state.peer(signer) else {
        return false;
    };

    signature
        .verify_aggregate(
            ChallengeDigestMessage::new(signer, epoch, digest).to_bytes(),
            core::slice::from_ref(&peer.bls_pubkey),
        )
        .is_ok()
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::state::Epoch;
    use crate::features::http::handlers::challenge::agreement_threshold;
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

    fn active_state(epoch: EpochNumber, reporters: &[Reporter]) -> ProtocolState {
        let mut peers = Vec::new();
        for reporter in reporters {
            let mut peer = Peer::new(reporter.node);
            peer.bls_pubkey = reporter.key.public_key().expect("pubkey");
            peers.push(peer);
        }

        ProtocolState {
            peers,
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    state: phase(EpochPhase::Active),
                    ..Epoch::zeroed()
                },
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

    // the quorum is the group's, not the count of peers that happened to report
    #[test]
    fn quorum_is_the_group_supermajority() {
        assert_eq!(DIVERGENCE_QUORUM, 14);
        assert!(DIVERGENCE_QUORUM > GROUP_SIZE / 2);
        assert!(DIVERGENCE_QUORUM > agreement_threshold(4));
    }

    // a report nobody signed for is not evidence, however many arrive
    #[test]
    fn unsigned_reports_are_refused() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            assert!(!watch.observe(
                &state,
                reporter.node,
                EPOCH,
                theirs(),
                BlsSignature::zeroed()
            ));
        }
    }

    // nor is one signed by a key the reporter does not hold
    #[test]
    fn forged_signer_is_refused() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();
        let outsider = Reporter::new(200);

        for reporter in &peers {
            let stolen = outsider.sign(EPOCH, theirs());
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), stolen));
        }
    }

    // one peer's report cannot be replayed under another peer's name
    #[test]
    fn reports_do_not_transfer() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();
        let captured = peers[0].sign(EPOCH, theirs());

        for reporter in peers.iter().skip(1) {
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), captured));
        }
    }

    // nor carried into another epoch
    #[test]
    fn reports_do_not_travel_between_epochs() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let stale = reporter.sign(EpochNumber(6), theirs());
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), stale));
        }
    }

    // a node mid-transition has no settled view to compare against
    #[test]
    fn suppressed_during_transition() {
        let peers = reporters(20);
        let mut state = active_state(EPOCH, &peers);
        state.current.epoch.state = phase(EpochPhase::Closing);
        let watch = DigestWatch::default();

        assert!(watch.own(&state).is_none());
        for reporter in &peers {
            let signature = reporter.sign(EPOCH, theirs());
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), signature));
        }
    }

    // peers advance slots apart, so one still speaking for the epoch we left
    // says nothing about the one we are in
    #[test]
    fn other_epochs_are_ignored() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EpochNumber(6), theirs());
            assert!(!watch.observe(&state, reporter.node, EpochNumber(6), theirs(), signature));
        }
    }

    // a peer that has not settled reports nothing rather than a wrong answer
    #[test]
    fn unsettled_peers_are_ignored() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in &peers {
            let signature = reporter.sign(EPOCH, Hash::default());
            assert!(!watch.observe(&state, reporter.node, EPOCH, Hash::default(), signature));
        }
    }

    // thirteen of twenty is not the group, and one peer repeating itself is not
    // thirteen
    #[test]
    fn short_of_quorum_stands_down() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in peers.iter().take(DIVERGENCE_QUORUM - 1) {
            let signature = reporter.sign(EPOCH, theirs());
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), signature));
        }

        let loud = &peers[0];
        for _ in 0..20 {
            let signature = loud.sign(EPOCH, theirs());
            assert!(!watch.observe(&state, loud.node, EPOCH, theirs(), signature));
        }
    }

    // peers reading the chain at different slots disagree with each other as
    // well as with us, which is skew rather than a view we have lost
    #[test]
    fn scattered_disagreement_stands_down() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for (index, reporter) in peers.iter().enumerate() {
            let scattered = Hash([index as u8 + 1; 32]);
            let signature = reporter.sign(EPOCH, scattered);
            assert!(!watch.observe(&state, reporter.node, EPOCH, scattered, signature));
        }
    }

    // the group agreeing on a view that is not ours is the thing worth acting on
    #[test]
    fn supermajority_on_one_other_digest() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in peers.iter().take(DIVERGENCE_QUORUM - 1) {
            let signature = reporter.sign(EPOCH, theirs());
            assert!(!watch.observe(&state, reporter.node, EPOCH, theirs(), signature));
        }

        let last = &peers[DIVERGENCE_QUORUM - 1];
        let signature = last.sign(EPOCH, theirs());
        assert!(watch.observe(&state, last.node, EPOCH, theirs(), signature));
    }

    // peers that agree with us keep the group's view ours, however many report
    #[test]
    fn agreement_never_diverges() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();
        let mine = watch.own(&state).expect("settled");

        for reporter in &peers {
            let signature = reporter.sign(EPOCH, mine);
            assert!(!watch.observe(&state, reporter.node, EPOCH, mine, signature));
        }
    }

    // a realigned node compares against what it just read, not what it held
    #[test]
    fn invalidate_drops_the_reports() {
        let peers = reporters(20);
        let state = active_state(EPOCH, &peers);
        let watch = DigestWatch::default();

        for reporter in peers.iter().take(DIVERGENCE_QUORUM - 1) {
            let signature = reporter.sign(EPOCH, theirs());
            watch.observe(&state, reporter.node, EPOCH, theirs(), signature);
        }
        watch.invalidate();

        let last = &peers[DIVERGENCE_QUORUM - 1];
        let signature = last.sign(EPOCH, theirs());
        assert!(!watch.observe(&state, last.node, EPOCH, theirs(), signature));
    }

    // a key rotation inside the epoch moves the digest, and a cache kept under
    // the epoch number alone would answer with the view that was replaced
    #[test]
    fn cache_follows_the_view_not_the_epoch() {
        let peers = reporters(20);
        let mut state = active_state(EPOCH, &peers);
        state.current.committee.push(Member::new(peers[0].node, TAPE(1_000)));
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
    async fn publishing_state_drops_the_cached_digest() {
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

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_crypto::hash::Hash;
use tape_protocol::ProtocolState;

use crate::features::http::handlers::challenge::agreement_threshold;

/// Peers whose digests must be in hand before any of them counts.
///
/// A group is twenty positions wide; a handful of reports is not a group's
/// opinion, and treating it as one turns a quiet epoch into a realign.
const MIN_REPORTS: usize = 4;

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
    epoch: EpochNumber,
    own: Option<Hash>,
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
        if let Some(digest) = inner.own {
            if inner.epoch == epoch {
                return Some(digest);
            }
        }

        let digest = state.view_digest();
        inner.epoch = epoch;
        inner.own = Some(digest);
        inner.reports.clear();
        Some(digest)
    }

    /// Drops the cached digest so the next read comes off freshly published state
    pub fn invalidate(&self) {
        let mut inner = self.lock();
        inner.own = None;
        inner.reports.clear();
    }

    /// Records one peer's digest, returning whether the group's view is not ours
    ///
    /// Divergence is a supermajority of the peers heard agreeing on one digest
    /// that is not this node's. Scattered disagreement is the ordinary skew of
    /// peers reading the chain at different slots.
    pub fn observe(
        &self,
        state: &ProtocolState,
        signer: Address,
        epoch: EpochNumber,
        digest: Hash,
    ) -> bool {
        let Some(mine) = self.own(state) else {
            return false;
        };
        if epoch != state.epoch() || digest == Hash::default() {
            return false;
        }

        let mut inner = self.lock();
        inner.reports.insert(signer, digest);

        let heard = inner.reports.len();
        if heard < MIN_REPORTS {
            return false;
        }

        let mut tally: HashMap<Hash, usize> = HashMap::new();
        for reported in inner.reports.values() {
            if *reported != mine {
                *tally.entry(*reported).or_default() += 1;
            }
        }

        let agreeing = tally.into_values().max().unwrap_or_default();
        agreeing >= agreement_threshold(heard)
    }

    fn lock(&self) -> MutexGuard<'_, WatchState> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::Zeroable;
    use tape_api::state::Epoch;
    use tape_protocol::EpochBundle;

    use super::*;

    fn active_state(epoch: EpochNumber) -> ProtocolState {
        ProtocolState {
            current: EpochBundle {
                epoch: Epoch {
                    id: epoch,
                    state: epoch_state(EpochPhase::Active),
                    ..Epoch::zeroed()
                },
                ..EpochBundle::default()
            },
            ..ProtocolState::default()
        }
    }

    fn epoch_state(phase: EpochPhase) -> tape_core::system::EpochState {
        let mut state = tape_core::system::EpochState::zeroed();
        state.phase = phase as u64;
        state
    }

    fn signer(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn theirs() -> Hash {
        Hash([0x5A; 32])
    }

    // a node mid-transition has no settled view to compare against, so nothing
    // a peer reports can be read as divergence
    #[test]
    fn suppressed_during_transition() {
        let watch = DigestWatch::default();
        let mut state = active_state(EpochNumber(7));
        state.current.epoch.state = epoch_state(EpochPhase::Closing);

        assert!(watch.own(&state).is_none());
        for index in 0..10 {
            assert!(!watch.observe(&state, signer(index), EpochNumber(7), theirs()));
        }
    }

    // peers advance slots apart, so one reporting the epoch we just left says
    // nothing about our view of the one we are in
    #[test]
    fn other_epochs_are_ignored() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));

        for index in 0..10 {
            assert!(!watch.observe(&state, signer(index), EpochNumber(6), theirs()));
        }
    }

    // a peer that has not settled sends nothing rather than a wrong answer
    #[test]
    fn unsettled_peers_are_ignored() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));

        for index in 0..10 {
            assert!(!watch.observe(&state, signer(index), EpochNumber(7), Hash::default()));
        }
    }

    // a handful of peers is not a group, and one loud peer repeating itself is
    // not several
    #[test]
    fn needs_enough_distinct_peers() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));

        for _ in 0..10 {
            assert!(!watch.observe(&state, signer(1), EpochNumber(7), theirs()));
        }
        assert!(!watch.observe(&state, signer(2), EpochNumber(7), theirs()));
    }

    // peers reading the chain at different slots disagree with each other as
    // well as with us, which is skew rather than a view we have lost
    #[test]
    fn scattered_disagreement_stands_down() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));

        for index in 0..9u8 {
            let scattered = Hash([index + 1; 32]);
            assert!(!watch.observe(&state, signer(index), EpochNumber(7), scattered));
        }
    }

    // the group agreeing on a view that is not ours is the thing worth acting on
    #[test]
    fn supermajority_on_one_other_digest() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));
        let mine = watch.own(&state).expect("settled");
        assert_ne!(mine, theirs());

        assert!(!watch.observe(&state, signer(1), EpochNumber(7), theirs()));
        assert!(!watch.observe(&state, signer(2), EpochNumber(7), theirs()));
        assert!(!watch.observe(&state, signer(3), EpochNumber(7), mine));
        assert!(watch.observe(&state, signer(4), EpochNumber(7), theirs()));
    }

    // peers that agree with us keep the group's view ours, however many report
    #[test]
    fn agreement_never_diverges() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));
        let mine = watch.own(&state).expect("settled");

        for index in 0..12 {
            assert!(!watch.observe(&state, signer(index), EpochNumber(7), mine));
        }
    }

    // a realigned node compares against what it just read, not what it held
    #[test]
    fn invalidate_drops_the_reports() {
        let watch = DigestWatch::default();
        let state = active_state(EpochNumber(7));

        for index in 1..4 {
            watch.observe(&state, signer(index), EpochNumber(7), theirs());
        }
        watch.invalidate();

        assert!(!watch.observe(&state, signer(4), EpochNumber(7), theirs()));
    }
}

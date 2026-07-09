//! Build the per-target eviction candidate from local protocol state.

use tape_core::system::{EpochPhase, VoteCandidate, VoteKind};
use tape_core::types::EpochNumber;
use tape_crypto::{Address, Hash};
use tape_protocol::ProtocolState;

#[derive(Debug, Clone, Copy)]
pub struct EvictionCandidate {
    pub voting_epoch: EpochNumber,
    pub target_epoch: EpochNumber,
    pub nonce: Hash,
    pub node: Address,
    pub hash: Hash,
}

impl EvictionCandidate {
    /// Stable identity for this vote
    ///
    /// The target node is encoded into the vote hash, matching the on-chain
    /// vote account and the peer vote responder.
    pub fn vote(&self) -> VoteCandidate {
        VoteCandidate {
            kind: VoteKind::Eviction,
            voting_epoch: self.voting_epoch,
            target_epoch: self.target_epoch,
            hash: self.hash,
        }
    }
}

/// Build an eviction candidate for the target node
///
/// Yields nothing outside the eviction voting window, which opens once the next
/// epoch is set up and closes once the epoch enters its closing phase. The
/// target need not be seated in the next committee: eviction is pre-emptive and
/// also blocks a not-yet-seated node from joining, matching the on-chain vote.
pub fn build_eviction(state: &ProtocolState, node: Address) -> Option<EvictionCandidate> {
    if state.phase() >= EpochPhase::Closing {
        return None;
    }

    let next_epoch = state.next_epoch.as_ref()?;

    Some(EvictionCandidate {
        voting_epoch: state.epoch(),
        target_epoch: next_epoch.id,
        nonce: next_epoch.nonce,
        node,
        hash: Hash(node.to_bytes()),
    })
}

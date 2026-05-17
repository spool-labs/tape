use tape_solana::*;
use tape_core::system::VoteKind;
use tape_core::types::{EpochNumber, Tail};
use tape_crypto::Hash;
use super::AccountType;
use crate::dynamic::DynamicState;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Vote {
    /// The kind of vote
    pub kind: u64,

    /// Hash being voted on
    pub hash: Hash,

    /// Epoch we're are voting in.
    pub voting_epoch: EpochNumber,

    /// Epoch whose state is updated when the vote lands.
    pub target_epoch: EpochNumber,

    /// Who registered the vote.
    pub registered_by: Pubkey,

    /// A bitmap of votes cast.
    pub bitmap: Tail<u8>,
}

tape_solana::state!(AccountType, Vote);

impl Vote {
    #[inline]
    pub fn kind(&self) -> Option<VoteKind> {
        VoteKind::try_from(self.kind).ok()
    }
}

impl DynamicState for Vote {
    type Entry = u8;

    fn tail(&self) -> &Tail<u8> { &self.bitmap }
    fn tail_mut(&mut self) -> &mut Tail<u8> { &mut self.bitmap }
}

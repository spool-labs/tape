use tape_core::prelude::*;
use tape_crypto::Hash;
use tape_solana::*;

use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Vote {
    /// The epoch this quorum vote belongs to.
    pub epoch: EpochNumber,

    /// The protocol vote domain/type.
    pub kind: u64,

    /// Hash of the canonical quorum-signed vote message.
    pub message_hash: Hash,

    /// The node that submitted/registered the accepted quorum vote.
    pub registered_by: NodeId,
}

tape_solana::state!(AccountType, Vote);

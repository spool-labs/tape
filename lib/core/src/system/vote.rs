use num_enum::{IntoPrimitive, TryFromPrimitive};
use tape_crypto::Hash;

use crate::types::EpochNumber;

#[repr(u64)]
#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, IntoPrimitive, TryFromPrimitive, serde::Serialize,
    serde::Deserialize,
)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
pub enum VoteKind {
    Unknown = 0,
    Snapshot,
    Assignment,
}

/// Durable identity for a candidate vote account.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
pub struct VoteCandidate {
    pub kind: VoteKind,
    pub voting_epoch: EpochNumber,
    pub target_epoch: EpochNumber,
    pub hash: Hash,
}

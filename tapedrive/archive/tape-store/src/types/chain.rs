//! Storage representations of on-chain state
//!
//! These types mirror on-chain account data but are optimized for storage with serde/wincode
//! serialization. They are named with a `Data` suffix to distinguish them from the on-chain
//! zero-copy POD types.

use super::impls::Pubkey;
use serde::{Deserialize, Serialize};
use tape_core::types::{EpochNumber, NodeId, TapeNumber, TrackNumber};
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Storage representation of on-chain tape account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeData {
    pub id: TapeNumber,
    pub authority: Pubkey,
    pub capacity: u64,
    pub used: u64,
    pub active_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
    pub track_count: u64,
}

/// Storage representation of on-chain track account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackData {
    pub id: TrackNumber,
    pub tape: Pubkey,
    pub key: Hash,
    pub size: u64,
    pub registered_epoch: EpochNumber,
    pub certified_epoch: EpochNumber,
    pub commitment_hash: Hash,
}

/// Storage representation of committee member information
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeMemberData {
    pub id: NodeId,
    pub stake: u64,
    pub weight: u64,
}

/// Storage representation of committee data for an epoch
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeData {
    pub epoch: EpochNumber,
    pub members: Vec<CommitteeMemberData>,
    pub total_stake: u64,
}

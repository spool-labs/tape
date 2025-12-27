//! Dummy on-chain mirror types - simplified versions until tape-core available

use super::ids::{EpochNumber, Hash, NodeId, Pubkey, TapeNumber, TrackNumber};
use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

// These represent the onchain state, but are not exactly the same as the onchain state, we should
// rename them to avoid confusion. Additionally, the onchain state is always zero-copy POD, where
// as these are bincode/windcode/serde. This reduces the stored data size and lets us add new
// fields in the future without breaking compatibility.

/// On-chain tape account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Tape { // should be named TapeData
    pub id: TapeNumber,
    pub authority: Pubkey,
    pub capacity: u64,
    pub used: u64,
    pub active_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
    pub track_count: u64,
}

/// On-chain track account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Track { // should be named TrackData
    pub id: TrackNumber,
    pub tape: Pubkey,
    pub key: Hash,
    pub size: u64,
    pub registered_epoch: EpochNumber,
    pub certified_epoch: EpochNumber,
    pub commitment_hash: Hash,
}

/// Committee member information
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeMember { // should be named CommitteeMemberData
    pub id: NodeId,
    pub stake: u64,
    pub weight: u64,
}

/// Committee data for an epoch
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Committee {  // should be named CommitteeData
    pub epoch: EpochNumber,
    pub members: Vec<CommitteeMember>,
    pub total_stake: u64,
}

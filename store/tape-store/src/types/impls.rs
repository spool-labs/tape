//! Wincode-compatible wrapper types for external types
//!
//! This module provides wrapper types with SchemaRead/SchemaWrite implementations
//! for types that can't be modified in their source crates.

use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use tape_core::bls::BlsPubkey;
use tape_core::types::{EpochNumber, NodeId};
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};
use wincode_derive::{SchemaRead, SchemaWrite};

/// A wincode-serializable wrapper around solana Pubkey for storage operations.
///
/// This type stores pubkeys as raw 32-byte arrays and provides conversions
/// to/from solana_program::pubkey::Pubkey via `.into()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Pubkey(pub [u8; 32]);

impl Pubkey {
    pub const LEN: usize = 32;

    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        Self(solana_program::pubkey::Pubkey::new_unique().to_bytes())
    }
}

impl From<solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl From<Pubkey> for solana_program::pubkey::Pubkey {
    fn from(stored: Pubkey) -> Self {
        solana_program::pubkey::Pubkey::new_from_array(stored.0)
    }
}

impl From<&solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: &solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl SchemaWrite for Pubkey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for Pubkey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Pubkey>) -> ReadResult<()> {
        let bytes: [u8; 32] = unsafe { reader.get_t()? };
        dst.write(Pubkey(bytes));
        Ok(())
    }
}

/// Information about a single committee member
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeMemberInfo {
    /// Unique node identifier
    pub id: NodeId,
    /// Node's on-chain account pubkey
    pub pubkey: Pubkey,
    /// BLS public key for signatures
    pub bls_pubkey: BlsPubkey,
    /// Network address for P2P communication
    pub network_address: String,
}

/// Cached committee information for an epoch
///
/// This struct contains all the information needed to:
/// - Route requests to the correct nodes
/// - Verify BLS signatures from committee members
/// - Determine local node's role and spool assignments
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeCache {
    /// Epoch this committee is active for
    pub epoch: EpochNumber,
    /// Ordered list of committee members
    pub members: Vec<CommitteeMemberInfo>,
    /// Spool-to-member assignment (index in members vec)
    /// spool_assignment[spool_id] = member_index
    pub spool_assignment: Vec<u8>,
    /// Index of local node in members (None if not in committee)
    pub my_member_index: Option<u8>,
    /// Spools assigned to local node (derived from spool_assignment)
    pub my_spools: Vec<u16>,
}

impl CommitteeCache {
    /// Create a new committee cache
    pub fn new(epoch: EpochNumber, members: Vec<CommitteeMemberInfo>) -> Self {
        Self {
            epoch,
            members,
            spool_assignment: Vec::new(),
            my_member_index: None,
            my_spools: Vec::new(),
        }
    }

    /// Get member info by member index
    pub fn get_member(&self, index: u8) -> Option<&CommitteeMemberInfo> {
        self.members.get(index as usize)
    }

    /// Get member index for a given spool
    pub fn get_spool_owner(&self, spool_id: u16) -> Option<u8> {
        self.spool_assignment.get(spool_id as usize).copied()
    }

    /// Check if the local node owns a given spool
    pub fn owns_spool(&self, spool_id: u16) -> bool {
        self.my_spools.contains(&spool_id)
    }

    /// Number of members in the committee
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;

    #[test]
    fn test_pubkey_roundtrip() {
        let pubkey = Pubkey::new([0xAB; 32]);
        let bytes = wincode::serialize(&pubkey).unwrap();
        let decoded: Pubkey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(pubkey, decoded);
    }

    #[test]
    fn test_pubkey_conversion() {
        let solana_pubkey = solana_program::pubkey::Pubkey::new_unique();
        let stored: Pubkey = solana_pubkey.into();
        let back: solana_program::pubkey::Pubkey = stored.into();
        assert_eq!(solana_pubkey, back);
    }

    #[test]
    fn test_committee_member_info_roundtrip() {
        let info = CommitteeMemberInfo {
            id: NodeId(42),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: CommitteeMemberInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_committee_cache_roundtrip() {
        let member1 = CommitteeMemberInfo {
            id: NodeId(1),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let member2 = CommitteeMemberInfo {
            id: NodeId(2),
            pubkey: Pubkey::new([2u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.2:8080".to_string(),
        };

        let cache = CommitteeCache {
            epoch: EpochNumber(100),
            members: vec![member1, member2],
            spool_assignment: vec![0, 1, 0, 1], // Alternating assignment
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        };

        let bytes = wincode::serialize(&cache).unwrap();
        let decoded: CommitteeCache = wincode::deserialize(&bytes).unwrap();
        assert_eq!(cache, decoded);
    }

    #[test]
    fn test_committee_cache_methods() {
        let member = CommitteeMemberInfo {
            id: NodeId(1),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let mut cache = CommitteeCache::new(EpochNumber(50), vec![member.clone()]);
        cache.spool_assignment = vec![0, 0, 0]; // All spools to member 0
        cache.my_member_index = Some(0);
        cache.my_spools = vec![0, 1, 2];

        assert_eq!(cache.member_count(), 1);
        assert_eq!(cache.get_member(0), Some(&member));
        assert_eq!(cache.get_member(1), None);
        assert_eq!(cache.get_spool_owner(0), Some(0));
        assert_eq!(cache.get_spool_owner(10), None);
        assert!(cache.owns_spool(0));
        assert!(cache.owns_spool(1));
        assert!(!cache.owns_spool(10));
    }
}

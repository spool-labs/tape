//! Committee management operations
//!
//! Caches committee data for routing and verification.

use crate::columns::*;
use crate::error::Result;
use crate::types::{EpochNumber, NodeId, Pubkey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::Store;
use tape_core::bls::BlsPubkey;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Cached committee data for an epoch
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeCache {
    /// Epoch this cache is for
    pub epoch: EpochNumber,
    /// Committee members
    pub members: Vec<CommitteeMemberInfo>,
    /// Spool assignment: spool_idx -> member_index
    pub spool_assignment: Vec<u8>,
    /// Our member index (if in committee)
    pub my_member_index: Option<u8>,
    /// Spools assigned to us
    pub my_spools: Vec<u16>,
}

/// Information about a committee member
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeMemberInfo {
    /// Node ID
    pub id: NodeId,
    /// Solana pubkey
    pub pubkey: Pubkey,
    /// BLS public key for aggregated signatures
    pub bls_pubkey: BlsPubkey,
    /// Network address (e.g., "192.168.1.1:8080")
    pub network_address: String,
}

/// High-level operations for committee management
pub trait CommitteeOps {
    /// Store committee cache for an epoch
    ///
    /// # Arguments
    /// * `cache` - The committee cache to store (epoch is in the cache)
    fn put_committee(&self, cache: CommitteeCache) -> Result<()>;

    /// Get committee cache for a specific epoch
    ///
    /// # Arguments
    /// * `epoch` - The epoch to query
    ///
    /// # Returns
    /// Committee cache if found
    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<CommitteeCache>>;

    /// Get current committee (most recent epoch)
    ///
    /// Iterates backward from the highest epoch to find the current committee.
    ///
    /// # Returns
    /// The most recent committee cache
    fn get_current_committee(&self) -> Result<Option<CommitteeCache>>;
}

impl<S: Store> CommitteeOps for TapeStore<S> {
    fn put_committee(&self, cache: CommitteeCache) -> Result<()> {
        self.put::<Committee>(&cache.epoch, &cache)?;
        Ok(())
    }

    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<CommitteeCache>> {
        let cache = self.get::<Committee>(&epoch)?;
        Ok(cache)
    }

    fn get_current_committee(&self) -> Result<Option<CommitteeCache>> {
        // Get the current epoch from meta, then look up the committee
        let current_epoch_key = "current_epoch".to_string();
        if let Some(epoch_bytes) = self.get::<Meta>(&current_epoch_key)? {
            if epoch_bytes.len() >= 8 {
                let epoch_value = u64::from_le_bytes(epoch_bytes[0..8].try_into().unwrap());
                let epoch = EpochNumber(epoch_value);
                return self.get_committee(epoch);
            }
        }

        // Fallback: iterate to find the highest epoch
        let iter = self.iter::<Committee>()?;
        let mut latest: Option<CommitteeCache> = None;
        for (_epoch, cache) in iter {
            match &latest {
                None => latest = Some(cache),
                Some(current) => {
                    if cache.epoch.0 > current.epoch.0 {
                        latest = Some(cache);
                    }
                }
            }
        }
        Ok(latest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use store_memory::MemoryStore;

    fn create_test_member(id: u64) -> CommitteeMemberInfo {
        CommitteeMemberInfo {
            id: NodeId(id),
            pubkey: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: format!("192.168.1.{}:8080", id),
        }
    }

    fn create_test_cache(epoch: u64) -> CommitteeCache {
        CommitteeCache {
            epoch: EpochNumber(epoch),
            members: vec![create_test_member(1), create_test_member(2)],
            spool_assignment: vec![0, 1, 0, 1], // Alternating assignment
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        }
    }

    #[test]
    fn put_and_get_committee() {
        let store = TapeStore::new(MemoryStore::new());
        let cache = create_test_cache(100);

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(cache));
    }

    #[test]
    fn get_current_committee() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for multiple epochs
        for epoch in [95, 100, 98] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Should return the highest epoch
        let current = store.get_current_committee().unwrap().unwrap();
        assert_eq!(current.epoch, EpochNumber(100));
    }

    #[test]
    fn committee_not_found() {
        let store = TapeStore::new(MemoryStore::new());

        let result = store.get_committee(EpochNumber(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn committee_member_info() {
        let store = TapeStore::new(MemoryStore::new());

        let cache = CommitteeCache {
            epoch: EpochNumber(100),
            members: vec![
                CommitteeMemberInfo {
                    id: NodeId(1),
                    pubkey: Pubkey::new([1u8; 32]),
                    bls_pubkey: BlsPubkey::zeroed(),
                    network_address: "10.0.0.1:9000".to_string(),
                },
                CommitteeMemberInfo {
                    id: NodeId(2),
                    pubkey: Pubkey::new([2u8; 32]),
                    bls_pubkey: BlsPubkey::zeroed(),
                    network_address: "10.0.0.2:9000".to_string(),
                },
            ],
            spool_assignment: vec![0, 1],
            my_member_index: Some(0),
            my_spools: vec![0],
        };

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap().unwrap();

        assert_eq!(retrieved.members.len(), 2);
        assert_eq!(retrieved.members[0].id, NodeId(1));
        assert_eq!(retrieved.members[0].network_address, "10.0.0.1:9000");
        assert_eq!(retrieved.members[1].id, NodeId(2));
    }

    #[test]
    fn spool_assignment() {
        let store = TapeStore::new(MemoryStore::new());

        // Create a cache with spool assignments
        let mut cache = create_test_cache(100);
        cache.spool_assignment = vec![0, 0, 1, 1, 0, 1]; // Member indices for spools 0-5
        cache.my_member_index = Some(1);
        cache.my_spools = vec![2, 3, 5]; // Spools assigned to member 1

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap().unwrap();

        assert_eq!(retrieved.spool_assignment.len(), 6);
        assert_eq!(retrieved.my_spools, vec![2, 3, 5]);
    }
}

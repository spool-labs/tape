//! Committee management operations
//!
//! Caches committee data for routing and verification.

use crate::columns::Committee;
use crate::error::Result;
use crate::ops::MetaOps;
use crate::types::{CommitteeCache, EpochKey, EpochNumber};
use crate::TapeStore;
use store::Store;

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

    /// Get current committee based on the stored current_epoch
    ///
    /// Uses the current_epoch from meta to look up the committee.
    ///
    /// # Returns
    /// The current committee cache if available
    fn get_current_committee(&self) -> Result<Option<CommitteeCache>>;

    /// Delete old committee caches, keeping the most recent N epochs
    ///
    /// # Arguments
    /// * `keep_epochs` - Number of recent epochs to keep
    fn delete_old_committees(&self, keep_epochs: usize) -> Result<()>;
}

impl<S: Store> CommitteeOps for TapeStore<S> {
    fn put_committee(&self, cache: CommitteeCache) -> Result<()> {
        let key = EpochKey::new(cache.epoch.as_u64());
        self.put::<Committee>(&key, &cache)?;
        Ok(())
    }

    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<CommitteeCache>> {
        let key = EpochKey::new(epoch.as_u64());
        Ok(self.get::<Committee>(&key)?)
    }

    fn get_current_committee(&self) -> Result<Option<CommitteeCache>> {
        // Get the current epoch from meta
        if let Some(epoch) = self.get_current_epoch()? {
            return self.get_committee(epoch);
        }

        // Fallback: iterate to find the highest epoch
        let iter = self.iter::<Committee>()?;
        let mut latest: Option<CommitteeCache> = None;
        for (_epoch, cache) in iter {
            match &latest {
                None => latest = Some(cache),
                Some(current) => {
                    if cache.epoch > current.epoch {
                        latest = Some(cache);
                    }
                }
            }
        }
        Ok(latest)
    }

    fn delete_old_committees(&self, keep_epochs: usize) -> Result<()> {
        // Collect all epochs
        let mut epochs: Vec<EpochNumber> = self
            .iter::<Committee>()?
            .into_iter()
            .map(|(key, _)| EpochNumber(key.0))
            .collect();

        // Sort descending to keep the highest
        epochs.sort_by(|a: &EpochNumber, b: &EpochNumber| b.cmp(a));

        // Delete all but the most recent `keep_epochs`
        for epoch in epochs.into_iter().skip(keep_epochs) {
            let key = EpochKey::new(epoch.as_u64());
            self.delete::<Committee>(&key)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommitteeMemberInfo, NodeId, Pubkey};
    use bytemuck::Zeroable;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsPubkey;

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
            spool_assignment: vec![0, 1, 0, 1],
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        }
    }

    #[test]
    fn test_put_and_get_committee() {
        let store = TapeStore::new(MemoryStore::new());
        let cache = create_test_cache(100);

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(cache));
    }

    #[test]
    fn test_get_current_committee() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for multiple epochs
        for epoch in [95, 100, 98] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Should return the highest epoch (fallback iteration)
        let current = store.get_current_committee().unwrap().unwrap();
        assert_eq!(current.epoch, EpochNumber(100));
    }

    #[test]
    fn test_get_current_committee_with_meta() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for multiple epochs
        for epoch in [95, 100, 98] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Set current epoch to 98 (not the highest)
        store.set_current_epoch(EpochNumber(98)).unwrap();

        // Should return epoch 98 (from meta)
        let current = store.get_current_committee().unwrap().unwrap();
        assert_eq!(current.epoch, EpochNumber(98));
    }

    #[test]
    fn test_committee_not_found() {
        let store = TapeStore::new(MemoryStore::new());

        let result = store.get_committee(EpochNumber(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_old_committees() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for 5 epochs
        for epoch in [1, 2, 3, 4, 5] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Keep only 2 most recent
        store.delete_old_committees(2).unwrap();

        // Only epochs 4 and 5 should remain
        assert!(store.get_committee(EpochNumber(1)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(2)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(3)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(4)).unwrap().is_some());
        assert!(store.get_committee(EpochNumber(5)).unwrap().is_some());
    }

    #[test]
    fn test_committee_member_info() {
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
}

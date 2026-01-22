//! Metadata operations for node state tracking
//!
//! Provides storage for:
//! - Node status (Standby/Active/Recovering)
//! - Cluster genesis hash (for validation on node restart)
//! - Current epoch number
//! - Sync cursor (last processed slot)
//! - GC progress (started/completed epochs)

use crate::columns::{Gc, Meta, SyncCursor};
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochNumber, Hash, NodeStatus, SlotNumber, UnitKey};
use crate::TapeStore;
use store::Store;

// Meta keys
const NODE_STATUS_KEY: &str = "node_status";
const CLUSTER_HASH_KEY: &str = "cluster_hash";
const CURRENT_EPOCH_KEY: &str = "current_epoch";

// GC keys
const GC_STARTED_KEY: &str = "started";
const GC_COMPLETED_KEY: &str = "completed";

/// Operations for node metadata
pub trait MetaOps {
    // Node status
    fn get_node_status(&self) -> Result<Option<NodeStatus>>;
    fn set_node_status(&self, status: NodeStatus) -> Result<()>;

    // Cluster hash
    fn get_cluster_hash(&self) -> Result<Option<Hash>>;
    fn set_cluster_hash(&self, hash: Hash) -> Result<()>;

    // Current epoch
    fn get_current_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_current_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // Sync cursor
    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>>;
    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()>;

    // GC epochs
    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()>;
    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> MetaOps for TapeStore<S> {
    fn get_node_status(&self) -> Result<Option<NodeStatus>> {
        let key = NODE_STATUS_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.is_empty() {
                    return Ok(None);
                }
                let status = match bytes[0] {
                    0 => NodeStatus::Standby,
                    1 => NodeStatus::Active,
                    2 => NodeStatus::Recovering,
                    _ => NodeStatus::Standby,
                };
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    fn set_node_status(&self, status: NodeStatus) -> Result<()> {
        let key = NODE_STATUS_KEY.to_string();
        let bytes = vec![status as u8];
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_cluster_hash(&self) -> Result<Option<Hash>> {
        let key = CLUSTER_HASH_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 32 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 32,
                        actual: bytes.len(),
                    });
                }
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&bytes);
                Ok(Some(Hash::from(hash_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_cluster_hash(&self, hash: Hash) -> Result<()> {
        let key = CLUSTER_HASH_KEY.to_string();
        let bytes = hash.as_ref().to_vec();
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_current_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = CURRENT_EPOCH_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 8,
                        actual: bytes.len(),
                    });
                }
                let mut epoch_bytes = [0u8; 8];
                epoch_bytes.copy_from_slice(&bytes);
                let epoch = u64::from_le_bytes(epoch_bytes);
                Ok(Some(EpochNumber(epoch)))
            }
            None => Ok(None),
        }
    }

    fn set_current_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = CURRENT_EPOCH_KEY.to_string();
        let bytes = epoch.as_u64().to_le_bytes().to_vec();
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>> {
        Ok(self.get::<SyncCursor>(&UnitKey)?)
    }

    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()> {
        self.put::<SyncCursor>(&UnitKey, &slot)?;
        Ok(())
    }

    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_STARTED_KEY.to_string();
        Ok(self.get::<Gc>(&key)?)
    }

    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_STARTED_KEY.to_string();
        self.put::<Gc>(&key, &epoch)?;
        Ok(())
    }

    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_COMPLETED_KEY.to_string();
        Ok(self.get::<Gc>(&key)?)
    }

    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_COMPLETED_KEY.to_string();
        self.put::<Gc>(&key, &epoch)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_node_status_roundtrip() {
        let store = test_store();

        assert!(store.get_node_status().unwrap().is_none());

        store.set_node_status(NodeStatus::Active).unwrap();
        assert_eq!(store.get_node_status().unwrap(), Some(NodeStatus::Active));

        store.set_node_status(NodeStatus::Recovering).unwrap();
        assert_eq!(
            store.get_node_status().unwrap(),
            Some(NodeStatus::Recovering)
        );
    }

    #[test]
    fn test_cluster_hash_roundtrip() {
        let store = test_store();
        let hash = Hash::new_unique();

        assert!(store.get_cluster_hash().unwrap().is_none());

        store.set_cluster_hash(hash).unwrap();
        assert_eq!(store.get_cluster_hash().unwrap(), Some(hash));
    }

    #[test]
    fn test_current_epoch_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(12345);

        assert!(store.get_current_epoch().unwrap().is_none());

        store.set_current_epoch(epoch).unwrap();
        assert_eq!(store.get_current_epoch().unwrap(), Some(epoch));
    }

    #[test]
    fn test_sync_cursor_roundtrip() {
        let store = test_store();
        let slot = SlotNumber(999999);

        assert!(store.get_sync_cursor().unwrap().is_none());

        store.set_sync_cursor(slot).unwrap();
        assert_eq!(store.get_sync_cursor().unwrap(), Some(slot));
    }

    #[test]
    fn test_gc_epochs_roundtrip() {
        let store = test_store();
        let started = EpochNumber(100);
        let completed = EpochNumber(99);

        assert!(store.get_gc_started_epoch().unwrap().is_none());
        assert!(store.get_gc_completed_epoch().unwrap().is_none());

        store.set_gc_started_epoch(started).unwrap();
        store.set_gc_completed_epoch(completed).unwrap();

        assert_eq!(store.get_gc_started_epoch().unwrap(), Some(started));
        assert_eq!(store.get_gc_completed_epoch().unwrap(), Some(completed));
    }
}

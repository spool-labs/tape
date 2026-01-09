//! Metadata operations for cursor and cluster tracking
//!
//! Provides storage for:
//! - Cluster genesis hash (for validation on node restart)
//! - Last processed slot cursor (for resumable block processing)

use crate::columns::Meta;
use crate::error::{Result, TapeStoreError};
use crate::TapeStore;
use store::Store;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;

/// Key for storing the cluster genesis hash
const CLUSTER_HASH_KEY: &str = "cluster_hash";

/// Key for storing the last processed slot cursor
const CURSOR_KEY: &str = "last_processed_slot";

/// Operations for node metadata (cursor, cluster validation)
pub trait MetaOps {
    /// Get the stored cluster genesis hash.
    ///
    /// Returns `None` if no hash has been set (fresh node).
    fn get_cluster_hash(&self) -> Result<Option<Hash>>;

    /// Set the cluster genesis hash.
    ///
    /// This should only be set once when the node first starts.
    /// Returns an error if a hash is already set (use `get_cluster_hash` to check first).
    fn set_cluster_hash(&self, hash: Hash) -> Result<()>;

    /// Get the last processed slot cursor.
    ///
    /// Returns `None` if no cursor has been set (fresh node).
    fn get_cursor(&self) -> Result<Option<SlotNumber>>;

    /// Set the last processed slot cursor.
    ///
    /// Called after successfully processing a batch of slots.
    fn set_cursor(&self, slot: SlotNumber) -> Result<()>;
}

impl<S: Store> MetaOps for TapeStore<S> {
    fn get_cluster_hash(&self) -> Result<Option<Hash>> {
        let key = CLUSTER_HASH_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 32 {
                    return Err(TapeStoreError::Serialization(format!(
                        "cluster hash: expected 32 bytes, got {}",
                        bytes.len()
                    )));
                }
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&bytes);
                Ok(Some(Hash::from(hash_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_cluster_hash(&self, hash: Hash) -> Result<()> {
        // Check if already set
        if self.get_cluster_hash()?.is_some() {
            return Err(TapeStoreError::Serialization(
                "cluster hash already set".to_string(),
            ));
        }

        let key = CLUSTER_HASH_KEY.to_string();
        let bytes = hash.as_ref().to_vec();
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_cursor(&self) -> Result<Option<SlotNumber>> {
        let key = CURSOR_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::Serialization(format!(
                        "cursor: expected 8 bytes, got {}",
                        bytes.len()
                    )));
                }
                let mut slot_bytes = [0u8; 8];
                slot_bytes.copy_from_slice(&bytes);
                let slot = u64::from_le_bytes(slot_bytes);
                Ok(Some(SlotNumber(slot)))
            }
            None => Ok(None),
        }
    }

    fn set_cursor(&self, slot: SlotNumber) -> Result<()> {
        let key = CURSOR_KEY.to_string();
        let bytes = slot.as_u64().to_le_bytes().to_vec();
        self.put::<Meta>(&key, &bytes)?;
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
    fn test_cluster_hash_roundtrip() {
        let store = test_store();
        let hash = Hash::new_unique();

        // Initially none
        assert!(store.get_cluster_hash().unwrap().is_none());

        // Set and retrieve
        store.set_cluster_hash(hash).unwrap();
        let retrieved = store.get_cluster_hash().unwrap();
        assert_eq!(retrieved, Some(hash));
    }

    #[test]
    fn test_cluster_hash_set_once() {
        let store = test_store();
        let hash1 = Hash::new_unique();
        let hash2 = Hash::new_unique();

        // First set succeeds
        store.set_cluster_hash(hash1).unwrap();

        // Second set fails
        let result = store.set_cluster_hash(hash2);
        assert!(result.is_err());

        // Original hash unchanged
        let retrieved = store.get_cluster_hash().unwrap();
        assert_eq!(retrieved, Some(hash1));
    }

    #[test]
    fn test_cursor_roundtrip() {
        let store = test_store();
        let slot = SlotNumber(123456789);

        // Initially none
        assert!(store.get_cursor().unwrap().is_none());

        // Set and retrieve
        store.set_cursor(slot).unwrap();
        let retrieved = store.get_cursor().unwrap();
        assert_eq!(retrieved, Some(slot));
    }

    #[test]
    fn test_cursor_update() {
        let store = test_store();

        // Set initial cursor
        store.set_cursor(SlotNumber(100)).unwrap();
        assert_eq!(store.get_cursor().unwrap(), Some(SlotNumber(100)));

        // Update cursor (should overwrite)
        store.set_cursor(SlotNumber(200)).unwrap();
        assert_eq!(store.get_cursor().unwrap(), Some(SlotNumber(200)));

        // Update again
        store.set_cursor(SlotNumber(300)).unwrap();
        assert_eq!(store.get_cursor().unwrap(), Some(SlotNumber(300)));
    }

    #[test]
    fn test_cursor_and_hash_independent() {
        let store = test_store();
        let hash = Hash::new_unique();
        let slot = SlotNumber(999);

        // Set both
        store.set_cluster_hash(hash).unwrap();
        store.set_cursor(slot).unwrap();

        // Both retrievable independently
        assert_eq!(store.get_cluster_hash().unwrap(), Some(hash));
        assert_eq!(store.get_cursor().unwrap(), Some(slot));
    }
}

//! Metadata operations for node state tracking
//!
//! Provides storage for:
//! - Node status (wincode-serialized)
//! - Cluster genesis hash
//! - Current epoch number
//! - Node address
//! - Sync cursor (last processed slot)
//! - GC progress (started/completed epochs)

use crate::columns::{GcCol, MetaCol, SyncCursorCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{ChunkIndex, EpochNumber, Hash, InvalidationProof, NodeStatus, Pubkey, SlotNumber, UnitKey};
use crate::TapeStore;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::types::NodeId;

// Meta keys
const NODE_STATUS_KEY: &str = "node_status";
const CLUSTER_HASH_KEY: &str = "cluster_hash";
const CURRENT_EPOCH_KEY: &str = "current_epoch";
const NODE_ADDRESS_KEY: &str = "node_address";
const NODE_ID_KEY: &str = "node_id";

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

    // Node address
    fn get_node_address(&self) -> Result<Option<Pubkey>>;
    fn set_node_address(&self, address: Pubkey) -> Result<()>;

    // Node ID
    fn get_node_id(&self) -> Result<Option<NodeId>>;
    fn set_node_id(&self, id: NodeId) -> Result<()>;

    // Sync cursor
    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>>;
    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()>;

    // GC epochs
    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()>;
    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // Snapshot commitments (per epoch+chunk_index)
    fn get_snapshot_commitment(&self, epoch: EpochNumber, chunk_index: ChunkIndex) -> Result<Option<Hash>>;
    fn set_snapshot_commitment(&self, epoch: EpochNumber, chunk_index: ChunkIndex, commitment: Hash) -> Result<()>;
    fn delete_snapshot_commitments(&self, epoch: EpochNumber) -> Result<()>;

    // Invalidation proofs
    fn get_invalidation_proof(&self, track: Pubkey) -> Result<Option<InvalidationProof>>;
    fn set_invalidation_proof(&self, track: Pubkey, proof: InvalidationProof) -> Result<()>;
    fn delete_invalidation_proof(&self, track: Pubkey) -> Result<()>;
}

impl<S: Store> MetaOps for TapeStore<S> {
    fn get_node_status(&self) -> Result<Option<NodeStatus>> {
        let key = NODE_STATUS_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.is_empty() {
                    return Ok(None);
                }
                let status: NodeStatus = wincode::deserialize(&bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("node status: {}", e)))?;
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    fn set_node_status(&self, status: NodeStatus) -> Result<()> {
        let key = NODE_STATUS_KEY.to_string();
        let bytes = wincode::serialize(&status)
            .map_err(|e| TapeStoreError::Serialization(format!("node status: {}", e)))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_cluster_hash(&self) -> Result<Option<Hash>> {
        let key = CLUSTER_HASH_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
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
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_current_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = CURRENT_EPOCH_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
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
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_node_address(&self) -> Result<Option<Pubkey>> {
        let key = NODE_ADDRESS_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.len() != 32 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 32,
                        actual: bytes.len(),
                    });
                }
                let mut addr_bytes = [0u8; 32];
                addr_bytes.copy_from_slice(&bytes);
                Ok(Some(Pubkey(addr_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_node_address(&self, address: Pubkey) -> Result<()> {
        let key = NODE_ADDRESS_KEY.to_string();
        let bytes = address.0.to_vec();
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_node_id(&self) -> Result<Option<NodeId>> {
        let key = NODE_ID_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 8,
                        actual: bytes.len(),
                    });
                }
                let mut id_bytes = [0u8; 8];
                id_bytes.copy_from_slice(&bytes);
                Ok(Some(NodeId(u64::from_le_bytes(id_bytes))))
            }
            None => Ok(None),
        }
    }

    fn set_node_id(&self, id: NodeId) -> Result<()> {
        let key = NODE_ID_KEY.to_string();
        let bytes = id.0.to_le_bytes().to_vec();
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>> {
        Ok(self.get::<SyncCursorCol>(&UnitKey)?)
    }

    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()> {
        self.put::<SyncCursorCol>(&UnitKey, &slot)?;
        Ok(())
    }

    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_STARTED_KEY.to_string();
        Ok(self.get::<GcCol>(&key)?)
    }

    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_STARTED_KEY.to_string();
        self.put::<GcCol>(&key, &epoch)?;
        Ok(())
    }

    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_COMPLETED_KEY.to_string();
        Ok(self.get::<GcCol>(&key)?)
    }

    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_COMPLETED_KEY.to_string();
        self.put::<GcCol>(&key, &epoch)?;
        Ok(())
    }

    fn get_snapshot_commitment(&self, epoch: EpochNumber, chunk_index: ChunkIndex) -> Result<Option<Hash>> {
        let key = format!("snapshot:{}:{}", epoch.as_u64(), chunk_index.as_u64());
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.len() != 32 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 32,
                        actual: bytes.len(),
                    });
                }
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&bytes);
                Ok(Some(Hash(hash_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_snapshot_commitment(&self, epoch: EpochNumber, chunk_index: ChunkIndex, commitment: Hash) -> Result<()> {
        let key = format!("snapshot:{}:{}", epoch.as_u64(), chunk_index.as_u64());
        let bytes = commitment.0.to_vec();
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn delete_snapshot_commitments(&self, epoch: EpochNumber) -> Result<()> {
        for i in 0..SPOOL_GROUP_COUNT {
            let key = format!("snapshot:{}:{}", epoch.as_u64(), i);
            self.delete::<MetaCol>(&key)?;
        }
        Ok(())
    }

    fn get_invalidation_proof(&self, track: Pubkey) -> Result<Option<InvalidationProof>> {
        let key = format!("invalidation:{}", track);
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                let proof: InvalidationProof = wincode::deserialize(&bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("invalidation proof: {}", e)))?;
                Ok(Some(proof))
            }
            None => Ok(None),
        }
    }

    fn set_invalidation_proof(&self, track: Pubkey, proof: InvalidationProof) -> Result<()> {
        let key = format!("invalidation:{}", track);
        let bytes = wincode::serialize(&proof)
            .map_err(|e| TapeStoreError::Serialization(format!("invalidation proof: {}", e)))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn delete_invalidation_proof(&self, track: Pubkey) -> Result<()> {
        let key = format!("invalidation:{}", track);
        self.delete::<MetaCol>(&key)?;
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

        store
            .set_node_status(NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(42),
            })
            .unwrap();
        assert_eq!(
            store.get_node_status().unwrap(),
            Some(NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(42)
            })
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
    fn test_node_address_roundtrip() {
        let store = test_store();
        let address = Pubkey::new_unique();

        assert!(store.get_node_address().unwrap().is_none());

        store.set_node_address(address).unwrap();
        assert_eq!(store.get_node_address().unwrap(), Some(address));
    }

    #[test]
    fn test_node_id_roundtrip() {
        let store = test_store();
        let id = NodeId(42);

        assert!(store.get_node_id().unwrap().is_none());

        store.set_node_id(id).unwrap();
        assert_eq!(store.get_node_id().unwrap(), Some(id));
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

    #[test]
    fn test_snapshot_commitment_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let hash = Hash::new_unique();

        assert!(store.get_snapshot_commitment(epoch, ChunkIndex(0)).unwrap().is_none());

        store.set_snapshot_commitment(epoch, ChunkIndex(0), hash).unwrap();
        assert_eq!(store.get_snapshot_commitment(epoch, ChunkIndex(0)).unwrap(), Some(hash));

        // Different chunk index returns None
        assert!(store.get_snapshot_commitment(epoch, ChunkIndex(1)).unwrap().is_none());

        // Set multiple chunks, then delete all
        store.set_snapshot_commitment(epoch, ChunkIndex(1), Hash::new_unique()).unwrap();
        store.set_snapshot_commitment(epoch, ChunkIndex(2), Hash::new_unique()).unwrap();
        store.delete_snapshot_commitments(epoch).unwrap();

        assert!(store.get_snapshot_commitment(epoch, ChunkIndex(0)).unwrap().is_none());
        assert!(store.get_snapshot_commitment(epoch, ChunkIndex(1)).unwrap().is_none());
        assert!(store.get_snapshot_commitment(epoch, ChunkIndex(2)).unwrap().is_none());
    }

    #[test]
    fn test_invalidation_proof_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let proof = InvalidationProof {
            bitmap: 0xFF,
            signature: [1u8; 32],
            computed_root: [2u8; 32],
        };

        assert!(store.get_invalidation_proof(track).unwrap().is_none());

        store.set_invalidation_proof(track, proof.clone()).unwrap();
        assert_eq!(store.get_invalidation_proof(track).unwrap(), Some(proof));

        store.delete_invalidation_proof(track).unwrap();
        assert!(store.get_invalidation_proof(track).unwrap().is_none());
    }
}

//! Metadata operations for node state tracking
//!
//! Provides storage for:
//! - Node status (wincode-serialized)
//! - Cluster genesis hash
//! - Chain epoch number
//! - Node address
//! - Sync cursor (last processed slot)
//! - GC progress (started/completed epochs)

use crate::columns::{GcCol, MetaCol, SyncCursorCol};
use crate::error::{Result, TapeStoreError};
use crate::TapeStore;
use store::Store;
use tape_core::types::NodeId;
use tape_crypto::address::Address;

use crate::types::{EpochNumber, Hash, InvalidationProof, SlotNumber, UnitKey};

// Meta keys
const CLUSTER_HASH_KEY: &str = "cluster_hash";
const NODE_ADDRESS_KEY: &str = "node_address";
const NODE_ID_KEY: &str = "node_id";
const SNAPSHOT_BOOTSTRAP_TARGET_EPOCH_KEY: &str = "snapshot_bootstrap_target_epoch";

// GC keys
const GC_STARTED_KEY: &str = "started";
const GC_COMPLETED_KEY: &str = "completed";

/// Operations for node metadata
pub trait MetaOps {
    // Cluster hash
    fn get_cluster_hash(&self) -> Result<Option<Hash>>;
    fn set_cluster_hash(&self, hash: Hash) -> Result<()>;

    // Node address
    fn get_node_address(&self) -> Result<Option<Address>>;
    fn set_node_address(&self, address: Address) -> Result<()>;

    // Node ID
    fn get_node_id(&self) -> Result<Option<NodeId>>;
    fn set_node_id(&self, id: NodeId) -> Result<()>;

    // Sync cursor
    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>>;
    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()>;

    // Snapshot bootstrap marker
    fn get_bootstrap_target_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_bootstrap_target_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // GC epochs
    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()>;
    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // Invalidation proofs
    fn get_invalidation_proof(&self, track: Address) -> Result<Option<InvalidationProof>>;
    fn set_invalidation_proof(&self, track: Address, proof: InvalidationProof) -> Result<()>;
    fn delete_invalidation_proof(&self, track: Address) -> Result<()>;

    // Epoch nonce (randomness seed for leader schedule)
    fn get_epoch_nonce(&self, epoch: EpochNumber) -> Result<Option<Hash>>;
    fn set_epoch_nonce(&self, epoch: EpochNumber, nonce: Hash) -> Result<()>;
    fn get_epoch_start_ts(&self, epoch: EpochNumber) -> Result<Option<i64>>;
    fn set_epoch_start_ts(&self, epoch: EpochNumber, ts: i64) -> Result<()>;
}

impl<S: Store> MetaOps for TapeStore<S> {
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
        self.put::<MetaCol>(&key, &hash.as_ref().to_vec())?;
        Ok(())
    }

    fn get_node_address(&self) -> Result<Option<Address>> {
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
                Ok(Some(Address::from(addr_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_node_address(&self, address: Address) -> Result<()> {
        let key = NODE_ADDRESS_KEY.to_string();
        self.put::<MetaCol>(&key, &address.to_bytes().to_vec())?;
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
        self.put::<MetaCol>(&key, &id.0.to_le_bytes().to_vec())?;
        Ok(())
    }

    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>> {
        Ok(self.get::<SyncCursorCol>(&UnitKey)?)
    }

    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()> {
        self.put::<SyncCursorCol>(&UnitKey, &slot)?;
        Ok(())
    }

    fn get_bootstrap_target_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = SNAPSHOT_BOOTSTRAP_TARGET_EPOCH_KEY.to_string();
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
                Ok(Some(EpochNumber(u64::from_le_bytes(epoch_bytes))))
            }
            None => Ok(None),
        }
    }

    fn set_bootstrap_target_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = SNAPSHOT_BOOTSTRAP_TARGET_EPOCH_KEY.to_string();
        self.put::<MetaCol>(&key, &epoch.as_u64().to_le_bytes().to_vec())?;
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

    fn get_invalidation_proof(&self, track: Address) -> Result<Option<InvalidationProof>> {
        let key = format!("invalidation:{}", track);
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                let proof: InvalidationProof = wincode::deserialize(&bytes).map_err(|e| {
                    TapeStoreError::Serialization(format!("invalidation proof: {}", e))
                })?;
                Ok(Some(proof))
            }
            None => Ok(None),
        }
    }

    fn set_invalidation_proof(&self, track: Address, proof: InvalidationProof) -> Result<()> {
        let key = format!("invalidation:{}", track);
        let bytes = wincode::serialize(&proof)
            .map_err(|e| TapeStoreError::Serialization(format!("invalidation proof: {}", e)))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn delete_invalidation_proof(&self, track: Address) -> Result<()> {
        let key = format!("invalidation:{}", track);
        self.delete::<MetaCol>(&key)?;
        Ok(())
    }

    fn get_epoch_nonce(&self, epoch: EpochNumber) -> Result<Option<Hash>> {
        let key = format!("epoch_nonce:{}", epoch.as_u64());
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

    fn set_epoch_nonce(&self, epoch: EpochNumber, nonce: Hash) -> Result<()> {
        let key = format!("epoch_nonce:{}", epoch.as_u64());
        self.put::<MetaCol>(&key, &nonce.0.to_vec())?;
        Ok(())
    }

    fn get_epoch_start_ts(&self, epoch: EpochNumber) -> Result<Option<i64>> {
        let key = format!("epoch_start_ts:{}", epoch.as_u64());
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 8,
                        actual: bytes.len(),
                    });
                }
                let mut ts_bytes = [0u8; 8];
                ts_bytes.copy_from_slice(&bytes);
                Ok(Some(i64::from_le_bytes(ts_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_epoch_start_ts(&self, epoch: EpochNumber, ts: i64) -> Result<()> {
        let key = format!("epoch_start_ts:{}", epoch.as_u64());
        self.put::<MetaCol>(&key, &ts.to_le_bytes().to_vec())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsSignature;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
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
    fn test_node_address_roundtrip() {
        let store = test_store();
        let address = Address::new_unique();

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
    fn test_bootstrap_target_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(55);

        assert!(store.get_bootstrap_target_epoch().unwrap().is_none());

        store.set_bootstrap_target_epoch(epoch).unwrap();
        assert_eq!(store.get_bootstrap_target_epoch().unwrap(), Some(epoch));
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
    fn epoch_nonce_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let nonce = Hash::new_unique();

        assert!(store.get_epoch_nonce(epoch).unwrap().is_none());

        store.set_epoch_nonce(epoch, nonce).unwrap();
        assert_eq!(store.get_epoch_nonce(epoch).unwrap(), Some(nonce));
        assert!(store.get_epoch_nonce(EpochNumber(43)).unwrap().is_none());
    }

    #[test]
    fn epoch_start_ts_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(7);

        assert!(store.get_epoch_start_ts(epoch).unwrap().is_none());
        store.set_epoch_start_ts(epoch, 1_700_000_000).unwrap();
        assert_eq!(store.get_epoch_start_ts(epoch).unwrap(), Some(1_700_000_000));
    }

    #[test]
    fn test_invalidation_proof_roundtrip() {
        let store = test_store();
        let track = Address::new_unique();
        let proof = InvalidationProof {
            bitmap: 0xFF,
            signature: BlsSignature(G1CompressedPoint([1u8; 32])),
            computed_root: [2u8; 32],
        };

        assert!(store.get_invalidation_proof(track).unwrap().is_none());

        store.set_invalidation_proof(track, proof.clone()).unwrap();
        assert_eq!(store.get_invalidation_proof(track).unwrap(), Some(proof));

        store.delete_invalidation_proof(track).unwrap();
        assert!(store.get_invalidation_proof(track).unwrap().is_none());
    }
}

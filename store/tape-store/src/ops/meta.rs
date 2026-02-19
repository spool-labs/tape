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
use crate::types::{
    ChunkIndex, EpochNumber, Hash, InvalidationProof, NodeStatus, Pubkey, SlotNumber,
    SnapshotCertResult, SnapshotChunkMeta, SnapshotPartialSignature, UnitKey,
};
use crate::TapeStore;
use store::Store;
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::system::EpochPhase;
use tape_core::types::NodeId;

const SNAPSHOT_PARTIAL_SIG_PREFIX: &str = "snapshot_partial_sig";

fn partial_sig_group_prefix(epoch: EpochNumber, group: u64) -> String {
    format!("{SNAPSHOT_PARTIAL_SIG_PREFIX}:{}:{}:", epoch.as_u64(), group)
}

fn parse_partial_sig_key(key: &[u8]) -> Option<(EpochNumber, u64, u8)> {
    let key = std::str::from_utf8(key).ok()?;
    let parts = key.split(':').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != SNAPSHOT_PARTIAL_SIG_PREFIX {
        return None;
    }

    let epoch = parts
        .get(1)
        .and_then(|p| p.parse::<u64>().ok())
        .map(EpochNumber)?;
    let group = parts.get(2).and_then(|p| p.parse::<u64>().ok())?;
    let member_index = parts.get(3).and_then(|p| p.parse::<u8>().ok())?;

    Some((epoch, group, member_index))
}

// Meta keys
const NODE_STATUS_KEY: &str = "node_status";
const CLUSTER_HASH_KEY: &str = "cluster_hash";
const CHAIN_EPOCH_KEY: &str = "chain_epoch";
const CHAIN_EPOCH_PHASE_KEY: &str = "chain_epoch_phase";
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

    // Chain epoch
    fn get_chain_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_chain_epoch(&self, epoch: EpochNumber) -> Result<()>;
    fn get_chain_epoch_phase(&self) -> Result<Option<EpochPhase>>;
    fn set_chain_epoch_phase(&self, phase: EpochPhase) -> Result<()>;

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

    // Snapshot metadata (encoding params + leaf hashes for registration)
    fn get_snapshot_metadata(&self, epoch: EpochNumber, chunk: ChunkIndex) -> Result<Option<SnapshotChunkMeta>>;
    fn set_snapshot_metadata(&self, epoch: EpochNumber, chunk: ChunkIndex, meta: SnapshotChunkMeta) -> Result<()>;
    fn delete_snapshot_metadata(&self, epoch: EpochNumber) -> Result<()>;

    // Snapshot cert results
    fn get_snapshot_cert(&self, epoch: EpochNumber, chunk: ChunkIndex) -> Result<Option<SnapshotCertResult>>;
    fn set_snapshot_cert(&self, epoch: EpochNumber, chunk: ChunkIndex, result: SnapshotCertResult) -> Result<()>;
    fn delete_snapshot_cert(&self, epoch: EpochNumber) -> Result<()>;

    /// Partial snapshot signatures (peer-pushed) for each group.
    fn set_snapshot_partial_signature(
        &self,
        epoch: EpochNumber,
        group: u64,
        partial: SnapshotPartialSignature,
    ) -> Result<()>;
    fn get_snapshot_partial_signature(
        &self,
        epoch: EpochNumber,
        group: u64,
        member_index: u8,
    ) -> Result<Option<SnapshotPartialSignature>>;
    fn get_snapshot_partial_signatures(
        &self,
        epoch: EpochNumber,
        group: u64,
    ) -> Result<Vec<SnapshotPartialSignature>>;
    fn delete_snapshot_partial_signatures(
        &self,
        epoch: EpochNumber,
        group: u64,
    ) -> Result<()>;
    fn delete_snapshot_partial_signatures_for_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // Invalidation proofs
    fn get_invalidation_proof(&self, track: Pubkey) -> Result<Option<InvalidationProof>>;
    fn set_invalidation_proof(&self, track: Pubkey, proof: InvalidationProof) -> Result<()>;
    fn delete_invalidation_proof(&self, track: Pubkey) -> Result<()>;

    // Epoch nonce (randomness seed for leader schedule)
    fn get_epoch_nonce(&self, epoch: EpochNumber) -> Result<Option<Hash>>;
    fn set_epoch_nonce(&self, epoch: EpochNumber, nonce: Hash) -> Result<()>;
    fn get_epoch_start_ts(&self, epoch: EpochNumber) -> Result<Option<i64>>;
    fn set_epoch_start_ts(&self, epoch: EpochNumber, ts: i64) -> Result<()>;
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

    fn get_chain_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = CHAIN_EPOCH_KEY.to_string();
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

    fn set_chain_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = CHAIN_EPOCH_KEY.to_string();
        let bytes = epoch.as_u64().to_le_bytes().to_vec();
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_chain_epoch_phase(&self) -> Result<Option<EpochPhase>> {
        let key = CHAIN_EPOCH_PHASE_KEY.to_string();
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 8,
                        actual: bytes.len(),
                    });
                }
                let mut phase_bytes = [0u8; 8];
                phase_bytes.copy_from_slice(&bytes);
                let phase_u64 = u64::from_le_bytes(phase_bytes);
                let phase = EpochPhase::try_from(phase_u64)
                    .map_err(|_| TapeStoreError::Serialization(format!("invalid epoch phase: {phase_u64}")))?;
                Ok(Some(phase))
            }
            None => Ok(None),
        }
    }

    fn set_chain_epoch_phase(&self, phase: EpochPhase) -> Result<()> {
        let key = CHAIN_EPOCH_PHASE_KEY.to_string();
        let bytes = u64::from(phase).to_le_bytes().to_vec();
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

    fn get_snapshot_metadata(&self, epoch: EpochNumber, chunk: ChunkIndex) -> Result<Option<SnapshotChunkMeta>> {
        let key = format!("snapshot_meta:{}:{}", epoch.as_u64(), chunk.as_u64());
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                let meta: SnapshotChunkMeta = wincode::deserialize(&bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("snapshot metadata: {}", e)))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    fn set_snapshot_metadata(&self, epoch: EpochNumber, chunk: ChunkIndex, meta: SnapshotChunkMeta) -> Result<()> {
        let key = format!("snapshot_meta:{}:{}", epoch.as_u64(), chunk.as_u64());
        let bytes = wincode::serialize(&meta)
            .map_err(|e| TapeStoreError::Serialization(format!("snapshot metadata: {}", e)))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn delete_snapshot_metadata(&self, epoch: EpochNumber) -> Result<()> {
        for i in 0..SPOOL_GROUP_COUNT {
            let key = format!("snapshot_meta:{}:{}", epoch.as_u64(), i);
            self.delete::<MetaCol>(&key)?;
        }
        Ok(())
    }

    fn get_snapshot_cert(&self, epoch: EpochNumber, chunk: ChunkIndex) -> Result<Option<SnapshotCertResult>> {
        let key = format!("snapshot_cert:{}:{}", epoch.as_u64(), chunk.as_u64());
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                let result: SnapshotCertResult = wincode::deserialize(&bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("snapshot cert: {}", e)))?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn set_snapshot_cert(&self, epoch: EpochNumber, chunk: ChunkIndex, result: SnapshotCertResult) -> Result<()> {
        let key = format!("snapshot_cert:{}:{}", epoch.as_u64(), chunk.as_u64());
        let bytes = wincode::serialize(&result)
            .map_err(|e| TapeStoreError::Serialization(format!("snapshot cert: {}", e)))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn delete_snapshot_cert(&self, epoch: EpochNumber) -> Result<()> {
        for i in 0..SPOOL_GROUP_COUNT {
            let key = format!("snapshot_cert:{}:{}", epoch.as_u64(), i);
            self.delete::<MetaCol>(&key)?;
        }
        Ok(())
    }

    fn set_snapshot_partial_signature(
        &self,
        epoch: EpochNumber,
        group: u64,
        partial: SnapshotPartialSignature,
    ) -> Result<()> {
        let key = format!(
            "{SNAPSHOT_PARTIAL_SIG_PREFIX}:{}:{}:{}",
            epoch.as_u64(),
            group,
            partial.member_index,
        );
        let bytes = wincode::serialize(&partial)
            .map_err(|e| TapeStoreError::Serialization(format!("snapshot partial signature: {e}")))?;
        self.put::<MetaCol>(&key, &bytes)?;
        Ok(())
    }

    fn get_snapshot_partial_signature(
        &self,
        epoch: EpochNumber,
        group: u64,
        member_index: u8,
    ) -> Result<Option<SnapshotPartialSignature>> {
        let key = format!(
            "{SNAPSHOT_PARTIAL_SIG_PREFIX}:{}:{}:{}",
            epoch.as_u64(),
            group,
            member_index
        );
        match self.get::<MetaCol>(&key)? {
            Some(bytes) => {
                let partial: SnapshotPartialSignature = wincode::deserialize(&bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("snapshot partial signature: {e}")))?;
                Ok(Some(partial))
            }
            None => Ok(None),
        }
    }

    fn get_snapshot_partial_signatures(
        &self,
        epoch: EpochNumber,
        group: u64,
    ) -> Result<Vec<SnapshotPartialSignature>> {
        let prefix = partial_sig_group_prefix(epoch, group);
        let mut partials = Vec::new();

        for (key, value_bytes) in self.inner().iter::<MetaCol>()? {
            if key.starts_with(&prefix) {
                let partial: SnapshotPartialSignature = wincode::deserialize(&value_bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!(
                        "snapshot partial signature: {e}"
                    )))?;
                partials.push(partial);
            }
        }
        Ok(partials)
    }

    fn delete_snapshot_partial_signatures(
        &self,
        epoch: EpochNumber,
        group: u64,
    ) -> Result<()> {
        let prefix = partial_sig_group_prefix(epoch, group);
        let keys: Vec<String> = self
            .inner()
            .iter::<MetaCol>()?
            .into_iter()
            .filter_map(|(key, _)| key.starts_with(&prefix).then_some(key))
            .collect();

        for key in keys {
            self.delete::<MetaCol>(&key)?;
        }
        Ok(())
    }

    fn delete_snapshot_partial_signatures_for_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let keys: Vec<String> = self
            .inner()
            .iter::<MetaCol>()?
            .into_iter()
            .filter_map(|(key, _)| {
                parse_partial_sig_key(key.as_bytes())
                    .filter(|(sig_epoch, _, _)| *sig_epoch == epoch)
                    .map(|_| key)
            })
            .collect();

        for key in keys {
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
        let bytes = nonce.0.to_vec();
        self.put::<MetaCol>(&key, &bytes)?;
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
    use tape_core::bls::BlsSignature;
    use store_memory::MemoryStore;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;

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
    fn test_chain_epoch_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(12345);

        assert!(store.get_chain_epoch().unwrap().is_none());

        store.set_chain_epoch(epoch).unwrap();
        assert_eq!(store.get_chain_epoch().unwrap(), Some(epoch));
    }

    #[test]
    fn test_chain_epoch_phase_roundtrip() {
        let store = test_store();

        assert!(store.get_chain_epoch_phase().unwrap().is_none());

        store.set_chain_epoch_phase(EpochPhase::Syncing).unwrap();
        assert_eq!(store.get_chain_epoch_phase().unwrap(), Some(EpochPhase::Syncing));
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
    fn snapshot_metadata_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(10);
        let chunk = ChunkIndex(5);

        assert!(store.get_snapshot_metadata(epoch, chunk).unwrap().is_none());

        let meta = SnapshotChunkMeta {
            leaves: vec![Hash::new_unique(); 20],
            stripe_size: 1024 * 1024,
            stripe_count: 3,
            encoding_type: 2,
            encoding_params: 0x100714,
        };

        store.set_snapshot_metadata(epoch, chunk, meta.clone()).unwrap();
        assert_eq!(store.get_snapshot_metadata(epoch, chunk).unwrap(), Some(meta));

        // Different chunk returns None
        assert!(store.get_snapshot_metadata(epoch, ChunkIndex(6)).unwrap().is_none());

        store.delete_snapshot_metadata(epoch).unwrap();
        assert!(store.get_snapshot_metadata(epoch, chunk).unwrap().is_none());
    }

    #[test]
    fn snapshot_cert_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(10);
        let chunk = ChunkIndex(5);

        assert!(store.get_snapshot_cert(epoch, chunk).unwrap().is_none());

        let cert = SnapshotCertResult {
            member_indices: vec![0, 2, 5, 7],
            signature: BlsSignature(G1CompressedPoint([0xAB; 32])),
            epoch: 10,
        };

        store.set_snapshot_cert(epoch, chunk, cert.clone()).unwrap();
        assert_eq!(store.get_snapshot_cert(epoch, chunk).unwrap(), Some(cert));

        store.delete_snapshot_cert(epoch).unwrap();
        assert!(store.get_snapshot_cert(epoch, chunk).unwrap().is_none());
    }

    #[test]
    fn epoch_nonce_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let nonce = Hash::new_unique();

        assert!(store.get_epoch_nonce(epoch).unwrap().is_none());

        store.set_epoch_nonce(epoch, nonce).unwrap();
        assert_eq!(store.get_epoch_nonce(epoch).unwrap(), Some(nonce));

        // Different epoch returns None
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
        let track = Pubkey::new_unique();
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

    #[test]
    fn partial_signature_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(7);
        let group = 3;

        assert!(store
            .get_snapshot_partial_signature(epoch, group, 0)
            .unwrap()
            .is_none());

        let sig_a = SnapshotPartialSignature {
            member_index: 2,
            signature: BlsSignature(G1CompressedPoint([0x11; 32])),
            epoch: epoch.0,
        };

        let sig_b = SnapshotPartialSignature {
            member_index: 5,
            signature: BlsSignature(G1CompressedPoint([0x22; 32])),
            epoch: epoch.0,
        };

        store
            .set_snapshot_partial_signature(epoch, group, sig_a.clone())
            .unwrap();
        store
            .set_snapshot_partial_signature(epoch, group, sig_b.clone())
            .unwrap();

        let mut sigs = store
            .get_snapshot_partial_signatures(epoch, group)
            .unwrap();
        sigs.sort_by_key(|partial| partial.member_index);

        assert_eq!(sigs, vec![sig_a.clone(), sig_b.clone()]);
        assert_eq!(
            store
                .get_snapshot_partial_signature(epoch, group, 2)
                .unwrap()
                .unwrap(),
            sig_a
        );

        assert!(store
            .delete_snapshot_partial_signatures(epoch, group)
            .is_ok());
        assert!(store
            .get_snapshot_partial_signature(epoch, group, 2)
            .unwrap()
            .is_none());

        assert!(store
            .delete_snapshot_partial_signatures_for_epoch(epoch)
            .is_ok());
    }
}

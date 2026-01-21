//! Slice operations
//!
//! Provides high-level operations for storing and retrieving slices
//! with the new key structure (spool_idx, track_address).

use crate::columns::*;
use crate::error::{Result, TapeStoreError};
use crate::types::{Hash, Pubkey, SliceKey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::{Column, Store, WriteBatch};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Merkle tree height for slice proofs (1024 slices = 2^10)
pub const MERKLE_HEIGHT: usize = 10;

/// Metadata for a slice (simplified + adds merkle proof)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceMeta {
    /// Original size
    pub len: u32,
    /// Merkle leaf hash of this slice
    pub leaf_hash: Hash,
    /// Merkle proof for serving downloads
    pub merkle_proof: [Hash; MERKLE_HEIGHT],
    /// When we received this slice (Unix timestamp)
    pub received_at: i64,
}

/// High-level operations for slice management
pub trait SliceOps {
    /// Store a slice with its metadata atomically
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index this slice belongs to
    /// * `track_address` - The track address (on-chain pubkey)
    /// * `data` - The slice data (potentially compressed)
    /// * `meta` - The slice metadata including merkle proof
    fn put_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        data: Vec<u8>,
        meta: SliceMeta,
    ) -> Result<()>;

    /// Get slice data and metadata for serving
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    ///
    /// # Returns
    /// Tuple of (data, metadata) if found
    fn get_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<Option<(Vec<u8>, SliceMeta)>>;

    /// Get all slices in a spool (for epoch transition sync)
    ///
    /// Iterates by spool prefix to get all tracks that have a slice for this spool.
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index to query
    ///
    /// # Returns
    /// Vector of (track_address, SliceMeta) tuples
    fn get_spool_slices(&self, spool_idx: u16) -> Result<Vec<(Pubkey, SliceMeta)>>;

    /// Delete a slice (for GC)
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track address
    fn delete_slice(&self, spool_idx: u16, track_address: Pubkey) -> Result<()>;
}

impl<S: Store> SliceOps for TapeStore<S> {
    fn put_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        data: Vec<u8>,
        meta: SliceMeta,
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        let key = SliceKey::new(spool_idx, track_address);

        // Serialize key and values
        let key_bytes = wincode::serialize(&key)
            .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
        let data_bytes = wincode::serialize(&data)
            .map_err(|e| TapeStoreError::Serialization(format!("slice data: {}", e)))?;
        let meta_bytes = wincode::serialize(&meta)
            .map_err(|e| TapeStoreError::Serialization(format!("slice meta: {}", e)))?;

        // Add both data and meta to batch (atomic)
        batch.put(SlicesData::CF_NAME, &key_bytes, &data_bytes);
        batch.put(SlicesMeta::CF_NAME, &key_bytes, &meta_bytes);

        // Execute atomically
        self.inner().inner().write_batch(batch)?;

        Ok(())
    }

    fn get_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<Option<(Vec<u8>, SliceMeta)>> {
        let key = SliceKey::new(spool_idx, track_address);

        // Get data
        let data = match self.get::<SlicesData>(&key)? {
            Some(d) => d,
            None => return Ok(None),
        };

        // Get metadata
        let meta = match self.get::<SlicesMeta>(&key)? {
            Some(m) => m,
            None => {
                // Inconsistent state: data exists but no metadata
                return Err(TapeStoreError::SliceNotFound(spool_idx, track_address));
            }
        };

        Ok(Some((data, meta)))
    }

    fn get_spool_slices(&self, spool_idx: u16) -> Result<Vec<(Pubkey, SliceMeta)>> {
        // Create prefix bytes for the spool_idx (2 bytes BE)
        let prefix_bytes = spool_idx.to_be_bytes();

        // Iterate with prefix to get all slices for this spool
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SlicesMeta::CF_NAME, &prefix_bytes)?;

        let mut slices = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;

            // Verify spool_idx matches (should be guaranteed by prefix)
            if key.spool_idx == spool_idx {
                let meta: SliceMeta = wincode::deserialize(&value_bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("slice meta: {}", e)))?;
                slices.push((key.track_address, meta));
            }
        }

        Ok(slices)
    }

    fn delete_slice(&self, spool_idx: u16, track_address: Pubkey) -> Result<()> {
        let mut batch = WriteBatch::new();
        let key = SliceKey::new(spool_idx, track_address);

        let key_bytes = wincode::serialize(&key)
            .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;

        // Delete both data and meta atomically
        batch.delete(SlicesData::CF_NAME, &key_bytes);
        batch.delete(SlicesMeta::CF_NAME, &key_bytes);

        self.inner().inner().write_batch(batch)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn create_test_meta() -> SliceMeta {
        SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            received_at: 123456789,
        }
    }

    #[test]
    fn put_and_get_slice() {
        let store = TapeStore::new(MemoryStore::new());
        let track_address = Pubkey::new_unique();
        let spool_idx = 42u16;
        let data = vec![0xAB; 1024];
        let meta = create_test_meta();

        // Put slice
        store
            .put_slice(spool_idx, track_address, data.clone(), meta.clone())
            .unwrap();

        // Get slice
        let (retrieved_data, retrieved_meta) =
            store.get_slice(spool_idx, track_address).unwrap().unwrap();

        assert_eq!(retrieved_data, data);
        assert_eq!(retrieved_meta, meta);
    }

    #[test]
    fn get_spool_slices() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_idx = 42u16;

        // Add several slices for the same spool
        let mut track_addresses = Vec::new();
        for _ in 0..5 {
            let track_address = Pubkey::new_unique();
            track_addresses.push(track_address);
            let data = vec![0xAB; 1024];
            let meta = create_test_meta();
            store
                .put_slice(spool_idx, track_address, data, meta)
                .unwrap();
        }

        // Add a slice for a different spool
        let other_track = Pubkey::new_unique();
        store
            .put_slice(99, other_track, vec![0; 100], create_test_meta())
            .unwrap();

        // Get slices for spool 42
        let slices = store.get_spool_slices(spool_idx).unwrap();
        assert_eq!(slices.len(), 5);

        // Verify all track addresses are present
        for (track_addr, _meta) in &slices {
            assert!(track_addresses.contains(track_addr));
        }
    }

    #[test]
    fn delete_slice() {
        let store = TapeStore::new(MemoryStore::new());
        let track_address = Pubkey::new_unique();
        let spool_idx = 42u16;
        let data = vec![0xAB; 1024];
        let meta = create_test_meta();

        // Put slice
        store
            .put_slice(spool_idx, track_address, data, meta)
            .unwrap();

        // Verify it exists
        assert!(store.get_slice(spool_idx, track_address).unwrap().is_some());

        // Delete slice
        store.delete_slice(spool_idx, track_address).unwrap();

        // Verify it's gone
        assert!(store.get_slice(spool_idx, track_address).unwrap().is_none());
    }

    #[test]
    fn slice_not_found() {
        let store = TapeStore::new(MemoryStore::new());
        let track_address = Pubkey::new_unique();

        let result = store.get_slice(42, track_address).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn spool_ordering() {
        let store = TapeStore::new(MemoryStore::new());

        // Add slices to different spools in random order
        let spools = [100u16, 1, 50, 200, 25];
        for spool_idx in spools {
            let track = Pubkey::new_unique();
            store
                .put_slice(spool_idx, track, vec![0; 10], create_test_meta())
                .unwrap();
        }

        // Verify we can query each spool independently
        for spool_idx in spools {
            let slices = store.get_spool_slices(spool_idx).unwrap();
            assert_eq!(slices.len(), 1);
        }
    }
}

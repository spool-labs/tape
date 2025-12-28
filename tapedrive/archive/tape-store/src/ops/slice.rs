//! Slice query operations

use crate::columns::*;
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochNumber, Hash, Pubkey, SliceKey, TrackNumber};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::{Column, Store};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Metadata for a slice
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceMeta {
    pub len: u32,
    pub leaf_hash: Hash,
    pub content_digest: Hash,
    pub compression: Compression,
    pub last_verified_at: i64,
    pub flags: u8,
}

/// Compression algorithm used for slice data
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

/// State tracking for a slice including ownership and lifecycle
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceState {
    pub current_epoch: EpochNumber,
    pub status: SliceStatus,
    pub prev_owner: Pubkey,
    pub current_owner: Pubkey,
    pub next_owner: Pubkey,
    pub repair_from: Pubkey,
    pub repair_last_attempt: i64,
    pub repair_retries: u16,
    pub handoff_to: Pubkey,
    pub handoff_last_attempt: i64,
    pub handoff_retries: u16,
    pub gc_at: i64,
    pub last_state_change: i64,
}

/// Status of a slice in its lifecycle
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SliceStatus {
    Unknown = 0,
    Required = 1,
    Present = 2,
    Verified = 3,
    RepairingFromPeer = 4,
    Uploading = 5,
    HandoffPending = 6,
    HandoffComplete = 7,
    Deletable = 8,
}

/// Assignment status for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum AssignmentStatus {
    None = 0,
    Active = 1,
    ActiveSync = 2,
    ActiveRecover = 3,
    LockedToMove = 4,
}

/// Sync progress for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SyncProgress {
    pub last_synced_track_id: u64,
    pub phase: SyncPhase,
}

/// Sync phase for a spool
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SyncPhase {
    Idle = 0,
    Ingesting = 1,
    Repairing = 2,
}

/// High-level operations for slice queries
pub trait SliceOps {
    /// Get all slices for a track
    ///
    /// Retrieves all slice metadata entries for a given track using
    /// prefix iteration on the track_id.
    ///
    /// # Arguments
    /// * `track_id` - The track number to query
    ///
    /// # Returns
    /// Vector of (spool_idx, SliceMeta) tuples in ascending spool order
    fn get_track_slices(&self, track_id: TrackNumber) -> Result<Vec<(u16, SliceMeta)>>;

    /// Get slices in a spool range for a track
    ///
    /// Uses range iteration to efficiently retrieve only slices within
    /// a specific spool index range.
    ///
    /// # Arguments
    /// * `track_id` - The track number to query
    /// * `start_spool` - Starting spool index (inclusive)
    /// * `end_spool` - Ending spool index (exclusive)
    ///
    /// # Returns
    /// Vector of (spool_idx, SliceMeta) tuples in the specified range
    fn get_track_slices_range(
        &self,
        track_id: TrackNumber,
        start_spool: u16,
        end_spool: u16,
    ) -> Result<Vec<(u16, SliceMeta)>>;

    /// Count slices for a track
    ///
    /// # Arguments
    /// * `track_id` - The track number to query
    ///
    /// # Returns
    /// Number of slices stored for this track
    fn count_track_slices(&self, track_id: TrackNumber) -> Result<usize>;

    /// Check if all slices exist for a track
    ///
    /// A track is complete when all 1024 slices (spools 0-1023) are present.
    ///
    /// # Arguments
    /// * `track_id` - The track number to check
    ///
    /// # Returns
    /// * `Ok(true)` if exactly 1024 slices exist
    /// * `Ok(false)` otherwise
    fn track_is_complete(&self, track_id: TrackNumber) -> Result<bool>;
}

impl<S: Store> SliceOps for TapeStore<S> {
    fn get_track_slices(&self, track_id: TrackNumber) -> Result<Vec<(u16, SliceMeta)>> {
        // Since SliceKey is (track_id, spool_idx), we need to use range iteration
        // from (track_id, 0) to (track_id, u16::MAX)
        let start_key = SliceKey::new(track_id, 0);
        let end_key = SliceKey::new(track_id, u16::MAX);

        let start_bytes = wincode::serialize(&start_key)
            .map_err(|e| TapeStoreError::Serialization(format!("start key: {}", e)))?;
        let end_bytes = wincode::serialize(&end_key)
            .map_err(|e| TapeStoreError::Serialization(format!("end key: {}", e)))?;

        // Range iteration from start to end (exclusive)
        let iter = self.inner().inner().iter_range(
            SlicesMeta::CF_NAME,
            &start_bytes,
            &end_bytes,
        )?;

        let mut slices = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;

            // Verify track_id matches (should be guaranteed by range)
            if key.track_id == track_id {
                let meta: SliceMeta = wincode::deserialize(&value_bytes)
                    .map_err(|e| TapeStoreError::Serialization(format!("slice meta: {}", e)))?;
                slices.push((key.spool_idx, meta));
            }
        }

        // Include the end_key if it exists
        if let Some(value_bytes) = self.inner().inner().get(SlicesMeta::CF_NAME, &end_bytes)? {
            let meta: SliceMeta = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice meta: {}", e)))?;
            slices.push((u16::MAX, meta));
        }

        Ok(slices)
    }

    fn get_track_slices_range(
        &self,
        track_id: TrackNumber,
        start_spool: u16,
        end_spool: u16,
    ) -> Result<Vec<(u16, SliceMeta)>> {
        if start_spool >= end_spool {
            return Ok(Vec::new());
        }

        // Create range keys
        let start_key = SliceKey::new(track_id, start_spool);
        let end_key = SliceKey::new(track_id, end_spool);

        let start_bytes = wincode::serialize(&start_key)
            .map_err(|e| TapeStoreError::Serialization(format!("start key: {}", e)))?;
        let end_bytes = wincode::serialize(&end_key)
            .map_err(|e| TapeStoreError::Serialization(format!("end key: {}", e)))?;

        // Range iteration
        let iter = self.inner().inner().iter_range(
            SlicesMeta::CF_NAME,
            &start_bytes,
            &end_bytes,
        )?;

        let mut slices = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            let meta: SliceMeta = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice meta: {}", e)))?;
            slices.push((key.spool_idx, meta));
        }

        Ok(slices)
    }

    fn count_track_slices(&self, track_id: TrackNumber) -> Result<usize> {
        // Use the same approach as get_track_slices
        let slices = self.get_track_slices(track_id)?;
        Ok(slices.len())
    }

    fn track_is_complete(&self, track_id: TrackNumber) -> Result<bool> {
        let count = self.count_track_slices(track_id)?;
        Ok(count == 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Hash;
    use store_memory::MemoryStore;

    #[test]
    fn get_track_slices() {
        let store = TapeStore::new(MemoryStore::new());

        let track_id = TrackNumber(1);

        // Add some slice metadata
        for spool_idx in [0u16, 10, 100, 500, 1000] {
            let key = SliceKey::new(track_id, spool_idx);
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                content_digest: Hash::default(),
                compression: Compression::Lz4,
                last_verified_at: 123456789,
                flags: 0,
            };
            store.put::<SlicesMeta>(&key, &meta).unwrap();
        }

        let slices = store.get_track_slices(track_id).unwrap();
        assert_eq!(slices.len(), 5);
        assert_eq!(slices[0].0, 0);
        assert_eq!(slices[1].0, 10);
        assert_eq!(slices[4].0, 1000);
    }

    #[test]
    fn get_track_slices_range() {
        let store = TapeStore::new(MemoryStore::new());

        let track_id = TrackNumber(1);

        // Add slice metadata
        for spool_idx in 0u16..20 {
            let key = SliceKey::new(track_id, spool_idx);
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                content_digest: Hash::default(),
                compression: Compression::Lz4,
                last_verified_at: 123456789,
                flags: 0,
            };
            store.put::<SlicesMeta>(&key, &meta).unwrap();
        }

        // Query range [5, 15)
        let slices = store.get_track_slices_range(track_id, 5, 15).unwrap();
        assert_eq!(slices.len(), 10);
        assert_eq!(slices[0].0, 5);
        assert_eq!(slices[9].0, 14);
    }

    #[test]
    fn count_track_slices() {
        let store = TapeStore::new(MemoryStore::new());

        let track_id = TrackNumber(1);

        // Add 50 slices
        for spool_idx in 0u16..50 {
            let key = SliceKey::new(track_id, spool_idx);
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                content_digest: Hash::default(),
                compression: Compression::Lz4,
                last_verified_at: 123456789,
                flags: 0,
            };
            store.put::<SlicesMeta>(&key, &meta).unwrap();
        }

        let count = store.count_track_slices(track_id).unwrap();
        assert_eq!(count, 50);
    }

    #[test]
    fn track_is_complete() {
        let store = TapeStore::new(MemoryStore::new());

        let track_id = TrackNumber(1);

        // Not complete with 0 slices
        assert!(!store.track_is_complete(track_id).unwrap());

        // Add all 1024 slices
        for spool_idx in 0u16..1024 {
            let key = SliceKey::new(track_id, spool_idx);
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                content_digest: Hash::default(),
                compression: Compression::Lz4,
                last_verified_at: 123456789,
                flags: 0,
            };
            store.put::<SlicesMeta>(&key, &meta).unwrap();
        }

        // Now complete
        assert!(store.track_is_complete(track_id).unwrap());
    }
}

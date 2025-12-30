//! Statistics and aggregation operations

use crate::columns::*;
use crate::error::Result;
use crate::TapeStore;
use store::Store;

/// Storage statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    /// Number of tracks in storage
    pub track_count: usize,
    /// Number of slice metadata entries
    pub slice_meta_count: usize,
    /// Number of slice data entries
    pub slice_data_count: usize,
    /// Number of assigned spools
    pub spool_count: usize,
    /// Number of pending recovery items
    pub pending_recover_count: usize,
    /// Number of pending handoff items
    pub pending_handoff_count: usize,
}

/// High-level operations for statistics and aggregations
pub trait StatsOps {
    /// Get aggregate storage statistics
    ///
    /// Iterates through column families to count entries and aggregate statistics.
    ///
    /// # Returns
    /// StorageStats with counts of tracks, slices, and spools
    fn get_storage_stats(&self) -> Result<StorageStats>;
}

impl<S: Store> StatsOps for TapeStore<S> {
    fn get_storage_stats(&self) -> Result<StorageStats> {
        // Count entries in each CF
        let track_count = self.iter::<Tracks>()?.len();
        let slice_meta_count = self.iter::<SlicesMeta>()?.len();
        let slice_data_count = self.iter::<SlicesData>()?.len();
        let spool_count = self.iter::<SpoolsAssigned>()?.len();
        let pending_recover_count = self.iter::<PendingRecover>()?.len();
        let pending_handoff_count = self.iter::<PendingHandoff>()?.len();

        Ok(StorageStats {
            track_count,
            slice_meta_count,
            slice_data_count,
            spool_count,
            pending_recover_count,
            pending_handoff_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{Compression, SliceMeta, SliceOps, SpoolOps, SpoolState, SpoolStatus, TrackInfo, TrackOps, MERKLE_HEIGHT};
    use crate::types::*;
    use store_memory::MemoryStore;

    #[test]
    fn get_storage_stats() {
        let store = TapeStore::new(MemoryStore::new());

        // Initially empty
        let stats = store.get_storage_stats().unwrap();
        assert_eq!(stats.track_count, 0);
        assert_eq!(stats.slice_meta_count, 0);
        assert_eq!(stats.slice_data_count, 0);
        assert_eq!(stats.spool_count, 0);

        // Add some data
        let track_address = Pubkey::new_unique();
        let track_info = TrackInfo {
            commitment_hash: Hash::default(),
            certified_epoch: EpochNumber(0),
            slice_count: 1,
        };
        store.put_track_info(track_address, track_info).unwrap();

        let slice_meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 123456789,
        };
        store.put_slice(0, track_address, vec![0u8; 1024], slice_meta).unwrap();

        let spool_state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };
        store.put_spool_state(42, spool_state).unwrap();

        // Verify stats
        let stats = store.get_storage_stats().unwrap();
        assert_eq!(stats.track_count, 1);
        assert_eq!(stats.slice_meta_count, 1);
        assert_eq!(stats.slice_data_count, 1);
        assert_eq!(stats.spool_count, 1);
    }
}

//! Statistics and aggregation operations

use crate::columns::*;
use crate::error::Result;
use crate::TapeStore;
use store::Store;

/// Storage statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    /// Number of tapes in storage
    pub tape_count: usize,
    /// Number of tracks in storage
    pub track_count: usize,
    /// Number of slice metadata entries
    pub slice_meta_count: usize,
    /// Number of slice data entries
    pub slice_data_count: usize,
}

/// High-level operations for statistics and aggregations
pub trait StatsOps {
    /// Get aggregate storage statistics
    ///
    /// Iterates through column families to count entries and aggregate statistics.
    ///
    /// # Returns
    /// StorageStats with counts of tapes, tracks, and slices
    fn get_storage_stats(&self) -> Result<StorageStats>;
}

impl<S: Store> StatsOps for TapeStore<S> {
    fn get_storage_stats(&self) -> Result<StorageStats> {
        // Count entries in each CF
        let tape_count = self.iter::<TapesById>()?.len();
        let track_count = self.iter::<TracksById>()?.len();
        let slice_meta_count = self.iter::<SlicesMeta>()?.len();
        let slice_data_count = self.iter::<SlicesData>()?.len();

        Ok(StorageStats {
            tape_count,
            track_count,
            slice_meta_count,
            slice_data_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{TapeOps, TrackOps};
    use crate::types::*;
    use store_memory::MemoryStore;

    #[test]
    fn get_storage_stats() {
        let store = TapeStore::new(MemoryStore::new());

        // Initially empty
        let stats = store.get_storage_stats().unwrap();
        assert_eq!(stats.tape_count, 0);
        assert_eq!(stats.track_count, 0);
        assert_eq!(stats.slice_meta_count, 0);
        assert_eq!(stats.slice_data_count, 0);

        // Add some data
        let tape = TapeData {
            id: TapeNumber(1),
            authority: Pubkey::default(),
            capacity: 1_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        store.put_tape(&tape).unwrap();

        let track = TrackData {
            id: TrackNumber(1),
            tape: Pubkey::default(),
            key: Hash::default(),
            size: 1024,
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::default(),
        };
        store.put_track(&track).unwrap();

        let slice_key = SliceKey::new(TrackNumber(1), 0);
        let slice_meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            content_digest: Hash::default(),
            compression: Compression::Lz4,
            last_verified_at: 123456789,
            flags: 0,
        };
        store.put::<SlicesMeta>(&slice_key, &slice_meta).unwrap();
        store.put::<SlicesData>(&slice_key, &vec![0u8; 1024]).unwrap();

        // Verify stats
        let stats = store.get_storage_stats().unwrap();
        assert_eq!(stats.tape_count, 1);
        assert_eq!(stats.track_count, 1);
        assert_eq!(stats.slice_meta_count, 1);
        assert_eq!(stats.slice_data_count, 1);
    }
}

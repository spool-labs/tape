//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - Slice info: Erasure coding metadata (hashes for verification)
//! - Tape info: Storage allocation metadata
//! - Track info: Blob metadata and certification status
//! - Slice data: Primary and recovery erasure-coded data
//! - Spool state: Epoch-namespaced spool assignments and sync progress
//! - Committee cache: Committee members for routing and verification
//!
//! # Column Families (12 total)
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata
//! - `slice_info`: Blob erasure coding metadata
//! - `tape_info`: Tape (storage allocation) metadata
//! - `track_info`: Track (blob) metadata
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot
//! - `gc`: GC progress tracking
//!
//! ## Epoch-Namespaced Spool Columns
//! - `spool/assigned`: Spool status per epoch
//! - `spool/sync_progress`: Sync cursor per spool per epoch
//! - `spool/pending_recovery`: Recovery queue per epoch
//!
//! ## Slice Data Columns (BlobDB)
//! - `spool/primary_slices`: Primary erasure-coded slices
//! - `spool/recovery_slices`: Recovery/parity slices
//!
//! ## Committee Column
//! - `committee`: Committee cache by epoch
//!
//! # Example
//!
//! ```
//! use tape_store::{TapeStore, MemoryStore, types::*, ops::*};
//!
//! let store = TapeStore::new(MemoryStore::new());
//!
//! // Store track info
//! let track_address = Pubkey::new([1u8; 32]);
//! let track_info = TrackInfo::new(
//!     Pubkey::new([2u8; 32]),
//!     EpochNumber(100),
//!     [0u8; 64],
//! );
//! store.put_track_info(track_address, track_info).unwrap();
//!
//! // Store a primary slice
//! let spool_id = 42u16;
//! let slice_data = PrimarySliceData::new(vec![0u8; 1024], 0);
//! store.put_primary_slice(spool_id, track_address, slice_data).unwrap();
//!
//! // Retrieve the slice
//! let retrieved = store.get_primary_slice(spool_id, track_address).unwrap();
//! assert!(retrieved.is_some());
//! ```

pub mod columns;
pub mod config;
pub mod error;
pub mod ops;
pub mod types;

use store::{Store, TypedStore};

pub use store::WriteBatch;
pub use store_memory::MemoryStore;
pub use store_rocks::RocksStore;

/// Wrapper around TypedStore providing tape-specific storage operations
pub struct TapeStore<S: Store> {
    inner: TypedStore<S>,
}

impl<S: Store> TapeStore<S> {
    /// Create a new TapeStore wrapping the given store
    pub fn new(store: S) -> Self {
        Self {
            inner: TypedStore::new(store),
        }
    }

    /// Get the inner TypedStore
    pub fn inner(&self) -> &TypedStore<S> {
        &self.inner
    }

    /// Get a mutable reference to the inner TypedStore
    pub fn inner_mut(&mut self) -> &mut TypedStore<S> {
        &mut self.inner
    }
}

// Delegate all TypedStore methods to inner
impl<S: Store> std::ops::Deref for TapeStore<S> {
    type Target = TypedStore<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> std::ops::DerefMut for TapeStore<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// RocksStore-specific constructors
impl TapeStore<RocksStore> {
    /// Open a primary TapeStore database with optimized configuration
    ///
    /// This constructor uses the recommended column family configurations from
    /// the `config` module, including:
    /// - PlainTable for fixed-size keys
    /// - BlobDB for large slice data
    /// - Prefix extractors for range queries
    ///
    /// # Arguments
    /// * `path` - Path to the RocksDB database directory
    pub fn open_primary<P: AsRef<std::path::Path>>(path: P) -> Result<Self, store::Error> {
        let db_opts = config::create_db_options();
        let cf_configs = config::create_tape_store_configs();
        let rocks = RocksStore::open_with_cf_config(path, db_opts, cf_configs)?;
        Ok(Self::new(rocks))
    }

    /// Open a read-only TapeStore replica
    ///
    /// Read-only databases cannot write data but can be opened by multiple processes
    /// simultaneously. This is useful for:
    /// - Web API servers that only need to read data
    /// - Analytics workloads
    /// - Monitoring and metrics collection
    /// - Load balancing read traffic across multiple instances
    ///
    /// # Arguments
    /// * `path` - Path to the RocksDB database directory
    pub fn open_read_only<P: AsRef<std::path::Path>>(path: P) -> Result<Self, store::Error> {
        let rocks = RocksStore::open_read_only(path, columns::ALL_COLUMN_FAMILIES)?;
        Ok(Self::new(rocks))
    }

    /// Open a secondary TapeStore instance for catch-up reads
    ///
    /// Secondary instances maintain their own write-ahead log (WAL) and can read from
    /// a primary database while it's being written to. The secondary must periodically
    /// call `catch_up_with_primary()` to sync with the primary's state.
    ///
    /// Use cases:
    /// - Read replicas that need to stay up-to-date with primary
    /// - Separating read and write workloads
    /// - Mining/validation workers reading from a syncing node
    /// - Database backups that can catch up incrementally
    ///
    /// # Arguments
    /// * `primary_path` - Path to the primary database directory
    /// * `secondary_path` - Path where the secondary instance will store its state
    pub fn open_secondary<P: AsRef<std::path::Path>>(
        primary_path: P,
        secondary_path: P,
    ) -> Result<Self, store::Error> {
        let rocks = RocksStore::open_secondary(
            primary_path,
            secondary_path,
            columns::ALL_COLUMN_FAMILIES,
        )?;
        Ok(Self::new(rocks))
    }

    /// Sync secondary instance with primary database
    ///
    /// This method must be called on secondary instances to catch up with changes
    /// made to the primary database. It's a no-op on primary or read-only instances.
    ///
    /// Call this method periodically (e.g., every 1-5 seconds) to keep the secondary
    /// instance up-to-date.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the sync fails
    pub fn catch_up_with_primary(&self) -> Result<(), store::Error> {
        self.inner.inner().catch_up_with_primary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::*;
    use crate::types::*;

    #[test]
    fn test_track_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(100), [0xAB; 64]);

        store.put_track_info(address, info.clone()).unwrap();
        let retrieved = store.get_track_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_slice_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = SliceInfo {
            encoding_type: EncodingType::Rotated,
            unencoded_length: 1024 * 1024,
            primary: vec![Hash::default(); 1024],
            recovery: vec![Hash::default(); 1024],
        };

        store.put_slice_info(address, info.clone()).unwrap();
        let retrieved = store.get_slice_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_spool_status_epoch_namespaced() {
        let store = TapeStore::new(MemoryStore::new());
        let epoch = EpochNumber(100);
        let spool_id = 42;

        store
            .set_spool_status(epoch, spool_id, SpoolStatus::Active)
            .unwrap();
        let status = store.get_spool_status(epoch, spool_id).unwrap();
        assert_eq!(status, Some(SpoolStatus::Active));

        // Different epoch should not have the status
        let other_epoch = EpochNumber(101);
        let status = store.get_spool_status(other_epoch, spool_id).unwrap();
        assert!(status.is_none());
    }

    #[test]
    fn test_committee_roundtrip() {
        use bytemuck::Zeroable;
        use tape_core::bls::BlsPubkey;

        let store = TapeStore::new(MemoryStore::new());

        let member1 = CommitteeMemberInfo {
            id: NodeId(1),
            pubkey: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let member2 = CommitteeMemberInfo {
            id: NodeId(2),
            pubkey: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.2:8080".to_string(),
        };

        let cache = CommitteeCache {
            epoch: EpochNumber(100),
            members: vec![member1, member2],
            spool_assignment: vec![0, 1, 0, 1],
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        };

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(cache));
    }

    #[test]
    fn test_slice_data_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let primary = PrimarySliceData::new(vec![0xAB; 1024], 128);
        let recovery = RecoverySliceData::new(vec![0xCD; 2048], 64);

        store
            .put_both_slices(spool_id, track, primary.clone(), recovery.clone())
            .unwrap();

        let retrieved_primary = store.get_primary_slice(spool_id, track).unwrap().unwrap();
        let retrieved_recovery = store.get_recovery_slice(spool_id, track).unwrap().unwrap();

        assert_eq!(retrieved_primary, primary);
        assert_eq!(retrieved_recovery, recovery);
    }

    #[test]
    fn test_meta_ops() {
        let store = TapeStore::new(MemoryStore::new());

        // Node status
        store.set_node_status(NodeStatus::Active).unwrap();
        assert_eq!(
            store.get_node_status().unwrap(),
            Some(NodeStatus::Active)
        );

        // Current epoch
        store.set_current_epoch(EpochNumber(100)).unwrap();
        assert_eq!(
            store.get_current_epoch().unwrap(),
            Some(EpochNumber(100))
        );

        // Sync cursor
        store.set_sync_cursor(SlotNumber(999)).unwrap();
        assert_eq!(
            store.get_sync_cursor().unwrap(),
            Some(SlotNumber(999))
        );

        // GC epochs
        store.set_gc_started_epoch(EpochNumber(50)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(49)).unwrap();
        assert_eq!(
            store.get_gc_started_epoch().unwrap(),
            Some(EpochNumber(50))
        );
        assert_eq!(
            store.get_gc_completed_epoch().unwrap(),
            Some(EpochNumber(49))
        );
    }

    #[test]
    fn test_slice_key_ordering() {
        let store = TapeStore::new(MemoryStore::new());

        // Insert slices in non-sequential spool order
        for spool_id in [100u16, 1, 50, 200, 25] {
            let track = Pubkey::new_unique();
            let data = PrimarySliceData::new(vec![0u8; 10], 0);
            store.put_primary_slice(spool_id, track, data).unwrap();
        }

        // Verify slices come back in sorted order by spool_id when iterating
        // Note: We iterate per-spool, so this just tests that each spool can be queried
        for spool_id in [1, 25, 50, 100, 200] {
            let slices: Vec<_> = store
                .iter_primary_slices_by_spool(spool_id)
                .unwrap()
                .collect();
            assert_eq!(slices.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn test_read_only_tape_store() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create primary and write some data
        {
            let store = TapeStore::open_primary(&path).unwrap();
            let track = Pubkey::new_unique();
            let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(0), [0; 64]);
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = TapeStore::open_read_only(&path).unwrap();

            // Can iterate tracks
            let tracks = ro_store
                .iter::<crate::columns::TrackInfoCol>()
                .unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn test_secondary_tape_store() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create primary and write initial data
        {
            let store = TapeStore::open_primary(&primary_path).unwrap();
            let track = Pubkey::new_unique();
            let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(0), [0; 64]);
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can iterate tracks
            let tracks = secondary
                .iter::<crate::columns::TrackInfoCol>()
                .unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }
}

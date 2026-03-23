#![allow(unexpected_cfgs)]

//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - Tape info: Storage allocation metadata
//! - Track info: Blob metadata with spool allocation and commitments
//! - Object info: Tracked object status (blacklisted, invalid, valid)
//! - Slice data: Raw erasure-coded data
//! - Spool state: Spool status, sync progress, pending repair/recovery
//!
//! # Column Families (11 total)
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata
//! - `tape`: Tape metadata
//! - `track`: Track metadata
//! - `object_info`: Object metadata
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot
//! - `gc`: GC progress tracking
//!
//! ## Spool Columns (NOT epoch-namespaced)
//! - `spool_status`: Spool status
//! - `spool_pending_repair`: Pending repair queue
//! - `spool_pending_recovery`: Pending recovery queue
//! - `spool_sync_cursor`: Sync cursor
//!
//! ## Slice Data Column (BlobDB)
//! - `slice`: Erasure-coded slice data

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
    pub fn open_primary<P: AsRef<std::path::Path>>(path: P) -> Result<Self, store::Error> {
        Self::open_primary_with_compaction_rate_limit(path, 100)
    }

    pub fn open_primary_with_compaction_rate_limit<P: AsRef<std::path::Path>>(
        path: P,
        compaction_rate_limit_mb_per_sec: u64,
    ) -> Result<Self, store::Error> {
        let db_opts =
            config::create_db_options_with_compaction_rate_limit_mb_per_sec(
                compaction_rate_limit_mb_per_sec,
            );
        let cf_configs = config::create_tape_store_configs();
        let rocks = RocksStore::open_with_cf_config(path, db_opts, cf_configs)?;
        Ok(Self::new(rocks))
    }

    /// Open a read-only TapeStore replica
    pub fn open_read_only<P: AsRef<std::path::Path>>(path: P) -> Result<Self, store::Error> {
        let rocks = RocksStore::open_read_only(path, columns::ALL_COLUMN_FAMILIES)?;
        Ok(Self::new(rocks))
    }

    /// Open a secondary TapeStore instance for catch-up reads
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

        let info = TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: SpoolGroup(3),
            original_size: 1024 * 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 2, // Clay
            encoding_params: 0,
            commitment: vec![],
        };

        store.put_track(address, info.clone()).unwrap();
        let retrieved = store.get_track(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = TapeInfo {
            end_epoch: EpochNumber(200),
        };

        store.put_tape(address, info.clone()).unwrap();
        let retrieved = store.get_tape(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_object_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = ObjectInfo::Valid {
            track_address: Pubkey::new_unique(),
            registered_epoch: EpochNumber(5),
            certified_epoch: Some(EpochNumber(6)),
            slot: SlotNumber(50),
        };

        store.put_object_info(address, info.clone()).unwrap();
        let retrieved = store.get_object_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_spool_status() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_id = 42;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(0)))
            .unwrap();
        let state = store.get_spool_state(spool_id).unwrap();
        assert!(state.unwrap().is_active());
    }

    #[test]
    fn test_slice_data_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = vec![0xAB; 1024];

        store
            .put_slice(spool_id, track, data.clone())
            .unwrap();

        let retrieved = store.get_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_meta_ops() {
        let store = TapeStore::new(MemoryStore::new());

        // Node address
        let addr = Pubkey::new_unique();
        store.set_node_address(addr).unwrap();
        assert_eq!(store.get_node_address().unwrap(), Some(addr));

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
            let data = vec![0u8; 10];
            store.put_slice(spool_id, track, data).unwrap();
        }

        // Verify slices come back when iterating per-spool
        for spool_id in [1, 25, 50, 100, 200] {
            let slices = store.iter_slices_by_spool(spool_id).unwrap();
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
            let info = TrackInfo {
                tape_address: Pubkey::new_unique(),
                spool_group: SpoolGroup(3),
                original_size: 1024,
                stripe_size: 0,
                stripe_count: 0,
                encoding_type: 1, // Basic
                encoding_params: 0,
                commitment: vec![],
            };
            store.put_track(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = TapeStore::open_read_only(&path).unwrap();

            // Can iterate tracks
            let tracks = ro_store
                .iter::<crate::columns::TrackCol>()
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
            let info = TrackInfo {
                tape_address: Pubkey::new_unique(),
                spool_group: SpoolGroup(0),
                original_size: 512,
                stripe_size: 0,
                stripe_count: 0,
                encoding_type: 1, // Basic
                encoding_params: 0,
                commitment: vec![],
            };
            store.put_track(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can iterate tracks
            let tracks = secondary
                .iter::<crate::columns::TrackCol>()
                .unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }
}

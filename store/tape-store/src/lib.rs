#![allow(unexpected_cfgs)]

//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - Tape info: Storage allocation metadata
//! - Track info: Canonical compressed-track catalog
//! - Track data: Locally stored track payloads
//! - Object info: Tracked object status (blacklisted, invalid, valid)
//! - Slice data: Raw erasure-coded data
//! - Spool state: Spool status, sync progress, pending repair/recovery
//!
//! # Column Families
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata
//! - `tape`: Tape metadata
//! - `track`: Track metadata
//! - `track_data`: Track payload data
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
pub mod stats;
pub mod types;

use std::path::Path;

use ops::SliceOps;
use store::{Store, TypedStore};
use store_rocks::{RocksStore, SplitStore};


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

/// Wrap an opened metadata store and bulk store in a split store, naming which
/// column families live on the bulk volume
fn split_store(meta: RocksStore, bulk: RocksStore) -> SplitStore {
    let bulk_cfs: Vec<String> = config::BULK_COLUMN_FAMILIES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    SplitStore::new(meta, bulk, bulk_cfs)
}

// Split-store constructors: a metadata store on the fast volume and a bulk
// store on the large volume, split by column family.
impl TapeStore<SplitStore> {
    /// Open a primary store under a single root directory
    ///
    /// The metadata store lives in a meta subdirectory and the bulk store in a
    /// bulk subdirectory, so a single-device box needs only one path. Use the
    /// split variant to place the bulk store on a separate device.
    pub fn open_primary<P: AsRef<Path>>(root: P) -> Result<Self, store::Error> {
        Self::open_primary_with_compaction_rate_limit(root, 100)
    }

    pub fn open_primary_with_compaction_rate_limit<P: AsRef<Path>>(
        root: P,
        compaction_rate_limit_mb_per_sec: u64,
    ) -> Result<Self, store::Error> {
        let root = root.as_ref();
        Self::open_primary_split(
            root.join(config::META_SUBDIR),
            root.join(config::BULK_SUBDIR),
            compaction_rate_limit_mb_per_sec,
        )
    }

    /// Open a primary store with the metadata and bulk stores at explicit,
    /// independent directories, typically on different devices
    pub fn open_primary_split<P: AsRef<Path>>(
        meta_dir: P,
        bulk_dir: P,
        compaction_rate_limit_mb_per_sec: u64,
    ) -> Result<Self, store::Error> {
        std::fs::create_dir_all(meta_dir.as_ref())?;
        std::fs::create_dir_all(bulk_dir.as_ref())?;

        let meta = RocksStore::open_with_cf_config(
            meta_dir.as_ref(),
            config::create_db_options_with_compaction_rate_limit_mb_per_sec(
                compaction_rate_limit_mb_per_sec,
            ),
            config::create_metadata_store_configs(),
        )?;
        let bulk = RocksStore::open_with_cf_config(
            bulk_dir.as_ref(),
            config::create_db_options_with_compaction_rate_limit_mb_per_sec(
                compaction_rate_limit_mb_per_sec,
            ),
            config::create_bulk_store_configs(),
        )?;
        let store = Self::new(split_store(meta, bulk));

        // A store written before the size index existed reports no slice totals
        // until the index is laid down.
        store.ensure_slice_size_index().map_err(|err| match err {
            error::TapeStoreError::Store(err) => err,
            other => store::Error::Database(other.to_string()),
        })?;
        Ok(store)
    }

    /// Open a read-only replica under a single root directory
    ///
    /// Passes the full column-family configs per volume: opening by column-family
    /// name alone uses default table options, which cannot read families written
    /// under a different table format.
    pub fn open_read_only<P: AsRef<Path>>(root: P) -> Result<Self, store::Error> {
        let root = root.as_ref();
        Self::open_read_only_split(root.join(config::META_SUBDIR), root.join(config::BULK_SUBDIR))
    }

    /// Open a read-only replica with the two volumes at explicit directories
    pub fn open_read_only_split<P: AsRef<Path>>(
        meta_dir: P,
        bulk_dir: P,
    ) -> Result<Self, store::Error> {
        let meta = RocksStore::open_read_only_with_cf_config(
            meta_dir.as_ref(),
            store_rocks::Options::default(),
            config::create_metadata_store_configs(),
        )?;
        let bulk = RocksStore::open_read_only_with_cf_config(
            bulk_dir.as_ref(),
            store_rocks::Options::default(),
            config::create_bulk_store_configs(),
        )?;
        Ok(Self::new(split_store(meta, bulk)))
    }

    /// Open a secondary store for catch-up reads
    ///
    /// The primary and secondary roots each contain meta and bulk
    /// subdirectories.
    pub fn open_secondary<P: AsRef<Path>>(
        primary_root: P,
        secondary_root: P,
    ) -> Result<Self, store::Error> {
        let primary_root = primary_root.as_ref();
        let secondary_root = secondary_root.as_ref();
        Self::open_secondary_split(
            primary_root.join(config::META_SUBDIR),
            secondary_root.join(config::META_SUBDIR),
            primary_root.join(config::BULK_SUBDIR),
            secondary_root.join(config::BULK_SUBDIR),
        )
    }

    /// Open a secondary for catch-up reads with the two volumes at explicit
    /// primary and secondary directories
    pub fn open_secondary_split<P: AsRef<Path>>(
        meta_primary: P,
        meta_secondary: P,
        bulk_primary: P,
        bulk_secondary: P,
    ) -> Result<Self, store::Error> {
        std::fs::create_dir_all(meta_secondary.as_ref())?;
        std::fs::create_dir_all(bulk_secondary.as_ref())?;

        let meta = RocksStore::open_secondary_with_cf_config(
            meta_primary.as_ref(),
            meta_secondary.as_ref(),
            config::create_db_options(),
            config::create_metadata_store_configs(),
        )?;
        let bulk = RocksStore::open_secondary_with_cf_config(
            bulk_primary.as_ref(),
            bulk_secondary.as_ref(),
            config::create_db_options(),
            config::create_bulk_store_configs(),
        )?;
        Ok(Self::new(split_store(meta, bulk)))
    }

    /// Sync both secondary instances with their primaries
    pub fn catch_up_with_primary(&self) -> Result<(), store::Error> {
        self.inner.inner().catch_up_with_primary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::*;
    use crate::types::{ObjectInfo, TapeInfo};
    use store_memory::MemoryStore;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::types::{EpochNumber, SlotNumber, GroupIndex, SpoolIndex, TapeNumber, TrackNumber};
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::StorageUnits;
    use tape_crypto::address::Address;
    use tape_crypto::Hash;

    #[test]
    fn test_track_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Address::new_unique();

        let info = CompressedTrack {
            tape: Address::new_unique(),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(1024 * 1024),
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        };

        store.put_track(address, info.clone()).unwrap();
        let retrieved = store.get_track(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Address::new_unique();

        let info = TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch: EpochNumber(200),
            next_track_number: TrackNumber(0),
        };

        store.put_tape(address, info.clone()).unwrap();
        let retrieved = store.get_tape(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_object_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Address::new_unique();

        let info = ObjectInfo::Valid {
            track_address: Address::new_unique(),
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
        let spool_id = SpoolIndex(42);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(0)))
            .unwrap();
        let state = store.get_spool_state(spool_id).unwrap();
        assert!(state.unwrap().is_active());
    }

    #[test]
    fn test_slice_data_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

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
        let addr = Address::new_unique();
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
        for spool_id in [100u64, 1, 50, 200, 25] {
            let track = Address::new_unique();
            let data = vec![0u8; 10];
            store.put_slice(SpoolIndex(spool_id), track, data).unwrap();
        }

        // Verify slices come back when iterating per-spool
        for spool_id in [1u64, 25, 50, 100, 200] {
            let slices = store.iter_slices_by_spool(SpoolIndex(spool_id)).unwrap();
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
            let track = Address::new_unique();
            let info = CompressedTrack {
                tape: Address::new_unique(),
                key: Hash::new_unique(),
                track_number: TrackNumber(0),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                size: StorageUnits::from_bytes(1024),
                group: GroupIndex(3),
                value_hash: Hash::new_unique(),
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
            let track = Address::new_unique();
            let info = CompressedTrack {
                tape: Address::new_unique(),
                key: Hash::new_unique(),
                track_number: TrackNumber(0),
                kind: TrackKind::Coded as u64,
                state: TrackState::Certified as u64,
                size: StorageUnits::from_bytes(512),
                group: GroupIndex(0),
                value_hash: Hash::new_unique(),
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

    fn certified_track(tape: Address, number: u64) -> CompressedTrack {
        CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(number),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(1024),
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        }
    }

    /// Seek-based iteration must see data after it leaves the memtable, in
    /// every open mode. Regression test for the PlainTable configuration
    /// that silently dropped flushed rows from `iter_from`
    #[test]
    #[cfg(not(miri))]
    fn flushed_iteration() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let store = TapeStore::open_primary(&primary_path).unwrap();

        let tape = Address::new_unique();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(60),
                    next_track_number: TrackNumber(5),
                },
            )
            .unwrap();

        let mut tracks = Vec::new();
        for number in 0..5 {
            let address = Address::new_unique();
            store.put_track(address, certified_track(tape, number)).unwrap();
            store
                .put_object_info(
                    address,
                    ObjectInfo::Valid {
                        track_address: address,
                        registered_epoch: EpochNumber(1),
                        certified_epoch: Some(EpochNumber(1)),
                        slot: SlotNumber(10),
                    },
                )
                .unwrap();
            tracks.push(address);
        }
        store
            .set_spool_state(SpoolIndex(7), SpoolState::new(SpoolStatus::Active, EpochNumber(0)))
            .unwrap();

        store.inner().inner().flush().unwrap();

        assert_eq!(store.count_tracks().unwrap(), 5);
        assert_eq!(store.iter_tracks_from(None, 100).unwrap().len(), 5);

        // Cursor pagination across the flushed CF.
        let first = store.iter_tracks_from(None, 2).unwrap();
        assert_eq!(first.len(), 2);
        let cursor = first[1].0;
        let rest = store.iter_tracks_from(Some(cursor), 100).unwrap();
        assert_eq!(rest.len(), 3);
        assert!(rest.iter().all(|(address, _)| *address != cursor));

        assert_eq!(store.iter_all_tapes().unwrap().len(), 1);
        assert_eq!(store.iter_all_spools().unwrap().len(), 1);
        assert!(store.get_object_info(tracks[0]).unwrap().is_some());
        drop(store);

        let read_only = TapeStore::open_read_only(&primary_path).unwrap();
        assert_eq!(read_only.count_tracks().unwrap(), 5);
        assert_eq!(read_only.iter_tracks_from(None, 100).unwrap().len(), 5);
        assert!(read_only.get_track(tracks[0]).unwrap().is_some());
        assert!(read_only.get_tape(tape).unwrap().is_some());
        drop(read_only);

        let secondary_path = dir.path().join("secondary");
        let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.count_tracks().unwrap(), 5);
        assert!(secondary.get_tape(tape).unwrap().is_some());
        assert!(secondary.get_object_info(tracks[0]).unwrap().is_some());
    }

    // Count files with the given extension anywhere under the directory
    #[cfg(not(miri))]
    fn count_files_with_ext(dir: &std::path::Path, ext: &str) -> usize {
        let mut total = 0;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += count_files_with_ext(&path, ext);
            } else if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                total += 1;
            }
        }
        total
    }

    // slice blobs land in the bulk directory and never in the metadata one
    #[test]
    #[cfg(not(miri))]
    fn split_placement() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let meta_dir = dir.path().join("nvme");
        let bulk_dir = dir.path().join("sata");
        let store = TapeStore::open_primary_split(&meta_dir, &bulk_dir, 100).unwrap();

        // A track is small metadata; a slice over the 256 KiB threshold is a blob.
        store
            .put_track(Address::new_unique(), certified_track(Address::new_unique(), 0))
            .unwrap();
        store
            .put_slice(SpoolIndex(7), Address::new_unique(), vec![0xAB; 512 * 1024])
            .unwrap();
        store.inner().inner().flush().unwrap();

        // Blob files exist only under the bulk directory.
        assert!(count_files_with_ext(&bulk_dir, "blob") > 0, "slice blob not in bulk dir");
        assert_eq!(count_files_with_ext(&meta_dir, "blob"), 0, "blob leaked into meta dir");

        // Both values read back through the unified store.
        assert_eq!(store.count_tracks().unwrap(), 1);
        assert_eq!(store.iter_slices_by_spool(SpoolIndex(7)).unwrap().len(), 1);
    }

    /// The RocksDB backend must observe the same results as MemoryStore,
    /// the reference implementation of the `Store` iteration contract.
    #[test]
    #[cfg(not(miri))]
    fn differential_memory_rocks() {
        use std::collections::HashMap;
        use tempfile::tempdir;

        fn seed<S: store::Store>(
            store: &TapeStore<S>,
            tape: Address,
            tracks: &[(Address, CompressedTrack)],
        ) {
            store
                .put_tape(
                    tape,
                    TapeInfo {
                        id: TapeNumber(9),
                        flags: 0,
                        end_epoch: EpochNumber(20),
                        next_track_number: TrackNumber(4),
                    },
                )
                .unwrap();
            for (address, info) in tracks {
                store.put_track(*address, info.clone()).unwrap();
            }
        }

        fn observe<S: store::Store>(
            store: &TapeStore<S>,
        ) -> (usize, HashMap<Address, CompressedTrack>, HashMap<Address, TapeInfo>) {
            (
                store.count_tracks().unwrap(),
                store.iter_tracks_from(None, 100).unwrap().into_iter().collect(),
                store.iter_all_tapes().unwrap().into_iter().collect(),
            )
        }

        let dir = tempdir().unwrap();
        let rocks = TapeStore::open_primary(dir.path()).unwrap();
        let memory = TapeStore::new(MemoryStore::new());

        let tape = Address::new_unique();
        let tracks: Vec<(Address, CompressedTrack)> = (0..4)
            .map(|number| (Address::new_unique(), certified_track(tape, number)))
            .collect();

        seed(&rocks, tape, &tracks);
        seed(&memory, tape, &tracks);
        rocks.inner().inner().flush().unwrap();

        assert_eq!(observe(&rocks), observe(&memory));
    }
}

//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - Track metadata (commitment hash, certification status, slice count)
//! - Erasure-coded slice data and metadata (with merkle proofs)
//! - Spool assignment tracking for epoch transitions
//! - Committee cache for routing and verification
//! - Recovery and handoff queues for sync operations
//! - Garbage collection scheduling
//!
//! # Column Families (9 total)
//!
//! - `meta`: Node configuration and metadata
//! - `tracks`: Minimal track info indexed by address
//! - `slices/data`: Slice blob data (BlobDB)
//! - `slices/meta`: Slice metadata with merkle proofs
//! - `spools/assigned`: Spool assignment tracking
//! - `committee`: Committee cache by epoch
//! - `pending/recover`: Recovery queue
//! - `pending/handoff`: Handoff queue
//! - `gc/scheduled`: Garbage collection index
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
//! let track_info = TrackInfo {
//!     commitment_hash: Hash::default(),
//!     certified_epoch: EpochNumber(0),
//!     slice_count: 0,
//! };
//! store.put_track_info(track_address, track_info).unwrap();
//!
//! // Store a slice
//! let spool_idx = 42u16;
//! let slice_meta = SliceMeta {
//!     len: 1024,
//!     leaf_hash: Hash::default(),
//!     merkle_proof: [Hash::default(); MERKLE_HEIGHT],
//!     compression: Compression::Lz4,
//!     received_at: 0,
//! };
//! store.put_slice(spool_idx, track_address, vec![0u8; 1024], slice_meta).unwrap();
//!
//! // Retrieve the slice
//! let (data, meta) = store.get_slice(spool_idx, track_address).unwrap().unwrap();
//! assert_eq!(meta.len, 1024);
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tape_store::TapeStore;
    ///
    /// let store = TapeStore::open_primary("/data/tapes")?;
    /// # Ok::<(), store::Error>(())
    /// ```
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tape_store::TapeStore;
    /// use tape_store::error::Result;
    ///
    /// fn example() -> Result<()> {
    ///     // Open read-only replica for serving read requests
    ///     let store = TapeStore::open_read_only("/data/tapes")?;
    ///
    ///     // Can read all data
    ///     use tape_store::types::*;
    ///     use tape_store::ops::*;
    ///     let track = store.get_track_info(Pubkey::new([1u8; 32]))?;
    ///
    ///     // Cannot write (RocksDB will return errors)
    ///     Ok(())
    /// }
    /// ```
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tape_store::TapeStore;
    /// use tape_store::error::Result;
    /// use std::time::Duration;
    ///
    /// fn example() -> Result<()> {
    ///     // Open secondary instance
    ///     let store = TapeStore::open_secondary("/data/tapes", "/data/tapes-secondary")?;
    ///
    ///     // Sync with primary before reading
    ///     store.catch_up_with_primary()?;
    ///
    ///     // Read data
    ///     use tape_store::types::*;
    ///     use tape_store::ops::*;
    ///     let track = store.get_track_info(Pubkey::new([1u8; 32]))?;
    ///
    ///     // In production, run sync in a background task
    ///     Ok(())
    /// }
    /// ```
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
    use crate::columns::*;
    use crate::ops::*;
    use crate::types::*;

    #[test]
    fn test_track_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(100),
            slice_count: 42,
        };

        store.put_track_info(address, info.clone()).unwrap();
        let retrieved = store.get_track_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_slice_meta_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::new_unique(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 123456789,
        };

        let key = SliceKey::new(42, Pubkey::new_unique());
        store.put::<SlicesMeta>(&key, &meta).unwrap();
        let retrieved = store.get::<SlicesMeta>(&key).unwrap();
        assert_eq!(retrieved, Some(meta));
    }

    #[test]
    fn test_spool_state_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };

        store.put_spool_state(42, state.clone()).unwrap();
        let retrieved = store.get_spool_state(42).unwrap();
        assert_eq!(retrieved, Some(state));
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
    fn test_gc_index() {
        let store = TapeStore::new(MemoryStore::new());
        let track = Pubkey::new_unique();

        let key = GcKey::new(123456789, 42, track);
        store.put::<GcScheduled>(&key, &()).unwrap();

        let exists = store.get::<GcScheduled>(&key).unwrap();
        assert_eq!(exists, Some(()));
    }

    #[test]
    fn test_meta_storage() {
        let store = TapeStore::new(MemoryStore::new());

        let key = "schema_version".to_string();
        let value = vec![0, 1, 0, 0]; // version 1.0.0

        store.put::<Meta>(&key, &value).unwrap();
        let retrieved = store.get::<Meta>(&key).unwrap();
        assert_eq!(retrieved, Some(value));
    }

    #[test]
    fn test_slice_key_ordering() {
        let store = TapeStore::new(MemoryStore::new());

        // Insert slices in non-sequential spool order
        for spool_idx in [100u16, 1, 50, 200, 25] {
            let track = Pubkey::new_unique();
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
                compression: Compression::Lz4,
                received_at: 0,
            };
            store.put_slice(spool_idx, track, vec![0u8; 10], meta).unwrap();
        }

        // Verify slices come back in sorted order by spool_idx
        let mut collected = Vec::new();
        for (key, _meta) in store.iter::<SlicesMeta>().unwrap() {
            collected.push(key.spool_idx);
        }

        assert_eq!(collected, vec![1, 25, 50, 100, 200]);
    }

    #[test]
    fn test_spool_prefix_iteration() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_idx = 42u16;

        // Add slices to spool 42
        for _ in 0..5 {
            let track = Pubkey::new_unique();
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
                compression: Compression::Lz4,
                received_at: 0,
            };
            store.put_slice(spool_idx, track, vec![0u8; 10], meta).unwrap();
        }

        // Add slices to other spools
        for other_spool in [1, 100, 500] {
            let track = Pubkey::new_unique();
            let meta = SliceMeta {
                len: 1024,
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
                compression: Compression::Lz4,
                received_at: 0,
            };
            store.put_slice(other_spool, track, vec![0u8; 10], meta).unwrap();
        }

        // Query just spool 42
        let spool_slices = store.get_spool_slices(42).unwrap();
        assert_eq!(spool_slices.len(), 5);
    }

    #[test]
    #[cfg(not(miri))] // Skip on miri as it uses tempfile
    fn test_read_only_tape_store() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create primary and write some data
        {
            let store = TapeStore::open_primary(&path).unwrap();
            let track = Pubkey::new_unique();
            let info = TrackInfo {
                commitment_hash: Hash::new_unique(),
                certified_epoch: EpochNumber(0),
                slice_count: 0,
            };
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = TapeStore::open_read_only(&path).unwrap();

            // Can iterate tracks
            let tracks = ro_store.iter::<Tracks>().unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))] // Skip on miri as it uses tempfile
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
                commitment_hash: Hash::new_unique(),
                certified_epoch: EpochNumber(0),
                slice_count: 5,
            };
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can iterate tracks
            let tracks = secondary.iter::<Tracks>().unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))] // Skip on miri as it uses tempfile
    fn test_secondary_catch_up() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create and keep primary open
        let primary = TapeStore::open_primary(&primary_path).unwrap();

        let track1 = Pubkey::new_unique();
        let info1 = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };
        primary.put_track_info(track1, info1).unwrap();
        primary.inner().inner().flush().unwrap();

        // Open secondary
        let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

        // Initial sync
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.iter::<Tracks>().unwrap().len(), 1);

        // Write more data to primary
        let track2 = Pubkey::new_unique();
        let info2 = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(1),
            slice_count: 10,
        };
        primary.put_track_info(track2, info2).unwrap();
        primary.inner().inner().flush().unwrap();

        // Catch up and verify we see the new data
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.iter::<Tracks>().unwrap().len(), 2);
    }

    #[test]
    #[cfg(not(miri))] // Skip on miri as it uses tempfile
    fn test_secondary_with_operation_traits() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        let track_address = Pubkey::new_unique();

        // Create primary and use high-level operations
        {
            let primary = TapeStore::open_primary(&primary_path).unwrap();
            let info = TrackInfo {
                commitment_hash: Hash::new_unique(),
                certified_epoch: EpochNumber(100),
                slice_count: 42,
            };

            // Use operation trait to store track
            primary.put_track_info(track_address, info).unwrap();
            primary.inner().inner().flush().unwrap();
        }

        // Open secondary and verify operation traits work
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();
            secondary.catch_up_with_primary().unwrap();

            // Use operation trait to read
            let found = secondary.get_track_info(track_address).unwrap();
            assert!(found.is_some());
            assert_eq!(found.unwrap().slice_count, 42);
        }
    }
}

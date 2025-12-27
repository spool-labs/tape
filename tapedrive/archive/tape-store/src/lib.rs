//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - On-chain account mirrors (tapes, tracks)
//! - Erasure-coded slice data (1024 slices per track, up to 32 MiB each)
//! - Epoch-based ownership rotation and assignment tracking
//! - Recovery queues and garbage collection indices
//!
//! # Example
//!
//! ```
//! use tape_store::{TapeStore, MemoryStore, types::*, columns::*};
//!
//! let store = TapeStore::new(MemoryStore::new());
//!
//! // Store a tape
//! let tape = TapeData {
//!     id: TapeNumber(1),
//!     authority: Pubkey::new_unique(),
//!     capacity: 1_000_000,
//!     used: 0,
//!     active_epoch: EpochNumber(100),
//!     expiry_epoch: EpochNumber(200),
//!     track_count: 0,
//! };
//! store.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape).unwrap();
//!
//! // Retrieve the tape
//! let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
//! assert_eq!(retrieved, Some(tape));
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
    ///
    /// // Open read-only replica for serving read requests
    /// let store = TapeStore::open_read_only("/data/tapes")?;
    ///
    /// // Can read all data
    /// use tape_store::types::*;
    /// use tape_store::columns::*;
    /// let tape = store.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    ///
    /// // Cannot write (RocksDB will return errors)
    /// # Ok::<(), store::Error>(())
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
    /// use std::time::Duration;
    ///
    /// // Open secondary instance
    /// let store = TapeStore::open_secondary("/data/tapes", "/data/tapes-secondary")?;
    ///
    /// // Sync with primary before reading
    /// store.catch_up_with_primary()?;
    ///
    /// // Read data
    /// use tape_store::types::*;
    /// use tape_store::columns::*;
    /// let tape = store.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    ///
    /// // In production, run sync in a background task
    /// // tokio::spawn(async move {
    /// //     let mut interval = tokio::time::interval(Duration::from_secs(1));
    /// //     loop {
    /// //         interval.tick().await;
    /// //         if let Err(e) = store.catch_up_with_primary() {
    /// //             eprintln!("Failed to sync with primary: {}", e);
    /// //         }
    /// //     }
    /// // });
    /// # Ok::<(), store::Error>(())
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tape_store::TapeStore;
    ///
    /// let secondary = TapeStore::open_secondary("/data/primary", "/data/secondary")?;
    ///
    /// // Periodic sync in a loop
    /// loop {
    ///     secondary.catch_up_with_primary()?;
    ///     std::thread::sleep(std::time::Duration::from_secs(1));
    /// }
    /// # Ok::<(), store::Error>(())
    /// ```
    pub fn catch_up_with_primary(&self) -> Result<(), store::Error> {
        self.inner.inner().catch_up_with_primary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columns::*;
    use crate::types::*;

    #[test]
    fn test_tape_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let tape = TapeData {
            id: TapeNumber(1),
            authority: Pubkey::new_unique(),
            capacity: 1_000_000,
            used: 500_000,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 42,
        };

        store.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape).unwrap();
        let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
        assert_eq!(retrieved, Some(tape));
    }

    #[test]
    fn test_track_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let track = TrackData {
            id: TrackNumber(1),
            tape: Pubkey::new_unique(),
            key: Hash::new_unique(),
            size: 1024,
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::new_unique(),
        };

        store.put::<TracksById>(&TrackKey(TrackNumber(1)), &track).unwrap();
        let retrieved = store.get::<TracksById>(&TrackKey(TrackNumber(1))).unwrap();
        assert_eq!(retrieved, Some(track));
    }

    #[test]
    fn test_slice_meta_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::new_unique(),
            content_digest: Hash::new_unique(),
            compression: Compression::Lz4,
            last_verified_at: 123456789,
            flags: 0,
        };

        let key = SliceKey::new(TrackNumber(1), 42);
        store.put::<SlicesMeta>(&key, &meta).unwrap();
        let retrieved = store.get::<SlicesMeta>(&key).unwrap();
        assert_eq!(retrieved, Some(meta));
    }

    #[test]
    fn test_slice_state_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let owner = Pubkey::new_unique();
        let state = SliceState {
            current_epoch: EpochNumber(100),
            status: SliceStatus::Verified,
            prev_owner: owner,
            current_owner: owner,
            next_owner: owner,
            repair_from: owner,
            repair_last_attempt: 0,
            repair_retries: 0,
            handoff_to: owner,
            handoff_last_attempt: 0,
            handoff_retries: 0,
            gc_at: 0,
            last_state_change: 123456789,
        };

        let key = SliceKey::new(TrackNumber(1), 42);
        store.put::<SlicesState>(&key, &state).unwrap();
        let retrieved = store.get::<SlicesState>(&key).unwrap();
        assert_eq!(retrieved, Some(state));
    }

    #[test]
    fn test_assignment_status() {
        let store = TapeStore::new(MemoryStore::new());

        let status = AssignmentStatus::ActiveSync;
        let key = SpoolKey(42);

        store.put::<AssignmentStatusCF>(&key, &status).unwrap();
        let retrieved = store.get::<AssignmentStatusCF>(&key).unwrap();
        assert_eq!(retrieved, Some(status));
    }

    #[test]
    fn test_sync_progress() {
        let store = TapeStore::new(MemoryStore::new());

        let progress = SyncProgress {
            last_synced_track_id: 1000,
            phase: SyncPhase::Ingesting,
        };
        let key = SpoolKey(42);

        store.put::<AssignmentProgressCF>(&key, &progress).unwrap();
        let retrieved = store.get::<AssignmentProgressCF>(&key).unwrap();
        assert_eq!(retrieved, Some(progress));
    }

    #[test]
    fn test_committee_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());

        let committee = CommitteeData {
            epoch: EpochNumber(100),
            members: vec![
                CommitteeMemberData {
                    id: NodeId(1),
                    stake: 1000,
                    weight: 100,
                },
                CommitteeMemberData {
                    id: NodeId(2),
                    stake: 2000,
                    weight: 200,
                },
            ],
            total_stake: 3000,
        };

        store.put::<CommitteeByEpoch>(&EpochNumber(100), &committee).unwrap();
        let retrieved = store.get::<CommitteeByEpoch>(&EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(committee));
    }

    #[test]
    fn test_recovery_queue() {
        let store = TapeStore::new(MemoryStore::new());

        let key = RecoveryKey::new(42, TrackNumber(1000));
        store.put::<PendingRecover>(&key, &()).unwrap();

        let exists = store.get::<PendingRecover>(&key).unwrap();
        assert_eq!(exists, Some(()));

        store.delete::<PendingRecover>(&key).unwrap();
        let gone = store.get::<PendingRecover>(&key).unwrap();
        assert_eq!(gone, None);
    }

    #[test]
    fn test_gc_index() {
        let store = TapeStore::new(MemoryStore::new());

        let key = GcKey::new(123456789, TrackNumber(1000), 42);
        store.put::<GcIndex>(&key, &()).unwrap();

        let exists = store.get::<GcIndex>(&key).unwrap();
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
    fn test_big_endian_ordering() {
        let store = TapeStore::new(MemoryStore::new());
        let tape = Pubkey::new_unique();
        let hash = Hash::new_unique();

        // Insert tracks in non-sequential order
        for id in [100u64, 1, 50, 200, 25] {
            let track = TrackData {
                id: TrackNumber(id),
                tape,
                key: hash,
                size: id,
                registered_epoch: EpochNumber(0),
                certified_epoch: EpochNumber(0),
                commitment_hash: hash,
            };
            store.put::<TracksById>(&TrackKey(TrackNumber(id)), &track).unwrap();
        }

        // Verify they come back in sorted order due to BE encoding
        let mut collected = Vec::new();
        for (key, _track) in store.iter::<TracksById>().unwrap() {
            collected.push(key.0 .0);
        }

        assert_eq!(collected, vec![1, 25, 50, 100, 200]);
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
            let tape = TapeData {
                id: TapeNumber(1),
                authority: Pubkey::new_unique(),
                capacity: 1_000_000,
                used: 500_000,
                active_epoch: EpochNumber(100),
                expiry_epoch: EpochNumber(200),
                track_count: 42,
            };
            store.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = TapeStore::open_read_only(&path).unwrap();

            // Can read the data
            let retrieved = ro_store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().id.0, 1);

            // Can iterate
            let tapes = ro_store.iter::<TapesById>().unwrap();
            assert_eq!(tapes.len(), 1);
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
            let tape = TapeData {
                id: TapeNumber(1),
                authority: Pubkey::new_unique(),
                capacity: 1_000_000,
                used: 0,
                active_epoch: EpochNumber(100),
                expiry_epoch: EpochNumber(200),
                track_count: 0,
            };
            store.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can read initial data
            let retrieved = secondary.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().id.0, 1);
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

        let tape1 = TapeData {
            id: TapeNumber(1),
            authority: Pubkey::new_unique(),
            capacity: 1_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        primary.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape1).unwrap();
        primary.inner().inner().flush().unwrap();

        // Open secondary
        let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

        // Initial sync
        secondary.catch_up_with_primary().unwrap();
        assert!(secondary.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap().is_some());

        // Write more data to primary
        let tape2 = TapeData {
            id: TapeNumber(2),
            authority: Pubkey::new_unique(),
            capacity: 2_000_000,
            used: 0,
            active_epoch: EpochNumber(101),
            expiry_epoch: EpochNumber(201),
            track_count: 0,
        };
        primary.put::<TapesById>(&TapeKey(TapeNumber(2)), &tape2).unwrap();
        primary.inner().inner().flush().unwrap();

        // Catch up and verify we see the new data
        secondary.catch_up_with_primary().unwrap();
        assert!(secondary.get::<TapesById>(&TapeKey(TapeNumber(2))).unwrap().is_some());
    }

    #[test]
    #[cfg(not(miri))] // Skip on miri as it uses tempfile
    fn test_secondary_with_operation_traits() {
        use tempfile::tempdir;
        use crate::ops::TapeOps;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        let authority = Pubkey::new_unique();

        // Create primary and use high-level operations
        {
            let primary = TapeStore::open_primary(&primary_path).unwrap();
            let tape = TapeData {
                id: TapeNumber(1),
                authority,
                capacity: 1_000_000,
                used: 0,
                active_epoch: EpochNumber(100),
                expiry_epoch: EpochNumber(200),
                track_count: 0,
            };

            // Use operation trait to atomically update all indices
            primary.put_tape(&tape).unwrap();
            primary.inner().inner().flush().unwrap();
        }

        // Open secondary and verify operation traits work
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();
            secondary.catch_up_with_primary().unwrap();

            // Use operation trait to read by address
            let found = secondary.get_tape_by_address(&authority).unwrap();
            assert!(found.is_some());
            assert_eq!(found.unwrap().id.0, 1);
        }
    }
}

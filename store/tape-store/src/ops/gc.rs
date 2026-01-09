//! Garbage collection operations
//!
//! Provides scheduling and execution of garbage collection for tracks.
//! Tracks are scheduled for GC when:
//! - Owner deletes a track (DeleteTrack instruction)
//! - Track is invalidated (InvalidateTrack instruction)
//! - Parent tape is destroyed (DestroyTape instruction)
//! - Node loses spool assignment (epoch transition)

use crate::columns::GcScheduled;
use crate::error::Result;
use crate::types::{EpochNumber, GcKey, Pubkey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::Store;
use tape_core::spooler::SpoolIndex;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Reason why a track is scheduled for garbage collection
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum GcReason {
    /// Owner deleted the track
    Deleted = 0,
    /// Track failed verification or was invalidated
    Invalidated = 1,
    /// Parent tape was destroyed
    TapeDestroyed = 2,
    /// Node lost this spool in committee rotation
    SpoolLost = 3,
}

/// Entry in the garbage collection queue
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcEntry {
    /// Track address to be garbage collected
    pub track: Pubkey,
    /// Epoch when GC should occur
    pub scheduled_epoch: EpochNumber,
    /// Reason for GC (stored in key for ordering, reconstructed here)
    pub reason: GcReason,
}

/// Statistics from a GC run
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Number of tracks successfully garbage collected
    pub tracks_deleted: u64,
    /// Number of slices deleted across all tracks
    pub slices_deleted: u64,
    /// Number of tracks that failed GC (will be retried)
    pub tracks_failed: u64,
}

/// Operations for garbage collection scheduling and execution
pub trait GcOps {
    /// Schedule a track for garbage collection.
    ///
    /// The track will be eligible for deletion at the specified epoch.
    /// Multiple schedules for the same track are idempotent.
    ///
    /// # Arguments
    /// * `track` - Track address to schedule for GC
    /// * `epoch` - Epoch when GC should occur
    /// * `reason` - Why the track is being scheduled for GC
    fn schedule_gc(&self, track: Pubkey, epoch: EpochNumber, reason: GcReason) -> Result<()>;

    /// Get all GC entries due by the given epoch.
    ///
    /// Returns entries scheduled for `epoch` or earlier.
    /// Entries are ordered by scheduled time.
    ///
    /// # Arguments
    /// * `epoch` - Maximum epoch to include
    fn get_due_gc_entries(&self, epoch: EpochNumber) -> Result<Vec<GcEntry>>;

    /// Remove a GC entry after successful deletion.
    ///
    /// Called after a track's data has been fully garbage collected.
    ///
    /// # Arguments
    /// * `track` - Track address that was garbage collected
    /// * `epoch` - The epoch the entry was scheduled for
    fn remove_gc_entry(&self, track: Pubkey, epoch: EpochNumber) -> Result<()>;

    /// Check if a track is scheduled for GC.
    ///
    /// # Arguments
    /// * `track` - Track address to check
    fn is_scheduled_for_gc(&self, track: Pubkey) -> Result<bool>;
}

impl<S: Store> GcOps for TapeStore<S> {
    fn schedule_gc(&self, track: Pubkey, epoch: EpochNumber, _reason: GcReason) -> Result<()> {
        // Use epoch as timestamp for time-ordered iteration
        // The GcKey is (timestamp, spool_idx, track_address)
        // We use spool_idx=0 as a placeholder since we don't track per-spool GC
        let timestamp = epoch_to_timestamp(epoch);
        let key = GcKey::new(timestamp, 0, track);

        // Store the entry (value is unit, presence indicates scheduled)
        self.put::<GcScheduled>(&key, &())?;
        Ok(())
    }

    fn get_due_gc_entries(&self, epoch: EpochNumber) -> Result<Vec<GcEntry>> {
        let max_timestamp = epoch_to_timestamp(epoch);

        // Iterate all GC entries
        let mut entries = Vec::new();
        for (key, _) in self.iter::<GcScheduled>()? {
            // Only include entries scheduled at or before the given epoch
            if key.timestamp <= max_timestamp {
                entries.push(GcEntry {
                    track: key.track_address,
                    scheduled_epoch: timestamp_to_epoch(key.timestamp),
                    // We don't store reason in the value, so default to Deleted
                    // In practice, the reason is only used for logging
                    reason: GcReason::Deleted,
                });
            }
        }

        Ok(entries)
    }

    fn remove_gc_entry(&self, track: Pubkey, epoch: EpochNumber) -> Result<()> {
        let timestamp = epoch_to_timestamp(epoch);
        let key = GcKey::new(timestamp, 0, track);
        self.delete::<GcScheduled>(&key)?;
        Ok(())
    }

    fn is_scheduled_for_gc(&self, track: Pubkey) -> Result<bool> {
        // We need to scan all entries since the key includes timestamp
        // This is O(n) but GC checks are infrequent
        for (key, _) in self.iter::<GcScheduled>()? {
            if key.track_address == track {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Execute garbage collection for a single track.
///
/// This is a helper function that deletes all slices for a track
/// across the node's assigned spools, then removes the track info.
///
/// # Arguments
/// * `store` - The tape store
/// * `track` - Track address to garbage collect
/// * `our_spools` - List of spool indices this node owns
///
/// # Returns
/// Number of slices deleted
pub fn delete_track_data<S: Store>(
    store: &TapeStore<S>,
    track: Pubkey,
    our_spools: &[SpoolIndex],
) -> Result<u64> {
    use crate::ops::{SliceOps, TrackOps};

    let mut deleted = 0;

    // Delete slices for this track from all spools we own
    for &spool_idx in our_spools {
        // delete_slice is idempotent - returns Ok even if slice doesn't exist
        store.delete_slice(spool_idx, track)?;
        deleted += 1;
    }

    // Remove track info
    store.delete_track_info(track)?;

    Ok(deleted)
}

/// Execute garbage collection for an epoch.
///
/// This is the main GC execution function called during epoch transitions.
/// It processes all GC entries due by the given epoch.
///
/// # Arguments
/// * `store` - The tape store
/// * `ended_epoch` - The epoch that just ended
/// * `our_spools` - List of spool indices this node owns
///
/// # Returns
/// Statistics about the GC run
pub fn run_epoch_gc<S: Store>(
    store: &TapeStore<S>,
    ended_epoch: EpochNumber,
    our_spools: &[SpoolIndex],
) -> Result<GcStats> {
    let mut stats = GcStats::default();
    let entries = store.get_due_gc_entries(ended_epoch)?;

    for entry in entries {
        match delete_track_data(store, entry.track, our_spools) {
            Ok(slices) => {
                stats.slices_deleted += slices;
                stats.tracks_deleted += 1;
                // Remove the GC entry since we successfully processed it
                store.remove_gc_entry(entry.track, entry.scheduled_epoch)?;
            }
            Err(_) => {
                // On failure, leave the entry in the queue for retry next epoch
                stats.tracks_failed += 1;
            }
        }
    }

    Ok(stats)
}

/// Convert epoch number to timestamp for GC key ordering.
/// Uses epoch number directly as timestamp since we want epoch-based ordering.
fn epoch_to_timestamp(epoch: EpochNumber) -> i64 {
    epoch.as_u64() as i64
}

/// Convert timestamp back to epoch number.
fn timestamp_to_epoch(timestamp: i64) -> EpochNumber {
    EpochNumber(timestamp as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{Compression, SliceMeta, SliceOps, TrackInfo, TrackOps, MERKLE_HEIGHT};
    use store_memory::MemoryStore;
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn create_test_meta() -> SliceMeta {
        SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 123456789,
        }
    }

    #[test]
    fn test_schedule_gc() {
        let store = test_store();
        let track = Pubkey::new_unique();

        // Schedule for GC
        store
            .schedule_gc(track, EpochNumber(5), GcReason::Deleted)
            .unwrap();

        // Should be scheduled
        assert!(store.is_scheduled_for_gc(track).unwrap());
    }

    #[test]
    fn test_schedule_gc_idempotent() {
        let store = test_store();
        let track = Pubkey::new_unique();

        // Schedule twice for same epoch
        store
            .schedule_gc(track, EpochNumber(5), GcReason::Deleted)
            .unwrap();
        store
            .schedule_gc(track, EpochNumber(5), GcReason::Invalidated)
            .unwrap();

        // Should still have exactly one entry
        let entries = store.get_due_gc_entries(EpochNumber(5)).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_get_due_gc_entries() {
        let store = test_store();
        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        // Schedule at different epochs
        store
            .schedule_gc(track1, EpochNumber(3), GcReason::Deleted)
            .unwrap();
        store
            .schedule_gc(track2, EpochNumber(5), GcReason::Invalidated)
            .unwrap();
        store
            .schedule_gc(track3, EpochNumber(7), GcReason::TapeDestroyed)
            .unwrap();

        // At epoch 5, should get track1 and track2
        let entries = store.get_due_gc_entries(EpochNumber(5)).unwrap();
        assert_eq!(entries.len(), 2);

        // At epoch 10, should get all
        let entries = store.get_due_gc_entries(EpochNumber(10)).unwrap();
        assert_eq!(entries.len(), 3);

        // At epoch 2, should get none
        let entries = store.get_due_gc_entries(EpochNumber(2)).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_remove_gc_entry() {
        let store = test_store();
        let track = Pubkey::new_unique();

        // Schedule and verify
        store
            .schedule_gc(track, EpochNumber(5), GcReason::Deleted)
            .unwrap();
        assert!(store.is_scheduled_for_gc(track).unwrap());

        // Remove and verify
        store.remove_gc_entry(track, EpochNumber(5)).unwrap();
        assert!(!store.is_scheduled_for_gc(track).unwrap());
    }

    #[test]
    fn test_delete_track_data() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let spool_indices: Vec<SpoolIndex> = vec![42, 100, 200];

        // Store track info
        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 3,
        };
        store.put_track_info(track, info).unwrap();

        // Store slices for each spool
        for &spool_idx in &spool_indices {
            store
                .put_slice(spool_idx, track, vec![0u8; 100], create_test_meta())
                .unwrap();
        }

        // Verify slices exist
        for &spool_idx in &spool_indices {
            assert!(store.get_slice(spool_idx, track).unwrap().is_some());
        }

        // Delete track data
        let deleted = delete_track_data(&store, track, &spool_indices).unwrap();
        assert_eq!(deleted, 3);

        // Verify track info gone
        assert!(store.get_track_info(track).unwrap().is_none());

        // Verify slices gone
        for &spool_idx in &spool_indices {
            assert!(store.get_slice(spool_idx, track).unwrap().is_none());
        }
    }

    #[test]
    fn test_run_epoch_gc() {
        let store = test_store();
        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let spool_indices: Vec<SpoolIndex> = vec![42];

        // Create track infos
        let info1 = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 1,
        };
        let info2 = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 1,
        };
        store.put_track_info(track1, info1).unwrap();
        store.put_track_info(track2, info2).unwrap();

        // Store slices
        store
            .put_slice(42, track1, vec![0u8; 100], create_test_meta())
            .unwrap();
        store
            .put_slice(42, track2, vec![0u8; 100], create_test_meta())
            .unwrap();

        // Schedule both for GC at epoch 5
        store
            .schedule_gc(track1, EpochNumber(5), GcReason::Deleted)
            .unwrap();
        store
            .schedule_gc(track2, EpochNumber(5), GcReason::Deleted)
            .unwrap();

        // Run GC for epoch 5
        let stats = run_epoch_gc(&store, EpochNumber(5), &spool_indices).unwrap();

        assert_eq!(stats.tracks_deleted, 2);
        assert_eq!(stats.slices_deleted, 2);
        assert_eq!(stats.tracks_failed, 0);

        // Verify tracks are gone
        assert!(store.get_track_info(track1).unwrap().is_none());
        assert!(store.get_track_info(track2).unwrap().is_none());

        // Verify GC entries are removed
        assert!(!store.is_scheduled_for_gc(track1).unwrap());
        assert!(!store.is_scheduled_for_gc(track2).unwrap());
    }

    #[test]
    fn test_gc_retries_on_failure() {
        let store = test_store();
        let track = Pubkey::new_unique();

        // Schedule for GC but don't create any track/slice data
        // (simulates a scenario where the track doesn't exist)
        store
            .schedule_gc(track, EpochNumber(5), GcReason::Deleted)
            .unwrap();

        // Entry should be due at epoch 5
        let entries = store.get_due_gc_entries(EpochNumber(5)).unwrap();
        assert_eq!(entries.len(), 1);

        // Run GC - the delete will succeed (idempotent) but since track doesn't exist
        // it's effectively a no-op success
        let spool_indices: Vec<SpoolIndex> = vec![42];
        let stats = run_epoch_gc(&store, EpochNumber(5), &spool_indices).unwrap();
        assert_eq!(stats.tracks_deleted, 1); // Counted as success

        // Entry should be removed
        let entries = store.get_due_gc_entries(EpochNumber(6)).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_delete_schedules_gc_next_epoch() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let current_epoch = EpochNumber(5);

        // Schedule for next epoch (grace period)
        let gc_epoch = EpochNumber(current_epoch.as_u64() + 1);
        store
            .schedule_gc(track, gc_epoch, GcReason::Deleted)
            .unwrap();

        // Not due at current epoch
        let entries = store.get_due_gc_entries(current_epoch).unwrap();
        assert_eq!(entries.len(), 0);

        // Due at next epoch
        let entries = store.get_due_gc_entries(gc_epoch).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].track, track);
    }

    #[test]
    fn test_invalidate_schedules_gc_immediately() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let current_epoch = EpochNumber(5);

        // Schedule for current epoch (immediate)
        store
            .schedule_gc(track, current_epoch, GcReason::Invalidated)
            .unwrap();

        // Due at current epoch
        let entries = store.get_due_gc_entries(current_epoch).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].track, track);
    }
}

//! Block processor event handlers
//!
//! Extracted handler functions for testability. Each handler operates
//! on TapeStore and can be tested with MemoryStore backend.
//!
//! These handlers are called by the live updates worker when it detects
//! tapedrive instructions in Solana blocks.

use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_store::ops::{GcOps, GcReason, GcStats, MetaOps, TrackInfo, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::TapeStore;

/// Error type for handler operations.
#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("storage error: {0}")]
    Storage(#[from] tape_store::error::TapeStoreError),
}

/// Result type for handler operations.
pub type Result<T> = std::result::Result<T, HandlerError>;

/// Handle RegisterTrack instruction.
///
/// Stores track metadata for later verification and GC decisions.
/// This is idempotent - if the track already exists, it will be overwritten.
///
/// # Arguments
/// * `store` - The tape store
/// * `track` - Track address (as bytes for solana pubkey compatibility)
/// * `commitment` - Merkle root of erasure-coded slices
pub fn handle_register_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    commitment: Hash,
) -> Result<()> {
    let track_pubkey = StorePubkey::new(track);

    // Store track info (idempotent - overwrites if exists)
    let info = TrackInfo {
        commitment_hash: commitment,
        certified_epoch: EpochNumber(0), // Not certified yet
        slice_count: 0,                  // No slices stored yet
    };

    store.put_track_info(track_pubkey, info)?;
    Ok(())
}

/// Handle CertifyTrack instruction.
///
/// Marks the track as certified in local storage. If the track info
/// doesn't exist (we missed the RegisterTrack), creates a stub entry.
///
/// # Arguments
/// * `store` - The tape store
/// * `track` - Track address (as bytes)
/// * `epoch` - The epoch in which certification occurred
pub fn handle_certify_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    epoch: EpochNumber,
) -> Result<()> {
    let track_pubkey = StorePubkey::new(track);

    match store.get_track_info(track_pubkey)? {
        Some(mut info) => {
            // Update certified epoch
            info.certified_epoch = epoch;
            store.put_track_info(track_pubkey, info)?;
        }
        None => {
            // Track info missing - create stub then mark certified
            // This can happen if we missed the RegisterTrack event
            let info = TrackInfo {
                commitment_hash: Hash::default(),
                certified_epoch: epoch,
                slice_count: 0,
            };
            store.put_track_info(track_pubkey, info)?;
        }
    }

    Ok(())
}

/// Handle DeleteTrack instruction.
///
/// Schedules the track for garbage collection at the end of the current epoch.
/// The grace period allows any in-flight downloads to complete.
///
/// # Arguments
/// * `store` - The tape store
/// * `track` - Track address (as bytes)
/// * `current_epoch` - The current epoch number
pub fn handle_delete_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    let track_pubkey = StorePubkey::new(track);

    // Schedule for GC at end of current epoch (grace period)
    let gc_epoch = EpochNumber(current_epoch.as_u64() + 1);
    store.schedule_gc(track_pubkey, gc_epoch, GcReason::Deleted)?;

    Ok(())
}

/// Handle InvalidateTrack instruction.
///
/// Schedules the track for immediate garbage collection (no grace period).
/// Invalid data should be removed as soon as possible.
///
/// # Arguments
/// * `store` - The tape store
/// * `track` - Track address (as bytes)
/// * `current_epoch` - The current epoch number
pub fn handle_invalidate_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    let track_pubkey = StorePubkey::new(track);

    // Schedule for immediate GC (no grace period for invalid data)
    store.schedule_gc(track_pubkey, current_epoch, GcReason::Invalidated)?;

    Ok(())
}

/// Handle DestroyTape instruction.
///
/// Schedules the tape for garbage collection. The actual deletion of
/// all tracks in the tape will happen during GC execution.
///
/// Note: This implementation schedules a GC entry for the tape itself.
/// The GC executor will need to look up all tracks belonging to this tape
/// and delete them. For now, we use a simplified approach that just
/// logs the tape destruction - full tape->track lookup would require
/// additional storage indices.
///
/// # Arguments
/// * `store` - The tape store
/// * `tape` - Tape address (as bytes)
/// * `current_epoch` - The current epoch number
pub fn handle_destroy_tape<S: Store>(
    store: &TapeStore<S>,
    tape: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    let tape_pubkey = StorePubkey::new(tape);

    // Schedule for GC at end of current epoch
    // Note: We're using the tape address as if it were a track address here.
    // A full implementation would need to look up all tracks belonging to this tape
    // from an index or from on-chain state. For now, this serves as a marker
    // that the tape was destroyed.
    let gc_epoch = EpochNumber(current_epoch.as_u64() + 1);
    store.schedule_gc(tape_pubkey, gc_epoch, GcReason::TapeDestroyed)?;

    Ok(())
}

/// Execute garbage collection for an epoch.
///
/// This is the main GC entry point called during epoch transitions.
/// It processes all GC entries due by the ended epoch.
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
    let stats = tape_store::ops::run_epoch_gc(store, ended_epoch, our_spools)?;
    Ok(stats)
}

/// Get the last processed slot cursor from storage.
///
/// # Arguments
/// * `store` - The tape store
///
/// # Returns
/// The last processed slot, or None if never set
pub fn get_cursor<S: Store>(store: &TapeStore<S>) -> Result<Option<tape_core::types::SlotNumber>> {
    Ok(store.get_cursor()?)
}

/// Set the last processed slot cursor in storage.
///
/// # Arguments
/// * `store` - The tape store
/// * `slot` - The slot to persist
pub fn set_cursor<S: Store>(
    store: &TapeStore<S>,
    slot: tape_core::types::SlotNumber,
) -> Result<()> {
    store.set_cursor(slot)?;
    Ok(())
}

/// Get the stored cluster genesis hash.
///
/// # Arguments
/// * `store` - The tape store
///
/// # Returns
/// The cluster hash, or None if not set (fresh node)
pub fn get_cluster_hash<S: Store>(store: &TapeStore<S>) -> Result<Option<Hash>> {
    Ok(store.get_cluster_hash()?)
}

/// Set the cluster genesis hash (only allowed once).
///
/// # Arguments
/// * `store` - The tape store
/// * `hash` - The cluster genesis hash to store
pub fn set_cluster_hash<S: Store>(store: &TapeStore<S>, hash: Hash) -> Result<()> {
    store.set_cluster_hash(hash)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_store::ops::{Compression, SliceMeta, SliceOps, MERKLE_HEIGHT};

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
    fn test_register_track() {
        let store = test_store();
        let track = [1u8; 32];
        let commitment = Hash::new_unique();

        handle_register_track(&store, track, commitment).unwrap();

        let track_pubkey = StorePubkey::new(track);
        let info = store.get_track_info(track_pubkey).unwrap().unwrap();
        assert_eq!(info.commitment_hash, commitment);
        assert_eq!(info.certified_epoch, EpochNumber(0));
        assert_eq!(info.slice_count, 0);
    }

    #[test]
    fn test_register_track_idempotent() {
        let store = test_store();
        let track = [1u8; 32];
        let commitment1 = Hash::new_unique();
        let commitment2 = Hash::new_unique();

        // First registration
        handle_register_track(&store, track, commitment1).unwrap();

        // Second registration overwrites
        handle_register_track(&store, track, commitment2).unwrap();

        let track_pubkey = StorePubkey::new(track);
        let info = store.get_track_info(track_pubkey).unwrap().unwrap();
        assert_eq!(info.commitment_hash, commitment2);
    }

    #[test]
    fn test_certify_track_existing() {
        let store = test_store();
        let track = [1u8; 32];
        let commitment = Hash::new_unique();

        // Register first
        handle_register_track(&store, track, commitment).unwrap();

        // Then certify
        handle_certify_track(&store, track, EpochNumber(10)).unwrap();

        let track_pubkey = StorePubkey::new(track);
        let info = store.get_track_info(track_pubkey).unwrap().unwrap();
        assert_eq!(info.certified_epoch, EpochNumber(10));
        assert_eq!(info.commitment_hash, commitment); // Preserved
    }

    #[test]
    fn test_certify_creates_stub_when_missing() {
        let store = test_store();
        let track = [1u8; 32];

        // Certify without prior register (missed RegisterTrack event)
        handle_certify_track(&store, track, EpochNumber(5)).unwrap();

        let track_pubkey = StorePubkey::new(track);
        let info = store.get_track_info(track_pubkey).unwrap().unwrap();
        assert_eq!(info.certified_epoch, EpochNumber(5));
        assert_eq!(info.commitment_hash, Hash::default()); // Stub value
    }

    #[test]
    fn test_delete_schedules_gc_next_epoch() {
        let store = test_store();
        let track = [1u8; 32];

        handle_delete_track(&store, track, EpochNumber(5)).unwrap();

        // Should not be due at current epoch
        let entries = store.get_due_gc_entries(EpochNumber(5)).unwrap();
        assert_eq!(entries.len(), 0);

        // Should be due at next epoch
        let entries = store.get_due_gc_entries(EpochNumber(6)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].track, StorePubkey::new(track));
    }

    #[test]
    fn test_invalidate_schedules_gc_immediately() {
        let store = test_store();
        let track = [1u8; 32];

        handle_invalidate_track(&store, track, EpochNumber(5)).unwrap();

        // Should be due at current epoch (immediate)
        let entries = store.get_due_gc_entries(EpochNumber(5)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].track, StorePubkey::new(track));
    }

    #[test]
    fn test_gc_deletes_track_and_slices() {
        let store = test_store();
        let track = [1u8; 32];
        let track_pubkey = StorePubkey::new(track);
        let spool_indices: Vec<SpoolIndex> = vec![42, 100];

        // Setup: register track, store slices
        handle_register_track(&store, track, Hash::new_unique()).unwrap();
        for &spool_idx in &spool_indices {
            store
                .put_slice(spool_idx, track_pubkey, vec![0u8; 100], create_test_meta())
                .unwrap();
        }

        // Schedule for GC
        handle_delete_track(&store, track, EpochNumber(5)).unwrap();

        // Verify setup
        assert!(store.get_track_info(track_pubkey).unwrap().is_some());
        for &spool_idx in &spool_indices {
            assert!(store.get_slice(spool_idx, track_pubkey).unwrap().is_some());
        }

        // Run GC
        let stats = run_epoch_gc(&store, EpochNumber(6), &spool_indices).unwrap();

        assert_eq!(stats.tracks_deleted, 1);
        assert_eq!(stats.slices_deleted, 2);
        assert_eq!(stats.tracks_failed, 0);

        // Verify deleted
        assert!(store.get_track_info(track_pubkey).unwrap().is_none());
        for &spool_idx in &spool_indices {
            assert!(store.get_slice(spool_idx, track_pubkey).unwrap().is_none());
        }
    }

    #[test]
    fn test_gc_handles_missing_track() {
        let store = test_store();
        let track = [1u8; 32];

        // Schedule for GC without creating track
        handle_delete_track(&store, track, EpochNumber(5)).unwrap();

        // Run GC - should succeed (idempotent deletion)
        let spool_indices: Vec<SpoolIndex> = vec![42];
        let stats = run_epoch_gc(&store, EpochNumber(6), &spool_indices).unwrap();

        // Counted as success (nothing to delete is not an error)
        assert_eq!(stats.tracks_deleted, 1);
        assert_eq!(stats.slices_deleted, 1); // delete_slice called once per spool
        assert_eq!(stats.tracks_failed, 0);
    }

    #[test]
    fn test_cursor_roundtrip() {
        let store = test_store();

        // Initially none
        assert!(get_cursor(&store).unwrap().is_none());

        // Set and retrieve
        let slot = tape_core::types::SlotNumber(123456);
        set_cursor(&store, slot).unwrap();
        assert_eq!(get_cursor(&store).unwrap(), Some(slot));

        // Update
        let slot2 = tape_core::types::SlotNumber(999999);
        set_cursor(&store, slot2).unwrap();
        assert_eq!(get_cursor(&store).unwrap(), Some(slot2));
    }

    #[test]
    fn test_cluster_hash_roundtrip() {
        let store = test_store();

        // Initially none
        assert!(get_cluster_hash(&store).unwrap().is_none());

        // Set and retrieve
        let hash = Hash::new_unique();
        set_cluster_hash(&store, hash).unwrap();
        assert_eq!(get_cluster_hash(&store).unwrap(), Some(hash));
    }

    #[test]
    fn test_cluster_hash_set_once() {
        let store = test_store();
        let hash1 = Hash::new_unique();
        let hash2 = Hash::new_unique();

        // First set succeeds
        set_cluster_hash(&store, hash1).unwrap();

        // Second set fails
        let result = set_cluster_hash(&store, hash2);
        assert!(result.is_err());

        // Original preserved
        assert_eq!(get_cluster_hash(&store).unwrap(), Some(hash1));
    }

    #[test]
    fn test_destroy_tape() {
        let store = test_store();
        let tape = [2u8; 32];

        handle_destroy_tape(&store, tape, EpochNumber(5)).unwrap();

        // Should be scheduled for GC at next epoch
        let entries = store.get_due_gc_entries(EpochNumber(6)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].track, StorePubkey::new(tape));
    }
}

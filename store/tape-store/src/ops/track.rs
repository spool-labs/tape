//! Track management operations
//!
//! Simplified operations for minimal track info storage.

use crate::columns::*;
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochNumber, Hash, Pubkey};
use crate::TapeStore;
use serde::{Deserialize, Serialize};
use store::Store;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Minimal track info stored by nodes
///
/// Nodes only need to know:
/// - The commitment hash to verify incoming slices
/// - Whether it's certified (for GC decisions)
/// - How many slices they've stored (for certification readiness)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackInfo {
    /// Merkle root of erasure-coded slices (for verification)
    pub commitment_hash: Hash,
    /// Epoch when certified (0 = not certified)
    pub certified_epoch: EpochNumber,
    /// Count of slices we have stored for this track
    pub slice_count: u16,
}

impl Default for TrackInfo {
    fn default() -> Self {
        Self {
            commitment_hash: Hash::default(),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        }
    }
}

/// High-level operations for track management
pub trait TrackOps {
    /// Store track info
    ///
    /// # Arguments
    /// * `address` - The on-chain address (Pubkey) of the track
    /// * `info` - The track info to store
    fn put_track_info(&self, address: Pubkey, info: TrackInfo) -> Result<()>;

    /// Get track info
    ///
    /// # Arguments
    /// * `address` - The on-chain address of the track
    ///
    /// # Returns
    /// Track info if found
    fn get_track_info(&self, address: Pubkey) -> Result<Option<TrackInfo>>;

    /// Increment slice count for a track
    ///
    /// Called when a new slice is stored for this track.
    /// If the track doesn't exist, creates it with slice_count = 1.
    ///
    /// # Arguments
    /// * `address` - The track address
    ///
    /// # Returns
    /// The new slice count
    fn increment_slice_count(&self, address: Pubkey) -> Result<u16>;

    /// Mark a track as certified
    ///
    /// # Arguments
    /// * `address` - The track address
    /// * `epoch` - The epoch in which it was certified
    fn mark_certified(&self, address: Pubkey, epoch: EpochNumber) -> Result<()>;

    /// Delete track info (for GC)
    ///
    /// Removes the track info from storage. This is called during garbage
    /// collection after all slices for the track have been deleted.
    ///
    /// # Arguments
    /// * `address` - The track address to delete
    ///
    /// # Returns
    /// Ok(true) if the track was deleted, Ok(false) if it didn't exist
    fn delete_track_info(&self, address: Pubkey) -> Result<bool>;
}

impl<S: Store> TrackOps for TapeStore<S> {
    fn put_track_info(&self, address: Pubkey, info: TrackInfo) -> Result<()> {
        self.put::<Tracks>(&address, &info)?;
        Ok(())
    }

    fn get_track_info(&self, address: Pubkey) -> Result<Option<TrackInfo>> {
        let info = self.get::<Tracks>(&address)?;
        Ok(info)
    }

    fn increment_slice_count(&self, address: Pubkey) -> Result<u16> {
        // Get existing track info or create new one
        let mut info = self.get::<Tracks>(&address)?.unwrap_or_default();

        // Increment slice count
        info.slice_count = info.slice_count.saturating_add(1);

        // Store updated info
        self.put::<Tracks>(&address, &info)?;

        Ok(info.slice_count)
    }

    fn mark_certified(&self, address: Pubkey, epoch: EpochNumber) -> Result<()> {
        // Get existing track info
        let mut info = match self.get::<Tracks>(&address)? {
            Some(i) => i,
            None => return Err(TapeStoreError::TrackNotFound(address)),
        };

        // Update certified epoch
        info.certified_epoch = epoch;

        // Store updated info
        self.put::<Tracks>(&address, &info)?;

        Ok(())
    }

    fn delete_track_info(&self, address: Pubkey) -> Result<bool> {
        // Check if track exists
        let exists = self.get::<Tracks>(&address)?.is_some();
        if !exists {
            return Ok(false);
        }

        // Delete the track info
        self.delete::<Tracks>(&address)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    #[test]
    fn put_and_get_track_info() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();
        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };

        store.put_track_info(address, info.clone()).unwrap();
        let retrieved = store.get_track_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn increment_slice_count() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        // First increment creates the track
        let count = store.increment_slice_count(address).unwrap();
        assert_eq!(count, 1);

        // Subsequent increments increase the count
        let count = store.increment_slice_count(address).unwrap();
        assert_eq!(count, 2);

        let count = store.increment_slice_count(address).unwrap();
        assert_eq!(count, 3);

        // Verify the stored value
        let info = store.get_track_info(address).unwrap().unwrap();
        assert_eq!(info.slice_count, 3);
    }

    #[test]
    fn mark_certified() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        // Create track first
        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 10,
        };
        store.put_track_info(address, info).unwrap();

        // Mark as certified
        store.mark_certified(address, EpochNumber(100)).unwrap();

        // Verify
        let info = store.get_track_info(address).unwrap().unwrap();
        assert_eq!(info.certified_epoch, EpochNumber(100));
        assert_eq!(info.slice_count, 10); // Other fields unchanged
    }

    #[test]
    fn mark_certified_not_found() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let result = store.mark_certified(address, EpochNumber(100));
        assert!(result.is_err());
    }

    #[test]
    fn track_not_found() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let result = store.get_track_info(address).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_track_info_existing() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();
        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(0),
            slice_count: 5,
        };

        // Create track
        store.put_track_info(address, info).unwrap();
        assert!(store.get_track_info(address).unwrap().is_some());

        // Delete returns true (existed)
        let result = store.delete_track_info(address).unwrap();
        assert!(result);

        // Track is gone
        assert!(store.get_track_info(address).unwrap().is_none());
    }

    #[test]
    fn delete_track_info_nonexistent() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        // Delete returns false (didn't exist)
        let result = store.delete_track_info(address).unwrap();
        assert!(!result);
    }
}

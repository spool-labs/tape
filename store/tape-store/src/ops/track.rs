//! TrackInfo operations for track metadata

use crate::columns::TrackCol;
use crate::error::Result;
use crate::types::{Pubkey, TrackInfo};
use crate::TapeStore;
use store::Store;

/// Operations for track info
pub trait TrackOps {
    /// Get track info by address
    fn get_track(&self, track_address: Pubkey) -> Result<Option<TrackInfo>>;

    /// Store track info
    fn put_track(&self, track_address: Pubkey, info: TrackInfo) -> Result<()>;

    /// Delete track info
    fn delete_track(&self, track_address: Pubkey) -> Result<()>;

    /// Check if track metadata exists without loading data
    fn has_track(&self, track_address: Pubkey) -> Result<bool>;
}

impl<S: Store> TrackOps for TapeStore<S> {
    fn get_track(&self, track_address: Pubkey) -> Result<Option<TrackInfo>> {
        Ok(self.get::<TrackCol>(&track_address)?)
    }

    fn put_track(&self, track_address: Pubkey, info: TrackInfo) -> Result<()> {
        self.put::<TrackCol>(&track_address, &info)?;
        Ok(())
    }

    fn delete_track(&self, track_address: Pubkey) -> Result<()> {
        self.delete::<TrackCol>(&track_address)?;
        Ok(())
    }

    fn has_track(&self, track_address: Pubkey) -> Result<bool> {
        Ok(self.contains::<TrackCol>(&track_address)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SpoolAllocation;
    use store_memory::MemoryStore;
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info() -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_allocation: SpoolAllocation::SpoolGroup(3),
            original_size: 1024 * 1024,
            encoding_type: 2, // Clay
            encoding_params: 0,
            commitment_hash: Hash::default(),
        }
    }

    #[test]
    fn test_track_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let info = make_track_info();

        assert!(store.get_track(track).unwrap().is_none());

        store.put_track(track, info.clone()).unwrap();

        let retrieved = store.get_track(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_delete() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let info = make_track_info();

        store.put_track(track, info).unwrap();
        assert!(store.get_track(track).unwrap().is_some());

        store.delete_track(track).unwrap();
        assert!(store.get_track(track).unwrap().is_none());
    }

    #[test]
    fn test_has_track() {
        let store = test_store();
        let track = Pubkey::new_unique();

        assert!(!store.has_track(track).unwrap());

        store.put_track(track, make_track_info()).unwrap();
        assert!(store.has_track(track).unwrap());

        store.delete_track(track).unwrap();
        assert!(!store.has_track(track).unwrap());
    }
}

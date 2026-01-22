//! SliceInfo operations for blob erasure coding metadata
//!
//! Provides storage for the hashes needed to verify slices for each track.

use crate::columns::SliceInfoCol;
use crate::error::Result;
use crate::types::{Pubkey, SliceInfo};
use crate::TapeStore;
use store::Store;

/// Operations for slice info (erasure coding metadata)
pub trait SliceInfoOps {
    /// Get slice info for a track
    fn get_slice_info(&self, track_address: Pubkey) -> Result<Option<SliceInfo>>;

    /// Store slice info for a track
    fn put_slice_info(&self, track_address: Pubkey, info: SliceInfo) -> Result<()>;

    /// Delete slice info for a track
    fn delete_slice_info(&self, track_address: Pubkey) -> Result<()>;

    /// Check if slice info exists for a track
    fn has_slice_info(&self, track_address: Pubkey) -> Result<bool>;
}

impl<S: Store> SliceInfoOps for TapeStore<S> {
    fn get_slice_info(&self, track_address: Pubkey) -> Result<Option<SliceInfo>> {
        Ok(self.get::<SliceInfoCol>(&track_address)?)
    }

    fn put_slice_info(&self, track_address: Pubkey, info: SliceInfo) -> Result<()> {
        self.put::<SliceInfoCol>(&track_address, &info)?;
        Ok(())
    }

    fn delete_slice_info(&self, track_address: Pubkey) -> Result<()> {
        self.delete::<SliceInfoCol>(&track_address)?;
        Ok(())
    }

    fn has_slice_info(&self, track_address: Pubkey) -> Result<bool> {
        Ok(self.contains::<SliceInfoCol>(&track_address)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EncodingType, Hash};
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_slice_info_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();

        let info = SliceInfo {
            encoding_type: EncodingType::Rotated,
            unencoded_length: 1024 * 1024,
            primary: vec![Hash::default(); 1024],
            recovery: vec![Hash::default(); 1024],
        };

        assert!(store.get_slice_info(track).unwrap().is_none());
        assert!(!store.has_slice_info(track).unwrap());

        store.put_slice_info(track, info.clone()).unwrap();

        assert!(store.has_slice_info(track).unwrap());
        let retrieved = store.get_slice_info(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_slice_info_delete() {
        let store = test_store();
        let track = Pubkey::new_unique();

        let info = SliceInfo {
            encoding_type: EncodingType::Basic,
            unencoded_length: 512,
            primary: vec![Hash::default(); 10],
            recovery: vec![],
        };

        store.put_slice_info(track, info).unwrap();
        assert!(store.has_slice_info(track).unwrap());

        store.delete_slice_info(track).unwrap();
        assert!(!store.has_slice_info(track).unwrap());
        assert!(store.get_slice_info(track).unwrap().is_none());
    }
}

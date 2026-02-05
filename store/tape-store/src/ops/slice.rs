//! Slice data operations (merged primary + recovery)

use crate::columns::SliceCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, SliceKey};
use crate::TapeStore;
use store::{Column, Store};

/// Operations for slice data storage
pub trait SliceOps {
    /// Get slice data
    fn get_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<Option<Vec<u8>>>;

    /// Store slice data
    fn put_slice(&self, spool_id: u16, track_address: Pubkey, data: Vec<u8>) -> Result<()>;

    /// Delete slice data
    fn delete_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()>;

    /// Iterate slices by spool
    fn iter_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>>;
}

impl<S: Store> SliceOps for TapeStore<S> {
    fn get_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<Option<Vec<u8>>> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.get::<SliceCol>(&key)?)
    }

    fn put_slice(&self, spool_id: u16, track_address: Pubkey, data: Vec<u8>) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<SliceCol>(&key, &data)?;
        Ok(())
    }

    fn delete_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<SliceCol>(&key)?;
        Ok(())
    }

    fn iter_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SliceCol::CF_NAME, &prefix)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            results.push((key.track_address, value_bytes.to_vec()));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_slice_roundtrip() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = vec![0xAB; 1024];

        assert!(store.get_slice(spool_id, track).unwrap().is_none());

        store
            .put_slice(spool_id, track, data.clone())
            .unwrap();

        let retrieved = store.get_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_delete_slice() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = vec![0u8; 100];

        store.put_slice(spool_id, track, data).unwrap();
        assert!(store.get_slice(spool_id, track).unwrap().is_some());

        store.delete_slice(spool_id, track).unwrap();
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[test]
    fn test_iter_slices_by_spool() {
        let store = test_store();
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        store
            .put_slice(spool_id, track1, vec![1])
            .unwrap();
        store
            .put_slice(spool_id, track2, vec![2])
            .unwrap();
        store
            .put_slice(spool_id, track3, vec![3])
            .unwrap();

        // Different spool
        store
            .put_slice(99, Pubkey::new_unique(), vec![99])
            .unwrap();

        let slices = store.iter_slices_by_spool(spool_id).unwrap();
        assert_eq!(slices.len(), 3);
    }
}

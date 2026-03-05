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

    /// Check if a slice exists without loading data
    fn has_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<bool>;

    /// Iterate slices by spool
    fn iter_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>>;

    /// Paginated slice iteration by spool. Returns up to `limit` slices
    /// starting after `after_track` (or from the beginning if None).
    fn iter_slices_by_spool_from(
        &self,
        spool_id: u16,
        after_track: Option<Pubkey>,
        limit: usize,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>>;

    /// Iterate slice keys (track addresses) by spool without loading data.
    fn iter_slice_keys_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<Vec<Pubkey>>;

    /// Count slices in a spool without loading data.
    fn count_slices_by_spool(&self, spool_id: u16) -> Result<usize>;

    /// Delete all slices for a spool. Returns count of deleted slices.
    fn delete_all_slices_for_spool(&self, spool_id: u16) -> Result<usize>;
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

    fn has_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SliceCol>(&key)?)
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
            let data: Vec<u8> = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;
            results.push((key.track_address, data));
        }
        Ok(results)
    }

    fn iter_slices_by_spool_from(
        &self,
        spool_id: u16,
        after_track: Option<Pubkey>,
        limit: usize,
    ) -> Result<Vec<(Pubkey, Vec<u8>)>> {
        let prefix = SliceKey::spool_prefix(spool_id);

        let start_key = match after_track {
            Some(track) => {
                let key = SliceKey::new(spool_id, track);
                wincode::serialize(&key)
                    .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?
            }
            None => prefix.to_vec(),
        };

        let iter = self
            .inner()
            .inner()
            .iter_from(SliceCol::CF_NAME, &start_key, store::Direction::Asc)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            // Stop when we leave the spool prefix
            if key_bytes.len() < 2 || key_bytes[..2] != prefix {
                break;
            }
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            // Skip the cursor key if resuming
            if after_track.is_some() && Some(key.track_address) == after_track {
                continue;
            }
            let data: Vec<u8> = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;
            results.push((key.track_address, data));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    fn iter_slice_keys_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<Vec<Pubkey>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SliceCol::CF_NAME, &prefix)?;

        let mut results = Vec::new();
        for (key_bytes, _value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            results.push(key.track_address);
        }
        Ok(results)
    }

    fn count_slices_by_spool(&self, spool_id: u16) -> Result<usize> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SliceCol::CF_NAME, &prefix)?;

        Ok(iter.count())
    }

    fn delete_all_slices_for_spool(&self, spool_id: u16) -> Result<usize> {
        let raw = self.inner().inner();
        let prefix = SliceKey::spool_prefix(spool_id);

        let keys: Vec<Vec<u8>> = raw
            .iter_prefix(SliceCol::CF_NAME, &prefix)?
            .map(|(k, _)| k)
            .collect();

        let count = keys.len();
        for key in keys {
            raw.delete(SliceCol::CF_NAME, &key)?;
        }

        Ok(count)
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

        // Verify data content matches what was stored
        for (track, data) in &slices {
            if *track == track1 { assert_eq!(data, &vec![1]); }
            else if *track == track2 { assert_eq!(data, &vec![2]); }
            else if *track == track3 { assert_eq!(data, &vec![3]); }
        }
    }

    #[test]
    fn test_has_slice() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        assert!(!store.has_slice(spool_id, track).unwrap());

        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();
        assert!(store.has_slice(spool_id, track).unwrap());

        store.delete_slice(spool_id, track).unwrap();
        assert!(!store.has_slice(spool_id, track).unwrap());
    }

    #[test]
    fn test_iter_slices_by_spool_from() {
        let store = test_store();
        let spool_id = 42;

        let mut tracks = Vec::new();
        for i in 0..5 {
            let track = Pubkey::new_unique();
            store.put_slice(spool_id, track, vec![i]).unwrap();
            tracks.push(track);
        }

        // Get all with limit
        let all = store.iter_slices_by_spool_from(spool_id, None, 10).unwrap();
        assert_eq!(all.len(), 5);

        // Verify data content survives iteration
        for (_, data) in &all {
            assert!(!data.is_empty());
            assert_eq!(data.len(), 1);
        }

        // Get first 2
        let first_two = store.iter_slices_by_spool_from(spool_id, None, 2).unwrap();
        assert_eq!(first_two.len(), 2);

        // Paginate: get next after the second
        let cursor = first_two[1].0;
        let next = store.iter_slices_by_spool_from(spool_id, Some(cursor), 10).unwrap();
        assert_eq!(next.len(), 3);

        // Different spool should be empty
        let empty = store.iter_slices_by_spool_from(99, None, 10).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_iter_slice_keys_by_spool() {
        let store = test_store();
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();

        store.put_slice(spool_id, track1, vec![1; 1024]).unwrap();
        store.put_slice(spool_id, track2, vec![2; 1024]).unwrap();
        store.put_slice(99, Pubkey::new_unique(), vec![3; 1024]).unwrap();

        let keys = store.iter_slice_keys_by_spool(spool_id).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn delete_all_for_spool() {
        let store = test_store();

        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let t3 = Pubkey::new_unique();

        store.put_slice(42, t1, vec![1]).unwrap();
        store.put_slice(42, t2, vec![2]).unwrap();
        store.put_slice(99, t3, vec![3]).unwrap();

        let count = store.delete_all_slices_for_spool(42).unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.count_slices_by_spool(42).unwrap(), 0);
        assert_eq!(store.count_slices_by_spool(99).unwrap(), 1);
    }

    #[test]
    fn test_count_slices_by_spool() {
        let store = test_store();
        let spool_id = 42;

        assert_eq!(store.count_slices_by_spool(spool_id).unwrap(), 0);

        for i in 0..5 {
            store.put_slice(spool_id, Pubkey::new_unique(), vec![i]).unwrap();
        }
        store.put_slice(99, Pubkey::new_unique(), vec![99]).unwrap();

        assert_eq!(store.count_slices_by_spool(spool_id).unwrap(), 5);
        assert_eq!(store.count_slices_by_spool(99).unwrap(), 1);
        assert_eq!(store.count_slices_by_spool(0).unwrap(), 0);
    }
}

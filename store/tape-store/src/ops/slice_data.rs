//! Slice data operations for primary and recovery slices
//!
//! Provides storage for erasure-coded slice data with atomic operations.

use crate::columns::{PrimarySlices, RecoverySlices};
use crate::error::{Result, TapeStoreError};
use crate::types::{PrimarySliceData, Pubkey, RecoverySliceData, SliceKey};
use crate::TapeStore;
use store::{Column, Store, WriteBatch};

/// Operations for slice data (primary and recovery)
pub trait SliceDataOps {
    // Primary slices
    fn get_primary_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
    ) -> Result<Option<PrimarySliceData>>;
    fn put_primary_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        data: PrimarySliceData,
    ) -> Result<()>;
    fn delete_primary_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()>;

    // Recovery slices
    fn get_recovery_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
    ) -> Result<Option<RecoverySliceData>>;
    fn put_recovery_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        data: RecoverySliceData,
    ) -> Result<()>;
    fn delete_recovery_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()>;

    // Iteration by spool
    fn iter_primary_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(Pubkey, PrimarySliceData)>>>;
    fn iter_recovery_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(Pubkey, RecoverySliceData)>>>;

    // Atomic operations for both slice types
    fn put_both_slices(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        primary: PrimarySliceData,
        recovery: RecoverySliceData,
    ) -> Result<()>;
    fn delete_both_slices(&self, spool_id: u16, track_address: Pubkey) -> Result<()>;
}

impl<S: Store> SliceDataOps for TapeStore<S> {
    fn get_primary_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
    ) -> Result<Option<PrimarySliceData>> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.get::<PrimarySlices>(&key)?)
    }

    fn put_primary_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        data: PrimarySliceData,
    ) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<PrimarySlices>(&key, &data)?;
        Ok(())
    }

    fn delete_primary_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<PrimarySlices>(&key)?;
        Ok(())
    }

    fn get_recovery_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
    ) -> Result<Option<RecoverySliceData>> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.get::<RecoverySlices>(&key)?)
    }

    fn put_recovery_slice(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        data: RecoverySliceData,
    ) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<RecoverySlices>(&key, &data)?;
        Ok(())
    }

    fn delete_recovery_slice(&self, spool_id: u16, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<RecoverySlices>(&key)?;
        Ok(())
    }

    fn iter_primary_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(Pubkey, PrimarySliceData)>>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(PrimarySlices::CF_NAME, &prefix)?;

        Ok(iter.filter_map(|(key_bytes, value_bytes)| {
            let key: SliceKey = match wincode::deserialize(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "slice key: {}",
                        e
                    ))))
                }
            };
            let data: PrimarySliceData = match wincode::deserialize(&value_bytes) {
                Ok(d) => d,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "primary slice data: {}",
                        e
                    ))))
                }
            };
            Some(Ok((key.track_address, data)))
        }))
    }

    fn iter_recovery_slices_by_spool(
        &self,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(Pubkey, RecoverySliceData)>>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(RecoverySlices::CF_NAME, &prefix)?;

        Ok(iter.filter_map(|(key_bytes, value_bytes)| {
            let key: SliceKey = match wincode::deserialize(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "slice key: {}",
                        e
                    ))))
                }
            };
            let data: RecoverySliceData = match wincode::deserialize(&value_bytes) {
                Ok(d) => d,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "recovery slice data: {}",
                        e
                    ))))
                }
            };
            Some(Ok((key.track_address, data)))
        }))
    }

    fn put_both_slices(
        &self,
        spool_id: u16,
        track_address: Pubkey,
        primary: PrimarySliceData,
        recovery: RecoverySliceData,
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        let key = SliceKey::new(spool_id, track_address);

        let key_bytes = wincode::serialize(&key)
            .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
        let primary_bytes = wincode::serialize(&primary)
            .map_err(|e| TapeStoreError::Serialization(format!("primary slice: {}", e)))?;
        let recovery_bytes = wincode::serialize(&recovery)
            .map_err(|e| TapeStoreError::Serialization(format!("recovery slice: {}", e)))?;

        batch.put(PrimarySlices::CF_NAME, &key_bytes, &primary_bytes);
        batch.put(RecoverySlices::CF_NAME, &key_bytes, &recovery_bytes);

        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn delete_both_slices(&self, spool_id: u16, track_address: Pubkey) -> Result<()> {
        let mut batch = WriteBatch::new();
        let key = SliceKey::new(spool_id, track_address);

        let key_bytes = wincode::serialize(&key)
            .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;

        batch.delete(PrimarySlices::CF_NAME, &key_bytes);
        batch.delete(RecoverySlices::CF_NAME, &key_bytes);

        self.inner().inner().write_batch(batch)?;
        Ok(())
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
    fn test_primary_slice_roundtrip() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = PrimarySliceData::new(vec![0xAB; 1024], 128);

        assert!(store.get_primary_slice(spool_id, track).unwrap().is_none());

        store.put_primary_slice(spool_id, track, data.clone()).unwrap();

        let retrieved = store.get_primary_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_recovery_slice_roundtrip() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = RecoverySliceData::new(vec![0xCD; 2048], 64);

        assert!(store.get_recovery_slice(spool_id, track).unwrap().is_none());

        store.put_recovery_slice(spool_id, track, data.clone()).unwrap();

        let retrieved = store.get_recovery_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_delete_primary_slice() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let data = PrimarySliceData::new(vec![0; 100], 0);

        store.put_primary_slice(spool_id, track, data).unwrap();
        assert!(store.get_primary_slice(spool_id, track).unwrap().is_some());

        store.delete_primary_slice(spool_id, track).unwrap();
        assert!(store.get_primary_slice(spool_id, track).unwrap().is_none());
    }

    #[test]
    fn test_iter_primary_slices_by_spool() {
        let store = test_store();
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        store
            .put_primary_slice(spool_id, track1, PrimarySliceData::new(vec![1], 0))
            .unwrap();
        store
            .put_primary_slice(spool_id, track2, PrimarySliceData::new(vec![2], 0))
            .unwrap();
        store
            .put_primary_slice(spool_id, track3, PrimarySliceData::new(vec![3], 0))
            .unwrap();

        // Different spool
        store
            .put_primary_slice(99, Pubkey::new_unique(), PrimarySliceData::new(vec![99], 0))
            .unwrap();

        let slices: Vec<(Pubkey, PrimarySliceData)> = store
            .iter_primary_slices_by_spool(spool_id)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(slices.len(), 3);
    }

    #[test]
    fn test_put_both_slices() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let primary = PrimarySliceData::new(vec![1; 100], 10);
        let recovery = RecoverySliceData::new(vec![2; 100], 20);

        store
            .put_both_slices(spool_id, track, primary.clone(), recovery.clone())
            .unwrap();

        let retrieved_primary = store.get_primary_slice(spool_id, track).unwrap().unwrap();
        let retrieved_recovery = store.get_recovery_slice(spool_id, track).unwrap().unwrap();

        assert_eq!(retrieved_primary, primary);
        assert_eq!(retrieved_recovery, recovery);
    }

    #[test]
    fn test_delete_both_slices() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let primary = PrimarySliceData::new(vec![1; 100], 10);
        let recovery = RecoverySliceData::new(vec![2; 100], 20);

        store
            .put_both_slices(spool_id, track, primary, recovery)
            .unwrap();

        assert!(store.get_primary_slice(spool_id, track).unwrap().is_some());
        assert!(store.get_recovery_slice(spool_id, track).unwrap().is_some());

        store.delete_both_slices(spool_id, track).unwrap();

        assert!(store.get_primary_slice(spool_id, track).unwrap().is_none());
        assert!(store.get_recovery_slice(spool_id, track).unwrap().is_none());
    }
}

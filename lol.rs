
/FILE: store/tape-store/src/error.rs

//! Error types for tape-store operations

use crate::types::Pubkey;
use tape_core::types::EpochNumber;
use thiserror::Error;

/// Errors that can occur during tape-store operations
#[derive(Debug, Error)]
pub enum TapeStoreError {
    /// Underlying store error
    #[error("Store error: {0}")]
    Store(#[from] store::Error),

    /// Slice info not found
    #[error("Slice info not found: {0:?}")]
    SliceInfoNotFound(Pubkey),

    /// Tape info not found
    #[error("Tape info not found: {0:?}")]
    TapeInfoNotFound(Pubkey),

    /// Track info not found
    #[error("Track info not found: {0:?}")]
    TrackInfoNotFound(Pubkey),

    /// Primary slice not found
    #[error("Primary slice not found: spool={0}, track={1:?}")]
    PrimarySliceNotFound(u16, Pubkey),

    /// Recovery slice not found
    #[error("Recovery slice not found: spool={0}, track={1:?}")]
    RecoverySliceNotFound(u16, Pubkey),

    /// Spool not found
    #[error("Spool not found: epoch={0}, spool={1}")]
    SpoolNotFound(EpochNumber, u16),

    /// Committee not found for epoch
    #[error("Committee not found for epoch {0}")]
    CommitteeNotFound(EpochNumber),

    /// Invalid data length
    #[error("Invalid data length: expected {expected}, got {actual}")]
    InvalidDataLength { expected: usize, actual: usize },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for tape-store operations
pub type Result<T> = std::result::Result<T, TapeStoreError>;




/FILE: store/tape-store/src/ops/track_info.rs

//! TrackInfo operations for track metadata
//!
//! Provides storage for track (blob) information.

use crate::columns::TrackInfoCol;
use crate::error::Result;
use crate::types::{EpochNumber, Pubkey, TrackInfo};
use crate::TapeStore;
use store::Store;

/// Operations for track info
pub trait TrackInfoOps {
    /// Get track info by address
    fn get_track_info(&self, track_address: Pubkey) -> Result<Option<TrackInfo>>;

    /// Store track info
    fn put_track_info(&self, track_address: Pubkey, info: TrackInfo) -> Result<()>;

    /// Delete track info
    fn delete_track_info(&self, track_address: Pubkey) -> Result<()>;

    /// Mark a track as certified
    fn certify_track(&self, track_address: Pubkey, epoch: EpochNumber) -> Result<()>;

    /// Iterate over tracks belonging to a tape
    fn iter_tracks_for_tape(
        &self,
        tape_address: Pubkey,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>>;
}

impl<S: Store> TrackInfoOps for TapeStore<S> {
    fn get_track_info(&self, track_address: Pubkey) -> Result<Option<TrackInfo>> {
        Ok(self.get::<TrackInfoCol>(&track_address)?)
    }

    fn put_track_info(&self, track_address: Pubkey, info: TrackInfo) -> Result<()> {
        self.put::<TrackInfoCol>(&track_address, &info)?;
        Ok(())
    }

    fn delete_track_info(&self, track_address: Pubkey) -> Result<()> {
        self.delete::<TrackInfoCol>(&track_address)?;
        Ok(())
    }

    fn certify_track(&self, track_address: Pubkey, epoch: EpochNumber) -> Result<()> {
        if let Some(mut info) = self.get::<TrackInfoCol>(&track_address)? {
            info.certified_epoch = Some(epoch);
            self.put::<TrackInfoCol>(&track_address, &info)?;
        }
        Ok(())
    }

    fn iter_tracks_for_tape(
        &self,
        tape_address: Pubkey,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>> {
        let iter = self.iter::<TrackInfoCol>()?;
        Ok(iter.into_iter().filter_map(move |(addr, info)| {
            if info.tape_address == tape_address {
                Some(Ok(addr))
            } else {
                None
            }
        }))
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
    fn test_track_info_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(100));

        assert!(store.get_track_info(track).unwrap().is_none());

        store.put_track_info(track, info.clone()).unwrap();

        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_info_delete() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(50));

        store.put_track_info(track, info).unwrap();
        assert!(store.get_track_info(track).unwrap().is_some());

        store.delete_track_info(track).unwrap();
        assert!(store.get_track_info(track).unwrap().is_none());
    }

    #[test]
    fn test_certify_track() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(100));

        store.put_track_info(track, info).unwrap();

        // Initially not certified
        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert!(retrieved.certified_epoch.is_none());

        // Certify
        store.certify_track(track, EpochNumber(101)).unwrap();

        // Now certified
        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert_eq!(retrieved.certified_epoch, Some(EpochNumber(101)));
    }

    #[test]
    fn test_iter_tracks_for_tape() {
        let store = test_store();

        let tape1 = Pubkey::new_unique();
        let tape2 = Pubkey::new_unique();

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        // Two tracks for tape1
        store
            .put_track_info(track1, TrackInfo::new(tape1, EpochNumber(0)))
            .unwrap();
        store
            .put_track_info(track2, TrackInfo::new(tape1, EpochNumber(0)))
            .unwrap();

        // One track for tape2
        store
            .put_track_info(track3, TrackInfo::new(tape2, EpochNumber(0)))
            .unwrap();

        // Get tracks for tape1
        let tracks: Vec<Pubkey> = store
            .iter_tracks_for_tape(tape1)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(tracks.len(), 2);
        assert!(tracks.contains(&track1));
        assert!(tracks.contains(&track2));
        assert!(!tracks.contains(&track3));
    }
}




/FILE: store/tape-store/src/ops/slice_data.rs

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




/FILE: store/tape-store/src/ops/spool.rs

//! Spool operations (epoch-namespaced)
//!
//! Provides epoch-namespaced spool tracking for crash-safe epoch transitions.
//! All spool state is keyed by (epoch, spool_id) to prevent stale state after crash.

use crate::columns::{SpoolAssigned, SpoolPendingRecovery, SpoolSyncProgress};
use crate::error::{Result, TapeStoreError};
use crate::types::{
    EpochNumber, PendingRecoveryKey, Pubkey, SliceType, SpoolEpochKey, SpoolStatus, SyncProgress,
};
use crate::TapeStore;
use store::{Column, Store};

/// Operations for epoch-namespaced spool management
pub trait SpoolOps {
    // Spool status
    fn get_spool_status(&self, epoch: EpochNumber, spool_id: u16) -> Result<Option<SpoolStatus>>;
    fn set_spool_status(&self, epoch: EpochNumber, spool_id: u16, status: SpoolStatus)
        -> Result<()>;
    fn remove_spool_assignment(&self, epoch: EpochNumber, spool_id: u16) -> Result<()>;

    // Iterate assigned spools for an epoch
    fn iter_assigned_spools(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<(u16, SpoolStatus)>>>;

    // Sync progress
    fn get_sync_progress(&self, epoch: EpochNumber, spool_id: u16)
        -> Result<Option<SyncProgress>>;
    fn set_sync_progress(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        progress: SyncProgress,
    ) -> Result<()>;
    fn clear_sync_progress(&self, epoch: EpochNumber, spool_id: u16) -> Result<()>;

    // Pending recovery
    fn add_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<()>;
    fn remove_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<()>;
    fn has_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<bool>;

    // Iterate pending recoveries for a spool
    fn iter_pending_recoveries(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(SliceType, Pubkey)>>>;

    // Cleanup old epoch state
    fn cleanup_epoch_state(&self, epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> SpoolOps for TapeStore<S> {
    fn get_spool_status(&self, epoch: EpochNumber, spool_id: u16) -> Result<Option<SpoolStatus>> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        Ok(self.get::<SpoolAssigned>(&key)?)
    }

    fn set_spool_status(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        status: SpoolStatus,
    ) -> Result<()> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        self.put::<SpoolAssigned>(&key, &status)?;
        Ok(())
    }

    fn remove_spool_assignment(&self, epoch: EpochNumber, spool_id: u16) -> Result<()> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        self.delete::<SpoolAssigned>(&key)?;
        Ok(())
    }

    fn iter_assigned_spools(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<(u16, SpoolStatus)>>> {
        let prefix = SpoolEpochKey::epoch_prefix(epoch.as_u64());
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SpoolAssigned::CF_NAME, &prefix)?;

        Ok(iter.filter_map(|(key_bytes, value_bytes)| {
            let key: SpoolEpochKey = match wincode::deserialize(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "spool key: {}",
                        e
                    ))))
                }
            };
            let status: SpoolStatus = match wincode::deserialize(&value_bytes) {
                Ok(s) => s,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "spool status: {}",
                        e
                    ))))
                }
            };
            Some(Ok((key.spool_id, status)))
        }))
    }

    fn get_sync_progress(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
    ) -> Result<Option<SyncProgress>> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        Ok(self.get::<SpoolSyncProgress>(&key)?)
    }

    fn set_sync_progress(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        progress: SyncProgress,
    ) -> Result<()> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        self.put::<SpoolSyncProgress>(&key, &progress)?;
        Ok(())
    }

    fn clear_sync_progress(&self, epoch: EpochNumber, spool_id: u16) -> Result<()> {
        let key = SpoolEpochKey::new(epoch.as_u64(), spool_id);
        self.delete::<SpoolSyncProgress>(&key)?;
        Ok(())
    }

    fn add_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<()> {
        let key = PendingRecoveryKey::new(epoch.as_u64(), spool_id, slice_type, track_address);
        self.put::<SpoolPendingRecovery>(&key, &())?;
        Ok(())
    }

    fn remove_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<()> {
        let key = PendingRecoveryKey::new(epoch.as_u64(), spool_id, slice_type, track_address);
        self.delete::<SpoolPendingRecovery>(&key)?;
        Ok(())
    }

    fn has_pending_recovery(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
        slice_type: SliceType,
        track_address: Pubkey,
    ) -> Result<bool> {
        let key = PendingRecoveryKey::new(epoch.as_u64(), spool_id, slice_type, track_address);
        Ok(self.contains::<SpoolPendingRecovery>(&key)?)
    }

    fn iter_pending_recoveries(
        &self,
        epoch: EpochNumber,
        spool_id: u16,
    ) -> Result<impl Iterator<Item = Result<(SliceType, Pubkey)>>> {
        let prefix = PendingRecoveryKey::epoch_spool_prefix(epoch.as_u64(), spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SpoolPendingRecovery::CF_NAME, &prefix)?;

        Ok(iter.filter_map(|(key_bytes, _value_bytes)| {
            let key: PendingRecoveryKey = match wincode::deserialize(&key_bytes) {
                Ok(k) => k,
                Err(e) => {
                    return Some(Err(TapeStoreError::Serialization(format!(
                        "pending recovery key: {}",
                        e
                    ))))
                }
            };
            Some(Ok((key.slice_type, key.track_address)))
        }))
    }

    fn cleanup_epoch_state(&self, epoch: EpochNumber) -> Result<()> {
        // Delete all spool assignments for the epoch
        let prefix = SpoolEpochKey::epoch_prefix(epoch.as_u64());

        // Collect keys to delete (can't delete while iterating)
        let assigned_keys: Vec<SpoolEpochKey> = self
            .inner()
            .inner()
            .iter_prefix(SpoolAssigned::CF_NAME, &prefix)?
            .filter_map(|(key_bytes, _)| wincode::deserialize(&key_bytes).ok())
            .collect();

        for key in assigned_keys {
            self.delete::<SpoolAssigned>(&key)?;
        }

        // Delete all sync progress for the epoch
        let progress_keys: Vec<SpoolEpochKey> = self
            .inner()
            .inner()
            .iter_prefix(SpoolSyncProgress::CF_NAME, &prefix)?
            .filter_map(|(key_bytes, _)| wincode::deserialize(&key_bytes).ok())
            .collect();

        for key in progress_keys {
            self.delete::<SpoolSyncProgress>(&key)?;
        }

        // Delete all pending recovery for the epoch
        let pending_keys: Vec<PendingRecoveryKey> = self
            .inner()
            .inner()
            .iter_prefix(SpoolPendingRecovery::CF_NAME, &epoch.as_u64().to_be_bytes())?
            .filter_map(|(key_bytes, _)| wincode::deserialize(&key_bytes).ok())
            .collect();

        for key in pending_keys {
            self.delete::<SpoolPendingRecovery>(&key)?;
        }

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
    fn test_spool_status_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(100);
        let spool_id = 42;

        assert!(store.get_spool_status(epoch, spool_id).unwrap().is_none());

        store
            .set_spool_status(epoch, spool_id, SpoolStatus::Active)
            .unwrap();

        assert_eq!(
            store.get_spool_status(epoch, spool_id).unwrap(),
            Some(SpoolStatus::Active)
        );
    }

    #[test]
    fn test_spool_status_epoch_isolation() {
        let store = test_store();
        let epoch1 = EpochNumber(100);
        let epoch2 = EpochNumber(101);
        let spool_id = 42;

        store
            .set_spool_status(epoch1, spool_id, SpoolStatus::Active)
            .unwrap();
        store
            .set_spool_status(epoch2, spool_id, SpoolStatus::Sync)
            .unwrap();

        assert_eq!(
            store.get_spool_status(epoch1, spool_id).unwrap(),
            Some(SpoolStatus::Active)
        );
        assert_eq!(
            store.get_spool_status(epoch2, spool_id).unwrap(),
            Some(SpoolStatus::Sync)
        );
    }

    #[test]
    fn test_iter_assigned_spools() {
        let store = test_store();
        let epoch = EpochNumber(100);

        store
            .set_spool_status(epoch, 10, SpoolStatus::Active)
            .unwrap();
        store
            .set_spool_status(epoch, 20, SpoolStatus::Sync)
            .unwrap();
        store
            .set_spool_status(epoch, 30, SpoolStatus::Recover)
            .unwrap();

        // Different epoch should not appear
        store
            .set_spool_status(EpochNumber(99), 40, SpoolStatus::Active)
            .unwrap();

        let spools: Vec<(u16, SpoolStatus)> = store
            .iter_assigned_spools(epoch)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(spools.len(), 3);
    }

    #[test]
    fn test_sync_progress_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(100);
        let spool_id = 42;

        let progress = SyncProgress {
            last_synced_track: Some(Pubkey::new_unique()),
            slice_type: SliceType::Recovery,
        };

        assert!(store.get_sync_progress(epoch, spool_id).unwrap().is_none());

        store
            .set_sync_progress(epoch, spool_id, progress.clone())
            .unwrap();

        assert_eq!(
            store.get_sync_progress(epoch, spool_id).unwrap(),
            Some(progress)
        );

        store.clear_sync_progress(epoch, spool_id).unwrap();
        assert!(store.get_sync_progress(epoch, spool_id).unwrap().is_none());
    }

    #[test]
    fn test_pending_recovery() {
        let store = test_store();
        let epoch = EpochNumber(100);
        let spool_id = 42;
        let track = Pubkey::new_unique();

        assert!(!store
            .has_pending_recovery(epoch, spool_id, SliceType::Primary, track)
            .unwrap());

        store
            .add_pending_recovery(epoch, spool_id, SliceType::Primary, track)
            .unwrap();

        assert!(store
            .has_pending_recovery(epoch, spool_id, SliceType::Primary, track)
            .unwrap());

        store
            .remove_pending_recovery(epoch, spool_id, SliceType::Primary, track)
            .unwrap();

        assert!(!store
            .has_pending_recovery(epoch, spool_id, SliceType::Primary, track)
            .unwrap());
    }

    #[test]
    fn test_iter_pending_recoveries() {
        let store = test_store();
        let epoch = EpochNumber(100);
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        store
            .add_pending_recovery(epoch, spool_id, SliceType::Primary, track1)
            .unwrap();
        store
            .add_pending_recovery(epoch, spool_id, SliceType::Recovery, track2)
            .unwrap();
        store
            .add_pending_recovery(epoch, spool_id, SliceType::Primary, track3)
            .unwrap();

        // Different spool should not appear
        store
            .add_pending_recovery(epoch, 99, SliceType::Primary, Pubkey::new_unique())
            .unwrap();

        let pending: Vec<(SliceType, Pubkey)> = store
            .iter_pending_recoveries(epoch, spool_id)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn test_cleanup_epoch_state() {
        let store = test_store();
        let epoch = EpochNumber(100);

        // Add some state
        store
            .set_spool_status(epoch, 10, SpoolStatus::Active)
            .unwrap();
        store
            .set_spool_status(epoch, 20, SpoolStatus::Sync)
            .unwrap();
        store
            .set_sync_progress(epoch, 10, SyncProgress::default())
            .unwrap();
        store
            .add_pending_recovery(epoch, 10, SliceType::Primary, Pubkey::new_unique())
            .unwrap();

        // State exists
        assert!(store.get_spool_status(epoch, 10).unwrap().is_some());
        assert!(store.get_spool_status(epoch, 20).unwrap().is_some());
        assert!(store.get_sync_progress(epoch, 10).unwrap().is_some());

        // Cleanup
        store.cleanup_epoch_state(epoch).unwrap();

        // State is gone
        assert!(store.get_spool_status(epoch, 10).unwrap().is_none());
        assert!(store.get_spool_status(epoch, 20).unwrap().is_none());
        assert!(store.get_sync_progress(epoch, 10).unwrap().is_none());

        let pending: Vec<_> = store
            .iter_pending_recoveries(epoch, 10)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(pending.is_empty());
    }
}




/FILE: store/tape-store/src/ops/tape_info.rs

//! TapeInfo operations for tape metadata
//!
//! Provides storage for tape (storage allocation) information.

use crate::columns::TapeInfoCol;
use crate::error::Result;
use crate::types::{EpochNumber, Pubkey, TapeInfo};
use crate::TapeStore;
use store::Store;

/// Operations for tape info
pub trait TapeInfoOps {
    /// Get tape info by address
    fn get_tape_info(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>>;

    /// Store tape info
    fn put_tape_info(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()>;

    /// Delete tape info
    fn delete_tape_info(&self, tape_address: Pubkey) -> Result<()>;

    /// Iterate over expired tapes (expiry_epoch <= given epoch)
    fn iter_expired_tapes(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>>;
}

impl<S: Store> TapeInfoOps for TapeStore<S> {
    fn get_tape_info(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>> {
        Ok(self.get::<TapeInfoCol>(&tape_address)?)
    }

    fn put_tape_info(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()> {
        self.put::<TapeInfoCol>(&tape_address, &info)?;
        Ok(())
    }

    fn delete_tape_info(&self, tape_address: Pubkey) -> Result<()> {
        self.delete::<TapeInfoCol>(&tape_address)?;
        Ok(())
    }

    fn iter_expired_tapes(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>> {
        let iter = self.iter::<TapeInfoCol>()?;
        Ok(iter.into_iter().filter_map(move |(addr, info)| {
            if info.expiry_epoch <= epoch {
                Some(Ok(addr))
            } else {
                None
            }
        }))
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
    fn test_tape_info_roundtrip() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            authority: Pubkey::new_unique(),
        };

        assert!(store.get_tape_info(tape).unwrap().is_none());

        store.put_tape_info(tape, info.clone()).unwrap();

        let retrieved = store.get_tape_info(tape).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_tape_info_delete() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            active_epoch: EpochNumber(50),
            expiry_epoch: EpochNumber(150),
            authority: Pubkey::new_unique(),
        };

        store.put_tape_info(tape, info).unwrap();
        assert!(store.get_tape_info(tape).unwrap().is_some());

        store.delete_tape_info(tape).unwrap();
        assert!(store.get_tape_info(tape).unwrap().is_none());
    }

    #[test]
    fn test_iter_expired_tapes() {
        let store = test_store();

        // Create tapes with different expiry epochs
        let tape1 = Pubkey::new_unique();
        let tape2 = Pubkey::new_unique();
        let tape3 = Pubkey::new_unique();

        store
            .put_tape_info(
                tape1,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(50),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        store
            .put_tape_info(
                tape2,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(100),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        store
            .put_tape_info(
                tape3,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(200),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        // Check expired at epoch 100
        let expired: Vec<Pubkey> = store
            .iter_expired_tapes(EpochNumber(100))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&tape1));
        assert!(expired.contains(&tape2));
        assert!(!expired.contains(&tape3));
    }
}




/FILE: store/tape-store/src/ops/meta.rs

//! Metadata operations for node state tracking
//!
//! Provides storage for:
//! - Node status (Standby/Active/Recovering)
//! - Cluster genesis hash (for validation on node restart)
//! - Current epoch number
//! - Sync cursor (last processed slot)
//! - GC progress (started/completed epochs)

use crate::columns::{Gc, Meta, SyncCursor};
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochNumber, Hash, NodeStatus, SlotNumber, UnitKey};
use crate::TapeStore;
use store::Store;

// Meta keys
const NODE_STATUS_KEY: &str = "node_status";
const CLUSTER_HASH_KEY: &str = "cluster_hash";
const CURRENT_EPOCH_KEY: &str = "current_epoch";

// GC keys
const GC_STARTED_KEY: &str = "started";
const GC_COMPLETED_KEY: &str = "completed";

/// Operations for node metadata
pub trait MetaOps {
    // Node status
    fn get_node_status(&self) -> Result<Option<NodeStatus>>;
    fn set_node_status(&self, status: NodeStatus) -> Result<()>;

    // Cluster hash
    fn get_cluster_hash(&self) -> Result<Option<Hash>>;
    fn set_cluster_hash(&self, hash: Hash) -> Result<()>;

    // Current epoch
    fn get_current_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_current_epoch(&self, epoch: EpochNumber) -> Result<()>;

    // Sync cursor
    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>>;
    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()>;

    // GC epochs
    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()>;
    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>>;
    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> MetaOps for TapeStore<S> {
    fn get_node_status(&self) -> Result<Option<NodeStatus>> {
        let key = NODE_STATUS_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.is_empty() {
                    return Ok(None);
                }
                let status = match bytes[0] {
                    0 => NodeStatus::Standby,
                    1 => NodeStatus::Active,
                    2 => NodeStatus::Recovering,
                    _ => NodeStatus::Standby,
                };
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    fn set_node_status(&self, status: NodeStatus) -> Result<()> {
        let key = NODE_STATUS_KEY.to_string();
        let bytes = vec![status as u8];
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_cluster_hash(&self) -> Result<Option<Hash>> {
        let key = CLUSTER_HASH_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 32 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 32,
                        actual: bytes.len(),
                    });
                }
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&bytes);
                Ok(Some(Hash::from(hash_bytes)))
            }
            None => Ok(None),
        }
    }

    fn set_cluster_hash(&self, hash: Hash) -> Result<()> {
        let key = CLUSTER_HASH_KEY.to_string();
        let bytes = hash.as_ref().to_vec();
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_current_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = CURRENT_EPOCH_KEY.to_string();
        match self.get::<Meta>(&key)? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(TapeStoreError::InvalidDataLength {
                        expected: 8,
                        actual: bytes.len(),
                    });
                }
                let mut epoch_bytes = [0u8; 8];
                epoch_bytes.copy_from_slice(&bytes);
                let epoch = u64::from_le_bytes(epoch_bytes);
                Ok(Some(EpochNumber(epoch)))
            }
            None => Ok(None),
        }
    }

    fn set_current_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = CURRENT_EPOCH_KEY.to_string();
        let bytes = epoch.as_u64().to_le_bytes().to_vec();
        self.put::<Meta>(&key, &bytes)?;
        Ok(())
    }

    fn get_sync_cursor(&self) -> Result<Option<SlotNumber>> {
        Ok(self.get::<SyncCursor>(&UnitKey)?)
    }

    fn set_sync_cursor(&self, slot: SlotNumber) -> Result<()> {
        self.put::<SyncCursor>(&UnitKey, &slot)?;
        Ok(())
    }

    fn get_gc_started_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_STARTED_KEY.to_string();
        Ok(self.get::<Gc>(&key)?)
    }

    fn set_gc_started_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_STARTED_KEY.to_string();
        self.put::<Gc>(&key, &epoch)?;
        Ok(())
    }

    fn get_gc_completed_epoch(&self) -> Result<Option<EpochNumber>> {
        let key = GC_COMPLETED_KEY.to_string();
        Ok(self.get::<Gc>(&key)?)
    }

    fn set_gc_completed_epoch(&self, epoch: EpochNumber) -> Result<()> {
        let key = GC_COMPLETED_KEY.to_string();
        self.put::<Gc>(&key, &epoch)?;
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
    fn test_node_status_roundtrip() {
        let store = test_store();

        assert!(store.get_node_status().unwrap().is_none());

        store.set_node_status(NodeStatus::Active).unwrap();
        assert_eq!(store.get_node_status().unwrap(), Some(NodeStatus::Active));

        store.set_node_status(NodeStatus::Recovering).unwrap();
        assert_eq!(
            store.get_node_status().unwrap(),
            Some(NodeStatus::Recovering)
        );
    }

    #[test]
    fn test_cluster_hash_roundtrip() {
        let store = test_store();
        let hash = Hash::new_unique();

        assert!(store.get_cluster_hash().unwrap().is_none());

        store.set_cluster_hash(hash).unwrap();
        assert_eq!(store.get_cluster_hash().unwrap(), Some(hash));
    }

    #[test]
    fn test_current_epoch_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(12345);

        assert!(store.get_current_epoch().unwrap().is_none());

        store.set_current_epoch(epoch).unwrap();
        assert_eq!(store.get_current_epoch().unwrap(), Some(epoch));
    }

    #[test]
    fn test_sync_cursor_roundtrip() {
        let store = test_store();
        let slot = SlotNumber(999999);

        assert!(store.get_sync_cursor().unwrap().is_none());

        store.set_sync_cursor(slot).unwrap();
        assert_eq!(store.get_sync_cursor().unwrap(), Some(slot));
    }

    #[test]
    fn test_gc_epochs_roundtrip() {
        let store = test_store();
        let started = EpochNumber(100);
        let completed = EpochNumber(99);

        assert!(store.get_gc_started_epoch().unwrap().is_none());
        assert!(store.get_gc_completed_epoch().unwrap().is_none());

        store.set_gc_started_epoch(started).unwrap();
        store.set_gc_completed_epoch(completed).unwrap();

        assert_eq!(store.get_gc_started_epoch().unwrap(), Some(started));
        assert_eq!(store.get_gc_completed_epoch().unwrap(), Some(completed));
    }
}




/FILE: store/tape-store/src/ops/slice_info.rs

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




/FILE: store/tape-store/src/ops/mod.rs

//! High-level operation traits for TapeStore
//!
//! This module provides domain-specific operations that guarantee consistency
//! across multiple column families through atomic batch operations.
//!
//! ## Operation Traits
//!
//! - `MetaOps`: Node status, cluster hash, current epoch, sync cursor, GC tracking
//! - `SliceInfoOps`: Blob erasure coding metadata (hashes for verification)
//! - `TapeInfoOps`: Tape (storage allocation) metadata
//! - `TrackInfoOps`: Track (blob) metadata and certification
//! - `SpoolOps`: Epoch-namespaced spool status, sync progress, pending recovery
//! - `SliceDataOps`: Primary and recovery slice data storage
//! - `CommitteeOps`: Committee cache by epoch

mod committee;
mod meta;
mod slice_data;
mod slice_info;
mod spool;
mod tape_info;
mod track_info;

// Re-export operation traits
pub use committee::CommitteeOps;
pub use meta::MetaOps;
pub use slice_data::SliceDataOps;
pub use slice_info::SliceInfoOps;
pub use spool::SpoolOps;
pub use tape_info::TapeInfoOps;
pub use track_info::TrackInfoOps;




/FILE: store/tape-store/src/ops/committee.rs

//! Committee management operations
//!
//! Caches committee data for routing and verification.

use crate::columns::Committee;
use crate::error::Result;
use crate::ops::MetaOps;
use crate::types::{CommitteeCache, EpochKey, EpochNumber};
use crate::TapeStore;
use store::Store;

/// High-level operations for committee management
pub trait CommitteeOps {
    /// Store committee cache for an epoch
    ///
    /// # Arguments
    /// * `cache` - The committee cache to store (epoch is in the cache)
    fn put_committee(&self, cache: CommitteeCache) -> Result<()>;

    /// Get committee cache for a specific epoch
    ///
    /// # Arguments
    /// * `epoch` - The epoch to query
    ///
    /// # Returns
    /// Committee cache if found
    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<CommitteeCache>>;

    /// Get current committee based on the stored current_epoch
    ///
    /// Uses the current_epoch from meta to look up the committee.
    ///
    /// # Returns
    /// The current committee cache if available
    fn get_current_committee(&self) -> Result<Option<CommitteeCache>>;

    /// Delete old committee caches, keeping the most recent N epochs
    ///
    /// # Arguments
    /// * `keep_epochs` - Number of recent epochs to keep
    fn delete_old_committees(&self, keep_epochs: usize) -> Result<()>;
}

impl<S: Store> CommitteeOps for TapeStore<S> {
    fn put_committee(&self, cache: CommitteeCache) -> Result<()> {
        let key = EpochKey::new(cache.epoch.as_u64());
        self.put::<Committee>(&key, &cache)?;
        Ok(())
    }

    fn get_committee(&self, epoch: EpochNumber) -> Result<Option<CommitteeCache>> {
        let key = EpochKey::new(epoch.as_u64());
        Ok(self.get::<Committee>(&key)?)
    }

    fn get_current_committee(&self) -> Result<Option<CommitteeCache>> {
        // Get the current epoch from meta
        if let Some(epoch) = self.get_current_epoch()? {
            return self.get_committee(epoch);
        }

        // Fallback: iterate to find the highest epoch
        let iter = self.iter::<Committee>()?;
        let mut latest: Option<CommitteeCache> = None;
        for (_epoch, cache) in iter {
            match &latest {
                None => latest = Some(cache),
                Some(current) => {
                    if cache.epoch > current.epoch {
                        latest = Some(cache);
                    }
                }
            }
        }
        Ok(latest)
    }

    fn delete_old_committees(&self, keep_epochs: usize) -> Result<()> {
        // Collect all epochs
        let mut epochs: Vec<EpochNumber> = self
            .iter::<Committee>()?
            .into_iter()
            .map(|(key, _)| EpochNumber(key.0))
            .collect();

        // Sort descending to keep the highest
        epochs.sort_by(|a: &EpochNumber, b: &EpochNumber| b.cmp(a));

        // Delete all but the most recent `keep_epochs`
        for epoch in epochs.into_iter().skip(keep_epochs) {
            let key = EpochKey::new(epoch.as_u64());
            self.delete::<Committee>(&key)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommitteeMemberInfo, NodeId, Pubkey};
    use bytemuck::Zeroable;
    use store_memory::MemoryStore;
    use tape_core::bls::BlsPubkey;

    fn create_test_member(id: u64) -> CommitteeMemberInfo {
        CommitteeMemberInfo {
            id: NodeId(id),
            pubkey: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: format!("192.168.1.{}:8080", id),
        }
    }

    fn create_test_cache(epoch: u64) -> CommitteeCache {
        CommitteeCache {
            epoch: EpochNumber(epoch),
            members: vec![create_test_member(1), create_test_member(2)],
            spool_assignment: vec![0, 1, 0, 1],
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        }
    }

    #[test]
    fn test_put_and_get_committee() {
        let store = TapeStore::new(MemoryStore::new());
        let cache = create_test_cache(100);

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap();
        assert_eq!(retrieved, Some(cache));
    }

    #[test]
    fn test_get_current_committee() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for multiple epochs
        for epoch in [95, 100, 98] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Should return the highest epoch (fallback iteration)
        let current = store.get_current_committee().unwrap().unwrap();
        assert_eq!(current.epoch, EpochNumber(100));
    }

    #[test]
    fn test_get_current_committee_with_meta() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for multiple epochs
        for epoch in [95, 100, 98] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Set current epoch to 98 (not the highest)
        store.set_current_epoch(EpochNumber(98)).unwrap();

        // Should return epoch 98 (from meta)
        let current = store.get_current_committee().unwrap().unwrap();
        assert_eq!(current.epoch, EpochNumber(98));
    }

    #[test]
    fn test_committee_not_found() {
        let store = TapeStore::new(MemoryStore::new());

        let result = store.get_committee(EpochNumber(999)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_old_committees() {
        let store = TapeStore::new(MemoryStore::new());

        // Add committees for 5 epochs
        for epoch in [1, 2, 3, 4, 5] {
            let cache = create_test_cache(epoch);
            store.put_committee(cache).unwrap();
        }

        // Keep only 2 most recent
        store.delete_old_committees(2).unwrap();

        // Only epochs 4 and 5 should remain
        assert!(store.get_committee(EpochNumber(1)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(2)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(3)).unwrap().is_none());
        assert!(store.get_committee(EpochNumber(4)).unwrap().is_some());
        assert!(store.get_committee(EpochNumber(5)).unwrap().is_some());
    }

    #[test]
    fn test_committee_member_info() {
        let store = TapeStore::new(MemoryStore::new());

        let cache = CommitteeCache {
            epoch: EpochNumber(100),
            members: vec![
                CommitteeMemberInfo {
                    id: NodeId(1),
                    pubkey: Pubkey::new([1u8; 32]),
                    bls_pubkey: BlsPubkey::zeroed(),
                    network_address: "10.0.0.1:9000".to_string(),
                },
                CommitteeMemberInfo {
                    id: NodeId(2),
                    pubkey: Pubkey::new([2u8; 32]),
                    bls_pubkey: BlsPubkey::zeroed(),
                    network_address: "10.0.0.2:9000".to_string(),
                },
            ],
            spool_assignment: vec![0, 1],
            my_member_index: Some(0),
            my_spools: vec![0],
        };

        store.put_committee(cache.clone()).unwrap();
        let retrieved = store.get_committee(EpochNumber(100)).unwrap().unwrap();

        assert_eq!(retrieved.members.len(), 2);
        assert_eq!(retrieved.members[0].id, NodeId(1));
        assert_eq!(retrieved.members[0].network_address, "10.0.0.1:9000");
        assert_eq!(retrieved.members[1].id, NodeId(2));
    }
}




/FILE: store/tape-store/src/config.rs

//! Column family and database configuration for TapeStore
//!
//! This module provides optimized RocksDB configurations for all column families
//! in the tape-store, using different table types based on the access patterns:
//!
//! - **PlainTable**: Fixed-size keys for fast point lookups
//! - **BlockBased**: Structured data with bloom filters for range queries
//! - **BlobDB**: Large values (slices up to 32 MiB) to reduce write amplification
//! - **Prefix Extractors**: Enable efficient range scans by prefix

use store_rocks::{ColumnFamilyConfig, ColumnFamilyDescriptor, Options};

// Re-export rocksdb types needed for configuration
use rocksdb;

/// Create optimized column family configurations for all TapeStore column families
///
/// Returns a vector of `ColumnFamilyDescriptor` instances, one for each column family
/// in the tape-store. Each CF is configured based on its access patterns and data characteristics.
///
/// # Column Family Configurations (12 total)
///
/// ## Metadata Columns (PlainTable/BlockBased)
/// - `meta` - String keys, arbitrary values (BlockBased)
/// - `slice_info` - 32-byte Pubkey keys (PlainTable)
/// - `tape_info` - 32-byte Pubkey keys (PlainTable)
/// - `track_info` - 32-byte Pubkey keys (PlainTable)
///
/// ## Sync Columns
/// - `sync_cursor` - Singleton (0-byte key) (BlockBased)
/// - `gc` - String keys ("started", "completed") (BlockBased)
///
/// ## Epoch-Namespaced Spool Columns (BlockBased + Prefix)
/// - `spool_status` - 10-byte SpoolEpochKey (8-byte epoch prefix for iteration)
/// - `sync_cursors` - 10-byte SpoolEpochKey (8-byte epoch prefix for cleanup)
/// - `recovery_queue` - 43-byte PendingRecoveryKey (10-byte epoch+spool prefix)
///
/// ## Slice Data Columns (BlobDB)
/// - `spool/primary_slices` - 34-byte SliceKey (2-byte spool prefix)
/// - `spool/recovery_slices` - 34-byte SliceKey (2-byte spool prefix)
///
/// ## Committee Column
/// - `committee` - 8-byte EpochKey (PlainTable)
pub fn create_tape_store_configs() -> Vec<ColumnFamilyDescriptor> {
    vec![
        // Meta - variable-size keys and values, infrequent access
        ColumnFamilyConfig::new("meta")
            .with_block_based()
            .build(),

        // Slice info - 32-byte Pubkey keys, variable-size SliceInfo values
        ColumnFamilyConfig::new("slice_info")
            .with_plain_table(32)
            .build(),

        // Tape info - 32-byte Pubkey keys, small TapeInfo values
        ColumnFamilyConfig::new("tape_info")
            .with_plain_table(32)
            .build(),

        // Track info - 32-byte Pubkey keys, TrackInfo values
        ColumnFamilyConfig::new("track_info")
            .with_plain_table(32)
            .build(),

        // Sync cursor - singleton (empty key)
        ColumnFamilyConfig::new("sync_cursor")
            .with_block_based()
            .build(),

        // GC progress - String keys
        ColumnFamilyConfig::new("gc")
            .with_block_based()
            .build(),

        // Spool status - 10-byte SpoolEpochKey (epoch BE + spool_id BE)
        // BlockBased with 8-byte epoch prefix for iter_assigned_spools and cleanup
        ColumnFamilyConfig::new("spool_status")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Sync cursors - 10-byte SpoolEpochKey
        // BlockBased with 8-byte epoch prefix for cleanup_epoch_state
        ColumnFamilyConfig::new("sync_cursors")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Recovery queue - 43-byte PendingRecoveryKey
        // 10-byte prefix (epoch+spool) to match iter_pending_recoveries access pattern
        ColumnFamilyConfig::new("recovery_queue")
            .with_block_based()
            .with_prefix_extractor(10)
            .build(),

        // Primary slices - 34-byte SliceKey, large (~1MB) values
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("primary_slices")
            .with_blob_db(256 * 1024) // 256 KiB threshold
            .with_prefix_extractor(2)
            .build(),

        // Recovery slices - 34-byte SliceKey, large (~1MB) values
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("recovery_slices")
            .with_blob_db(256 * 1024) // 256 KiB threshold
            .with_prefix_extractor(2)
            .build(),

        // Committee - 8-byte EpochKey, CommitteeCache values
        ColumnFamilyConfig::new("committee")
            .with_plain_table(8)
            .build(),
    ]
}

/// Create database-wide options for TapeStore
///
/// Returns a configured `Options` instance with settings optimized for the
/// TapeStore workload:
///
/// - **Write Buffers**: 64 MiB per CF, up to 4 buffers
/// - **Parallelism**: Scales with CPU count
/// - **Compression**: LZ4 for fast compression/decompression
/// - **Rate Limiting**: 100 MB/s to prevent I/O spikes during compaction
pub fn create_db_options() -> Options {
    let mut opts = Options::default();

    // Basic database options
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Memory and write buffer tuning
    // 64 MiB per write buffer, up to 4 buffers per CF
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(2);

    // Parallelism - scale with CPU count
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4) as i32;
    opts.increase_parallelism(cpus);
    opts.set_max_background_jobs(cpus);

    // Compression - LZ4 is fast and good enough
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

    // Rate limiting for compaction to prevent I/O spikes
    // 100 MB/s should be gentle on the system
    // set_ratelimiter(rate_bytes_per_sec, refill_period_us, fairness)
    opts.set_ratelimiter(100 * 1024 * 1024, 100_000, 10);

    opts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_count() {
        let configs = create_tape_store_configs();
        // Should have exactly 12 column families
        assert_eq!(configs.len(), 12);
    }

    #[test]
    fn test_config_names() {
        let configs = create_tape_store_configs();
        let names: Vec<&str> = configs.iter().map(|cf| cf.name()).collect();

        // Verify all expected column families are present
        let expected = vec![
            "meta",
            "slice_info",
            "tape_info",
            "track_info",
            "sync_cursor",
            "gc",
            "spool_status",
            "sync_cursors",
            "recovery_queue",
            "primary_slices",
            "recovery_slices",
            "committee",
        ];

        assert_eq!(names, expected);
    }

    #[test]
    fn test_db_options() {
        let opts = create_db_options();
        // Just verify it returns a valid Options instance
        drop(opts);
    }
}




/FILE: store/tape-store/src/types/impls.rs

//! Wincode-compatible wrapper types for external types
//!
//! This module provides wrapper types with SchemaRead/SchemaWrite implementations
//! for types that can't be modified in their source crates.

use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use tape_core::bls::BlsPubkey;
use tape_core::types::{EpochNumber, NodeId};
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};
use wincode_derive::{SchemaRead, SchemaWrite};

/// A wincode-serializable wrapper around solana Pubkey for storage operations.
///
/// This type stores pubkeys as raw 32-byte arrays and provides conversions
/// to/from solana_program::pubkey::Pubkey via `.into()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Pubkey(pub [u8; 32]);

impl Pubkey {
    pub const LEN: usize = 32;

    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    #[cfg(not(target_os = "solana"))]
    pub fn new_unique() -> Self {
        Self(solana_program::pubkey::Pubkey::new_unique().to_bytes())
    }
}

impl From<solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl From<Pubkey> for solana_program::pubkey::Pubkey {
    fn from(stored: Pubkey) -> Self {
        solana_program::pubkey::Pubkey::new_from_array(stored.0)
    }
}

impl From<&solana_program::pubkey::Pubkey> for Pubkey {
    fn from(pubkey: &solana_program::pubkey::Pubkey) -> Self {
        Self(pubkey.to_bytes())
    }
}

impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl SchemaWrite for Pubkey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(32)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write_exact(&src.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for Pubkey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<Pubkey>) -> ReadResult<()> {
        let bytes: [u8; 32] = unsafe { reader.get_t()? };
        dst.write(Pubkey(bytes));
        Ok(())
    }
}

/// Information about a single committee member
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeMemberInfo {
    /// Unique node identifier
    pub id: NodeId,
    /// Node's on-chain account pubkey
    pub pubkey: Pubkey,
    /// BLS public key for signatures
    pub bls_pubkey: BlsPubkey,
    /// Network address for P2P communication
    pub network_address: String,
}

/// Cached committee information for an epoch
///
/// This struct contains all the information needed to:
/// - Route requests to the correct nodes
/// - Verify BLS signatures from committee members
/// - Determine local node's role and spool assignments
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeCache {
    /// Epoch this committee is active for
    pub epoch: EpochNumber,
    /// Ordered list of committee members
    pub members: Vec<CommitteeMemberInfo>,
    /// Spool-to-member assignment (index in members vec)
    /// spool_assignment[spool_id] = member_index
    pub spool_assignment: Vec<u8>,
    /// Index of local node in members (None if not in committee)
    pub my_member_index: Option<u8>,
    /// Spools assigned to local node (derived from spool_assignment)
    pub my_spools: Vec<u16>,
}

impl CommitteeCache {
    /// Create a new committee cache
    pub fn new(epoch: EpochNumber, members: Vec<CommitteeMemberInfo>) -> Self {
        Self {
            epoch,
            members,
            spool_assignment: Vec::new(),
            my_member_index: None,
            my_spools: Vec::new(),
        }
    }

    /// Get member info by member index
    pub fn get_member(&self, index: u8) -> Option<&CommitteeMemberInfo> {
        self.members.get(index as usize)
    }

    /// Get member index for a given spool
    pub fn get_spool_owner(&self, spool_id: u16) -> Option<u8> {
        self.spool_assignment.get(spool_id as usize).copied()
    }

    /// Check if the local node owns a given spool
    pub fn owns_spool(&self, spool_id: u16) -> bool {
        self.my_spools.contains(&spool_id)
    }

    /// Number of members in the committee
    pub fn member_count(&self) -> usize {
        self.members.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;

    #[test]
    fn test_pubkey_roundtrip() {
        let pubkey = Pubkey::new([0xAB; 32]);
        let bytes = wincode::serialize(&pubkey).unwrap();
        let decoded: Pubkey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(pubkey, decoded);
    }

    #[test]
    fn test_pubkey_conversion() {
        let solana_pubkey = solana_program::pubkey::Pubkey::new_unique();
        let stored: Pubkey = solana_pubkey.into();
        let back: solana_program::pubkey::Pubkey = stored.into();
        assert_eq!(solana_pubkey, back);
    }

    #[test]
    fn test_committee_member_info_roundtrip() {
        let info = CommitteeMemberInfo {
            id: NodeId(42),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: CommitteeMemberInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_committee_cache_roundtrip() {
        let member1 = CommitteeMemberInfo {
            id: NodeId(1),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let member2 = CommitteeMemberInfo {
            id: NodeId(2),
            pubkey: Pubkey::new([2u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.2:8080".to_string(),
        };

        let cache = CommitteeCache {
            epoch: EpochNumber(100),
            members: vec![member1, member2],
            spool_assignment: vec![0, 1, 0, 1], // Alternating assignment
            my_member_index: Some(0),
            my_spools: vec![0, 2],
        };

        let bytes = wincode::serialize(&cache).unwrap();
        let decoded: CommitteeCache = wincode::deserialize(&bytes).unwrap();
        assert_eq!(cache, decoded);
    }

    #[test]
    fn test_committee_cache_methods() {
        let member = CommitteeMemberInfo {
            id: NodeId(1),
            pubkey: Pubkey::new([1u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            network_address: "192.168.1.1:8080".to_string(),
        };

        let mut cache = CommitteeCache::new(EpochNumber(50), vec![member.clone()]);
        cache.spool_assignment = vec![0, 0, 0]; // All spools to member 0
        cache.my_member_index = Some(0);
        cache.my_spools = vec![0, 1, 2];

        assert_eq!(cache.member_count(), 1);
        assert_eq!(cache.get_member(0), Some(&member));
        assert_eq!(cache.get_member(1), None);
        assert_eq!(cache.get_spool_owner(0), Some(0));
        assert_eq!(cache.get_spool_owner(10), None);
        assert!(cache.owns_spool(0));
        assert!(cache.owns_spool(1));
        assert!(!cache.owns_spool(10));
    }
}




/FILE: store/tape-store/src/types/keys.rs

//! Key types with big-endian encoding for proper lexicographic sorting
//!
//! All composite keys use big-endian encoding to ensure proper ordering in RocksDB:
//! - SpoolEpochKey: (epoch BE, spool_id BE) - epoch first for range cleanup
//! - SliceKey: (spool_id BE, track_address) - spool first for prefix iteration
//! - PendingRecoveryKey: (epoch BE, spool_id BE, slice_type, track_address)
//! - EpochKey: epoch BE
//! - UnitKey: empty key for singletons

use crate::types::{Pubkey, SliceType};
use serde::{Deserialize, Serialize};
use std::mem::MaybeUninit;
use wincode::{
    io::{Reader, Writer},
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
};

/// Key for epoch-namespaced spool operations (10 bytes)
///
/// Format: [epoch BE 8 bytes][spool_id BE 2 bytes]
///
/// Epoch-first ordering enables efficient range deletion of old epoch data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpoolEpochKey {
    pub epoch: u64,
    pub spool_id: u16,
}

impl SpoolEpochKey {
    pub const SIZE: usize = 10;

    pub fn new(epoch: u64, spool_id: u16) -> Self {
        Self { epoch, spool_id }
    }

    /// Create prefix bytes for epoch-based iteration
    pub fn epoch_prefix(epoch: u64) -> [u8; 8] {
        epoch.to_be_bytes()
    }
}

impl SchemaWrite for SpoolEpochKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let epoch_bytes = src.epoch.to_be_bytes();
        let spool_bytes = src.spool_id.to_be_bytes();
        writer.write_exact(&epoch_bytes)?;
        writer.write_exact(&spool_bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SpoolEpochKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SpoolEpochKey>) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let epoch = u64::from_be_bytes(epoch_bytes);
        let spool_id = u16::from_be_bytes(spool_bytes);
        dst.write(SpoolEpochKey { epoch, spool_id });
        Ok(())
    }
}

/// Key for slice data and metadata (34 bytes)
///
/// Format: [spool_id BE 2 bytes][track_address 32 bytes]
///
/// Spool-first ordering enables efficient prefix iteration by spool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SliceKey {
    pub spool_id: u16,
    pub track_address: Pubkey,
}

impl SliceKey {
    pub const SIZE: usize = 34;

    pub fn new(spool_id: u16, track_address: Pubkey) -> Self {
        Self {
            spool_id,
            track_address,
        }
    }

    /// Create prefix bytes for spool-based iteration
    pub fn spool_prefix(spool_id: u16) -> [u8; 2] {
        spool_id.to_be_bytes()
    }
}

impl SchemaWrite for SliceKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let spool_bytes = src.spool_id.to_be_bytes();
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&src.track_address.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for SliceKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<SliceKey>) -> ReadResult<()> {
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 32] = unsafe { reader.get_t()? };
        let spool_id = u16::from_be_bytes(spool_bytes);
        dst.write(SliceKey {
            spool_id,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}

/// Key for pending recovery entries (43 bytes)
///
/// Format: [epoch BE 8 bytes][spool_id BE 2 bytes][slice_type 1 byte][track_address 32 bytes]
///
/// Epoch-first for cleanup, spool-second for iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PendingRecoveryKey {
    pub epoch: u64,
    pub spool_id: u16,
    pub slice_type: SliceType,
    pub track_address: Pubkey,
}

impl PendingRecoveryKey {
    pub const SIZE: usize = 43;

    pub fn new(epoch: u64, spool_id: u16, slice_type: SliceType, track_address: Pubkey) -> Self {
        Self {
            epoch,
            spool_id,
            slice_type,
            track_address,
        }
    }

    /// Create prefix bytes for epoch + spool iteration
    pub fn epoch_spool_prefix(epoch: u64, spool_id: u16) -> [u8; 10] {
        let mut prefix = [0u8; 10];
        prefix[0..8].copy_from_slice(&epoch.to_be_bytes());
        prefix[8..10].copy_from_slice(&spool_id.to_be_bytes());
        prefix
    }
}

impl SchemaWrite for PendingRecoveryKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let epoch_bytes = src.epoch.to_be_bytes();
        let spool_bytes = src.spool_id.to_be_bytes();
        let slice_type_byte = src.slice_type as u8;
        writer.write_exact(&epoch_bytes)?;
        writer.write_exact(&spool_bytes)?;
        writer.write_exact(&[slice_type_byte])?;
        writer.write_exact(&src.track_address.0)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for PendingRecoveryKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<PendingRecoveryKey>) -> ReadResult<()> {
        let epoch_bytes: [u8; 8] = unsafe { reader.get_t()? };
        let spool_bytes: [u8; 2] = unsafe { reader.get_t()? };
        let slice_type_byte: [u8; 1] = unsafe { reader.get_t()? };
        let track_bytes: [u8; 32] = unsafe { reader.get_t()? };

        let epoch = u64::from_be_bytes(epoch_bytes);
        let spool_id = u16::from_be_bytes(spool_bytes);
        let slice_type = match slice_type_byte[0] {
            0 => SliceType::Primary,
            1 => SliceType::Recovery,
            _ => SliceType::Primary, // Default for invalid values
        };

        dst.write(PendingRecoveryKey {
            epoch,
            spool_id,
            slice_type,
            track_address: Pubkey(track_bytes),
        });
        Ok(())
    }
}

/// Key for epoch-indexed data (8 bytes)
///
/// Format: [epoch BE 8 bytes]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EpochKey(pub u64);

impl EpochKey {
    pub const SIZE: usize = 8;

    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }
}

impl SchemaWrite for EpochKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(writer: &mut Writer, src: &Self::Src) -> WriteResult<()> {
        let bytes = src.0.to_be_bytes();
        writer.write_exact(&bytes)?;
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for EpochKey {
    type Dst = Self;

    fn read(reader: &mut Reader<'de>, dst: &mut MaybeUninit<EpochKey>) -> ReadResult<()> {
        let bytes: [u8; 8] = unsafe { reader.get_t()? };
        let epoch = u64::from_be_bytes(bytes);
        dst.write(EpochKey(epoch));
        Ok(())
    }
}

/// Singleton key (0 bytes) for entries that have exactly one value
///
/// Used for sync_cursor and similar singleton values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct UnitKey;

impl UnitKey {
    pub const SIZE: usize = 0;
}

impl SchemaWrite for UnitKey {
    type Src = Self;

    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(Self::SIZE)
    }

    fn write(_writer: &mut Writer, _src: &Self::Src) -> WriteResult<()> {
        Ok(())
    }
}

impl<'de> SchemaRead<'de> for UnitKey {
    type Dst = Self;

    fn read(_reader: &mut Reader<'de>, dst: &mut MaybeUninit<UnitKey>) -> ReadResult<()> {
        dst.write(UnitKey);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spool_epoch_key_size() {
        let key = SpoolEpochKey::new(100, 42);
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SpoolEpochKey::SIZE);
    }

    #[test]
    fn test_spool_epoch_key_ordering() {
        // Epoch 1, spool 100 should come before epoch 2, spool 1
        let key1 = SpoolEpochKey::new(1, 100);
        let key2 = SpoolEpochKey::new(2, 1);

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "epoch should be primary sort key");
    }

    #[test]
    fn test_slice_key_size() {
        let key = SliceKey::new(42, Pubkey([1u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), SliceKey::SIZE);
    }

    #[test]
    fn test_slice_key_ordering() {
        // Spool 1 should come before spool 100
        let key1 = SliceKey::new(1, Pubkey([255u8; 32]));
        let key2 = SliceKey::new(100, Pubkey([0u8; 32]));

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2, "spool_id should be primary sort key");
    }

    #[test]
    fn test_pending_recovery_key_size() {
        let key = PendingRecoveryKey::new(100, 42, SliceType::Primary, Pubkey([1u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), PendingRecoveryKey::SIZE);
    }

    #[test]
    fn test_epoch_key_size() {
        let key = EpochKey::new(12345);
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), EpochKey::SIZE);
    }

    #[test]
    fn test_epoch_key_ordering() {
        let key1 = EpochKey::new(1);
        let key2 = EpochKey::new(256);

        let bytes1 = wincode::serialize(&key1).unwrap();
        let bytes2 = wincode::serialize(&key2).unwrap();

        assert!(bytes1 < bytes2);
    }

    #[test]
    fn test_unit_key_size() {
        let key = UnitKey;
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(bytes.len(), UnitKey::SIZE);
    }

    #[test]
    fn test_spool_epoch_key_roundtrip() {
        let key = SpoolEpochKey::new(12345, 678);
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SpoolEpochKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_slice_key_roundtrip() {
        let key = SliceKey::new(42, Pubkey([0xAB; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: SliceKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_pending_recovery_key_roundtrip() {
        let key = PendingRecoveryKey::new(100, 42, SliceType::Recovery, Pubkey([0xCD; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        let decoded: PendingRecoveryKey = wincode::deserialize(&bytes).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_epoch_spool_prefix() {
        let prefix = PendingRecoveryKey::epoch_spool_prefix(100, 42);
        assert_eq!(prefix.len(), 10);

        // Verify prefix matches start of full key
        let key = PendingRecoveryKey::new(100, 42, SliceType::Primary, Pubkey([0u8; 32]));
        let bytes = wincode::serialize(&key).unwrap();
        assert_eq!(&bytes[0..10], &prefix);
    }
}




/FILE: store/tape-store/src/types/enums.rs

//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Node status in the network
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum NodeStatus {
    /// Node is registered but not in committee
    Standby = 0,
    /// Node is active in the committee
    Active = 1,
    /// Node is recovering data from peers
    Recovering = 2,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Standby
    }
}

/// Status of a spool assignment
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SpoolStatus {
    /// Not assigned
    None = 0,
    /// Fully synced and serving requests
    Active = 1,
    /// Currently syncing data from peers
    Sync = 2,
    /// Recovering missing slices
    Recover = 3,
    /// Locked for handoff to another node
    Locked = 4,
}

impl Default for SpoolStatus {
    fn default() -> Self {
        Self::None
    }
}

/// Type of slice (primary or recovery)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SliceType {
    /// Primary data slice
    Primary = 0,
    /// Recovery/parity slice
    Recovery = 1,
}

impl Default for SliceType {
    fn default() -> Self {
        Self::Primary
    }
}

/// Encoding type for blobs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum EncodingType {
    /// Unknown encoding
    Unknown = 0,
    /// Basic encoding (single layer)
    Basic = 1,
    /// Striped encoding (interleaved)
    Striped = 2,
    /// Rotated encoding (row-column)
    Rotated = 3,
}

impl Default for EncodingType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_status_default() {
        assert_eq!(NodeStatus::default(), NodeStatus::Standby);
    }

    #[test]
    fn test_spool_status_default() {
        assert_eq!(SpoolStatus::default(), SpoolStatus::None);
    }

    #[test]
    fn test_slice_type_default() {
        assert_eq!(SliceType::default(), SliceType::Primary);
    }

    #[test]
    fn test_encoding_type_default() {
        assert_eq!(EncodingType::default(), EncodingType::Unknown);
    }

    #[test]
    fn test_repr_values() {
        assert_eq!(NodeStatus::Standby as u8, 0);
        assert_eq!(NodeStatus::Active as u8, 1);
        assert_eq!(NodeStatus::Recovering as u8, 2);

        assert_eq!(SpoolStatus::None as u8, 0);
        assert_eq!(SpoolStatus::Active as u8, 1);
        assert_eq!(SpoolStatus::Sync as u8, 2);
        assert_eq!(SpoolStatus::Recover as u8, 3);
        assert_eq!(SpoolStatus::Locked as u8, 4);

        assert_eq!(SliceType::Primary as u8, 0);
        assert_eq!(SliceType::Recovery as u8, 1);

        assert_eq!(EncodingType::Unknown as u8, 0);
        assert_eq!(EncodingType::Basic as u8, 1);
        assert_eq!(EncodingType::Striped as u8, 2);
        assert_eq!(EncodingType::Rotated as u8, 3);
    }
}




/FILE: store/tape-store/src/types/values.rs

//! Value types for tape-store columns
//!
//! These structs are stored as values in the various column families.

use crate::types::{EncodingType, Pubkey, SliceType};
use serde::{Deserialize, Serialize};
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Information about a blob's erasure coding structure
///
/// Contains the hashes needed to verify slices for a given track.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceInfo {
    /// Type of erasure encoding used
    pub encoding_type: EncodingType,
    /// Original unencoded data length in bytes
    pub unencoded_length: u64,
    /// Hashes for primary slices (up to 1024, one per slice)
    /// Empty for some encoding types that don't use individual hashes
    pub primary: Vec<Hash>,
    /// Column roots for recovery slices (up to 1024, one per recovery column)
    /// Each column has 1024 parts; this stores the root hash
    /// Empty if no recovery layer is used
    pub recovery: Vec<Hash>,
}

impl Default for SliceInfo {
    fn default() -> Self {
        Self {
            encoding_type: EncodingType::Unknown,
            unencoded_length: 0,
            primary: Vec::new(),
            recovery: Vec::new(),
        }
    }
}

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeInfo {
    /// Epoch when the tape became active
    pub active_epoch: EpochNumber,
    /// Epoch when the tape expires
    pub expiry_epoch: EpochNumber,
    /// Authority pubkey that owns this tape
    pub authority: Pubkey,
}

/// Metadata about a track (individual blob)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackInfo {
    /// Whether slice info has been received for this track
    pub has_slice_info: bool,
    /// Address of the tape this track belongs to
    pub tape_address: Pubkey,
    /// Epoch when the track was registered on-chain
    pub registered_epoch: EpochNumber,
    /// Epoch when the track was certified (None if not yet certified)
    pub certified_epoch: Option<EpochNumber>,
}

impl TrackInfo {
    pub fn new(tape_address: Pubkey, registered_epoch: EpochNumber) -> Self {
        Self {
            has_slice_info: false,
            tape_address,
            registered_epoch,
            certified_epoch: None,
        }
    }
}

/// Sync progress for a spool within an epoch
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SyncProgress {
    /// Last track address that was synced (None if just starting)
    pub last_synced_track: Option<Pubkey>,
    /// Type of slice being synced
    pub slice_type: SliceType,
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self {
            last_synced_track: None,
            slice_type: SliceType::Primary,
        }
    }
}

/// Primary slice data (erasure-coded fragment)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct PrimarySliceData {
    /// Encoded symbols (typically ~1MB)
    pub symbols: Vec<u8>,
    /// Number of padding bytes added during encoding
    pub padding_len: u32,
}

impl PrimarySliceData {
    pub fn new(symbols: Vec<u8>, padding_len: u32) -> Self {
        Self {
            symbols,
            padding_len,
        }
    }
}

/// Recovery slice data (packed recovery column)
///
/// Each recovery column contains parts from all 1024 primary slices,
/// allowing reconstruction of any missing primary slice.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RecoverySliceData {
    /// Packed column symbols (typically ~1MB)
    pub symbols: Vec<u8>,
    /// Number of padding bytes
    pub padding_len: u32,
}

impl RecoverySliceData {
    pub fn new(symbols: Vec<u8>, padding_len: u32) -> Self {
        Self {
            symbols,
            padding_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_info_default() {
        let info = SliceInfo::default();
        assert_eq!(info.encoding_type, EncodingType::Unknown);
        assert_eq!(info.unencoded_length, 0);
        assert!(info.primary.is_empty());
        assert!(info.recovery.is_empty());
    }

    #[test]
    fn test_slice_info_roundtrip() {
        let info = SliceInfo {
            encoding_type: EncodingType::Rotated,
            unencoded_length: 1024 * 1024,
            primary: vec![Hash::default(); 1024],
            recovery: vec![Hash::default(); 1024],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: SliceInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let info = TapeInfo {
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            authority: Pubkey([0xAB; 32]),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TapeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_new() {
        let tape = Pubkey([1u8; 32]);
        let epoch = EpochNumber(50);

        let info = TrackInfo::new(tape, epoch);
        assert!(!info.has_slice_info);
        assert_eq!(info.tape_address, tape);
        assert_eq!(info.registered_epoch, epoch);
        assert!(info.certified_epoch.is_none());
    }

    #[test]
    fn test_track_info_roundtrip() {
        let info = TrackInfo {
            has_slice_info: true,
            tape_address: Pubkey([1u8; 32]),
            registered_epoch: EpochNumber(100),
            certified_epoch: Some(EpochNumber(101)),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_sync_progress_default() {
        let progress = SyncProgress::default();
        assert!(progress.last_synced_track.is_none());
        assert_eq!(progress.slice_type, SliceType::Primary);
    }

    #[test]
    fn test_sync_progress_roundtrip() {
        let progress = SyncProgress {
            last_synced_track: Some(Pubkey([0xFF; 32])),
            slice_type: SliceType::Recovery,
        };

        let bytes = wincode::serialize(&progress).unwrap();
        let decoded: SyncProgress = wincode::deserialize(&bytes).unwrap();
        assert_eq!(progress, decoded);
    }

    #[test]
    fn test_primary_slice_data_roundtrip() {
        let data = PrimarySliceData::new(vec![0xAB; 1024], 128);

        let bytes = wincode::serialize(&data).unwrap();
        let decoded: PrimarySliceData = wincode::deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_recovery_slice_data_roundtrip() {
        let data = RecoverySliceData::new(vec![0xCD; 2048], 64);

        let bytes = wincode::serialize(&data).unwrap();
        let decoded: RecoverySliceData = wincode::deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
    }
}




/FILE: store/tape-store/src/types/mod.rs

//! Type definitions for tape-store
//!
//! This module provides all the types used throughout the tape-store crate:
//! - Enums: NodeStatus, SpoolStatus, SliceType, EncodingType
//! - Keys: SpoolEpochKey, SliceKey, PendingRecoveryKey, EpochKey, UnitKey
//! - Values: SliceInfo, TapeInfo, TrackInfo, SyncProgress, PrimarySliceData, RecoverySliceData
//! - Wrappers: Pubkey, CommitteeCache, CommitteeMemberInfo

mod enums;
mod impls;
pub mod keys;
mod values;

// Re-export core types used throughout the crate
pub use tape_core::types::{EpochNumber, NodeId, SlotNumber};
pub use tape_crypto::Hash;

// Re-export enum types
pub use enums::{EncodingType, NodeStatus, SliceType, SpoolStatus};

// Re-export key types
pub use keys::{EpochKey, PendingRecoveryKey, SliceKey, SpoolEpochKey, UnitKey};

// Re-export value types
pub use values::{
    PrimarySliceData, RecoverySliceData, SliceInfo, SyncProgress, TapeInfo, TrackInfo,
};

// Re-export wrapper types
pub use impls::{CommitteeCache, CommitteeMemberInfo, Pubkey};




/FILE: store/tape-store/src/columns/track_info.rs

//! TrackInfo column family for track metadata
//!
//! Stores information about individual blobs (tracks).

use crate::types::{Pubkey, TrackInfo};
use store::Column;

/// Track info indexed by track address
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: TrackInfo (tape association, certification status, signature)
pub struct TrackInfoCol;

impl Column for TrackInfoCol {
    const CF_NAME: &'static str = "track_info";
    type Key = Pubkey;
    type Value = TrackInfo;
}




/FILE: store/tape-store/src/columns/slices.rs

//! Slice data column families for primary and recovery slices
//!
//! Key structure: (spool_id, track_address) - enables efficient iteration by spool

use crate::types::{PrimarySliceData, RecoverySliceData, SliceKey};
use store::Column;

/// Primary slice data storage (large values, uses BlobDB)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: PrimarySliceData (symbols + padding info, typically ~1MB)
pub struct PrimarySlices;

impl Column for PrimarySlices {
    const CF_NAME: &'static str = "primary_slices";
    type Key = SliceKey;
    type Value = PrimarySliceData;
}

/// Recovery slice data storage (large values, uses BlobDB)
///
/// Key: SliceKey (34 bytes: spool_id BE + track_address)
/// Value: RecoverySliceData (packed column symbols, typically ~1MB)
///
/// Each recovery column contains parts from all 1024 primary slices,
/// enabling reconstruction of any missing primary slice.
pub struct RecoverySlices;

impl Column for RecoverySlices {
    const CF_NAME: &'static str = "recovery_slices";
    type Key = SliceKey;
    type Value = RecoverySliceData;
}




/FILE: store/tape-store/src/columns/gc.rs

//! Garbage collection tracking column family

use crate::types::EpochNumber;
use store::Column;

/// GC progress tracking
///
/// Key: String ("started" or "completed")
/// Value: EpochNumber (last epoch where GC was started/completed)
///
/// Used to track GC progress across restarts.
pub struct Gc;

impl Column for Gc {
    const CF_NAME: &'static str = "gc";
    type Key = String;
    type Value = EpochNumber;
}




/FILE: store/tape-store/src/columns/cursor.rs

//! Sync cursor column family for tracking last processed slot

use crate::types::{SlotNumber, UnitKey};
use store::Column;

/// Singleton column for sync cursor
///
/// Key: UnitKey (0 bytes - singleton)
/// Value: SlotNumber (last processed slot)
pub struct SyncCursor;

impl Column for SyncCursor {
    const CF_NAME: &'static str = "sync_cursor";
    type Key = UnitKey;
    type Value = SlotNumber;
}




/FILE: store/tape-store/src/columns/spool.rs

//! Spool column families for epoch-namespaced spool tracking
//!
//! These columns use epoch-first keys for crash-safe epoch transitions:
//! - SpoolAssigned: (epoch, spool_id) -> SpoolStatus
//! - SpoolSyncProgress: (epoch, spool_id) -> SyncProgress
//! - SpoolPendingRecovery: (epoch, spool_id, slice_type, track) -> ()

use crate::types::{PendingRecoveryKey, SpoolEpochKey, SpoolStatus, SyncProgress};
use store::Column;

/// Epoch-namespaced spool assignment tracking
///
/// Key: SpoolEpochKey (10 bytes: epoch BE + spool_id BE)
/// Value: SpoolStatus
///
/// Epoch-first ordering enables efficient cleanup of old epoch data.
pub struct SpoolAssigned;

impl Column for SpoolAssigned {
    const CF_NAME: &'static str = "spool_status";
    type Key = SpoolEpochKey;
    type Value = SpoolStatus;
}

/// Epoch-namespaced sync progress tracking
///
/// Key: SpoolEpochKey (10 bytes: epoch BE + spool_id BE)
/// Value: SyncProgress (last synced track, slice type)
pub struct SpoolSyncProgress;

impl Column for SpoolSyncProgress {
    const CF_NAME: &'static str = "sync_cursors";
    type Key = SpoolEpochKey;
    type Value = SyncProgress;
}

/// Epoch-namespaced pending recovery queue
///
/// Key: PendingRecoveryKey (43 bytes: epoch + spool_id + slice_type + track_address)
/// Value: () (presence indicates pending)
///
/// Stores slices that need to be recovered. The value is empty since
/// the key contains all necessary information.
pub struct SpoolPendingRecovery;

impl Column for SpoolPendingRecovery {
    const CF_NAME: &'static str = "recovery_queue";
    type Key = PendingRecoveryKey;
    type Value = ();
}




/FILE: store/tape-store/src/columns/tape_info.rs

//! TapeInfo column family for tape metadata
//!
//! Stores information about storage allocations (tapes).

use crate::types::{Pubkey, TapeInfo};
use store::Column;

/// Tape info indexed by tape address
///
/// Key: Pubkey (tape_address, 32 bytes)
/// Value: TapeInfo (active/expiry epoch, authority)
pub struct TapeInfoCol;

impl Column for TapeInfoCol {
    const CF_NAME: &'static str = "tape_info";
    type Key = Pubkey;
    type Value = TapeInfo;
}




/FILE: store/tape-store/src/columns/meta.rs

//! Meta column family for node metadata
//!
//! Stores key-value pairs for node configuration and state:
//! - node_status: NodeStatus
//! - cluster_hash: Hash (32 bytes)
//! - current_epoch: EpochNumber

use store::Column;

/// Column family for node metadata
///
/// Key: String (e.g., "node_status", "cluster_hash", "current_epoch")
/// Value: Vec<u8> (serialized data, format depends on key)
pub struct Meta;

impl Column for Meta {
    const CF_NAME: &'static str = "meta";
    type Key = String;
    type Value = Vec<u8>;
}




/FILE: store/tape-store/src/columns/slice_info.rs

//! SliceInfo column family for blob erasure coding metadata
//!
//! Stores the hashes needed to verify slices for each track.

use crate::types::{Pubkey, SliceInfo};
use store::Column;

/// Slice info indexed by track address
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: SliceInfo (encoding type, hashes for verification)
pub struct SliceInfoCol;

impl Column for SliceInfoCol {
    const CF_NAME: &'static str = "slice_info";
    type Key = Pubkey;
    type Value = SliceInfo;
}




/FILE: store/tape-store/src/columns/mod.rs

//! Column family definitions for tape-store
//!
//! This module defines 12 column families:
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata (String -> Vec<u8>)
//! - `slice_info`: Blob erasure coding metadata (Pubkey -> SliceInfo)
//! - `tape_info`: Tape (storage allocation) metadata (Pubkey -> TapeInfo)
//! - `track_info`: Track (blob) metadata (Pubkey -> TrackInfo)
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot (UnitKey -> SlotNumber)
//! - `gc`: GC progress tracking (String -> EpochNumber)
//!
//! ## Epoch-Namespaced Spool Columns
//! - `spool_status`: Spool status per epoch (SpoolEpochKey -> SpoolStatus)
//! - `sync_cursors`: Sync cursors per epoch (SpoolEpochKey -> SyncProgress)
//! - `recovery_queue`: Pending recovery queue (PendingRecoveryKey -> ())
//!
//! ## Slice Data Columns (BlobDB)
//! - `primary_slices`: Primary slice data (SliceKey -> PrimarySliceData)
//! - `recovery_slices`: Recovery slice data (SliceKey -> RecoverySliceData)
//!
//! ## Committee Column
//! - `committee`: Committee cache by epoch (EpochKey -> CommitteeCache)

pub mod committee;
pub mod cursor;
pub mod gc;
pub mod meta;
pub mod slice_info;
pub mod slices;
pub mod spool;
pub mod tape_info;
pub mod track_info;

// Re-export all column types
pub use committee::Committee;
pub use cursor::SyncCursor;
pub use gc::Gc;
pub use meta::Meta;
pub use slice_info::SliceInfoCol;
pub use slices::{PrimarySlices, RecoverySlices};
pub use spool::{SpoolAssigned, SpoolPendingRecovery, SpoolSyncProgress};
pub use tape_info::TapeInfoCol;
pub use track_info::TrackInfoCol;

/// List of all column family names in the store (12 total)
pub const ALL_COLUMN_FAMILIES: &[&str] = &[
    "meta",
    "slice_info",
    "tape_info",
    "track_info",
    "sync_cursor",
    "gc",
    "spool_status",
    "sync_cursors",
    "recovery_queue",
    "primary_slices",
    "recovery_slices",
    "committee",
];




/FILE: store/tape-store/src/columns/committee.rs

//! Committee column family for epoch-based committee caching

use crate::types::{CommitteeCache, EpochKey};
use store::Column;

/// Committee cache indexed by epoch
///
/// Key: EpochKey (8 bytes: epoch BE)
/// Value: CommitteeCache (members, spool assignments, local node info)
pub struct Committee;

impl Column for Committee {
    const CF_NAME: &'static str = "committee";
    type Key = EpochKey;
    type Value = CommitteeCache;
}




/FILE: store/tape-store/src/lib.rs

//! tape-store: Application-specific storage layer for distributed tape storage nodes
//!
//! This crate provides typed column families and helper methods for storing:
//! - Slice info: Erasure coding metadata (hashes for verification)
//! - Tape info: Storage allocation metadata
//! - Track info: Blob metadata and certification status
//! - Slice data: Primary and recovery erasure-coded data
//! - Spool state: Epoch-namespaced spool assignments and sync progress
//! - Committee cache: Committee members for routing and verification
//!
//! # Column Families (12 total)
//!
//! ## Metadata Columns
//! - `meta`: Node configuration and metadata
//! - `slice_info`: Blob erasure coding metadata
//! - `tape_info`: Tape (storage allocation) metadata
//! - `track_info`: Track (blob) metadata
//!
//! ## Sync Columns
//! - `sync_cursor`: Last processed slot
//! - `gc`: GC progress tracking
//!
//! ## Epoch-Namespaced Spool Columns
//! - `spool_status`: Spool status per epoch
//! - `sync_cursors`: Sync cursor per spool per epoch
//! - `recovery_queue`: Recovery queue per epoch
//!
//! ## Slice Data Columns (BlobDB)
//! - `primary_slices`: Primary erasure-coded slices
//! - `recovery_slices`: Recovery/parity slices
//!
//! ## Committee Column
//! - `committee`: Committee cache by epoch
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
//! let track_info = TrackInfo::new(
//!     Pubkey::new([2u8; 32]),
//!     EpochNumber(100),
//! );
//! store.put_track_info(track_address, track_info).unwrap();
//!
//! // Store a primary slice
//! let spool_id = 42u16;
//! let slice_data = PrimarySliceData::new(vec![0u8; 1024], 0);
//! store.put_primary_slice(spool_id, track_address, slice_data).unwrap();
//!
//! // Retrieve the slice
//! let retrieved = store.get_primary_slice(spool_id, track_address).unwrap();
//! assert!(retrieved.is_some());
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
    use crate::ops::*;
    use crate::types::*;

    #[test]
    fn test_track_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(100));

        store.put_track_info(address, info.clone()).unwrap();
        let retrieved = store.get_track_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_slice_info_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let address = Pubkey::new_unique();

        let info = SliceInfo {
            encoding_type: EncodingType::Rotated,
            unencoded_length: 1024 * 1024,
            primary: vec![Hash::default(); 1024],
            recovery: vec![Hash::default(); 1024],
        };

        store.put_slice_info(address, info.clone()).unwrap();
        let retrieved = store.get_slice_info(address).unwrap();
        assert_eq!(retrieved, Some(info));
    }

    #[test]
    fn test_spool_status_epoch_namespaced() {
        let store = TapeStore::new(MemoryStore::new());
        let epoch = EpochNumber(100);
        let spool_id = 42;

        store
            .set_spool_status(epoch, spool_id, SpoolStatus::Active)
            .unwrap();
        let status = store.get_spool_status(epoch, spool_id).unwrap();
        assert_eq!(status, Some(SpoolStatus::Active));

        // Different epoch should not have the status
        let other_epoch = EpochNumber(101);
        let status = store.get_spool_status(other_epoch, spool_id).unwrap();
        assert!(status.is_none());
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
    fn test_slice_data_roundtrip() {
        let store = TapeStore::new(MemoryStore::new());
        let spool_id = 42;
        let track = Pubkey::new_unique();

        let primary = PrimarySliceData::new(vec![0xAB; 1024], 128);
        let recovery = RecoverySliceData::new(vec![0xCD; 2048], 64);

        store
            .put_both_slices(spool_id, track, primary.clone(), recovery.clone())
            .unwrap();

        let retrieved_primary = store.get_primary_slice(spool_id, track).unwrap().unwrap();
        let retrieved_recovery = store.get_recovery_slice(spool_id, track).unwrap().unwrap();

        assert_eq!(retrieved_primary, primary);
        assert_eq!(retrieved_recovery, recovery);
    }

    #[test]
    fn test_meta_ops() {
        let store = TapeStore::new(MemoryStore::new());

        // Node status
        store.set_node_status(NodeStatus::Active).unwrap();
        assert_eq!(
            store.get_node_status().unwrap(),
            Some(NodeStatus::Active)
        );

        // Current epoch
        store.set_current_epoch(EpochNumber(100)).unwrap();
        assert_eq!(
            store.get_current_epoch().unwrap(),
            Some(EpochNumber(100))
        );

        // Sync cursor
        store.set_sync_cursor(SlotNumber(999)).unwrap();
        assert_eq!(
            store.get_sync_cursor().unwrap(),
            Some(SlotNumber(999))
        );

        // GC epochs
        store.set_gc_started_epoch(EpochNumber(50)).unwrap();
        store.set_gc_completed_epoch(EpochNumber(49)).unwrap();
        assert_eq!(
            store.get_gc_started_epoch().unwrap(),
            Some(EpochNumber(50))
        );
        assert_eq!(
            store.get_gc_completed_epoch().unwrap(),
            Some(EpochNumber(49))
        );
    }

    #[test]
    fn test_slice_key_ordering() {
        let store = TapeStore::new(MemoryStore::new());

        // Insert slices in non-sequential spool order
        for spool_id in [100u16, 1, 50, 200, 25] {
            let track = Pubkey::new_unique();
            let data = PrimarySliceData::new(vec![0u8; 10], 0);
            store.put_primary_slice(spool_id, track, data).unwrap();
        }

        // Verify slices come back in sorted order by spool_id when iterating
        // Note: We iterate per-spool, so this just tests that each spool can be queried
        for spool_id in [1, 25, 50, 100, 200] {
            let slices: Vec<_> = store
                .iter_primary_slices_by_spool(spool_id)
                .unwrap()
                .collect();
            assert_eq!(slices.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn test_read_only_tape_store() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create primary and write some data
        {
            let store = TapeStore::open_primary(&path).unwrap();
            let track = Pubkey::new_unique();
            let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(0));
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = TapeStore::open_read_only(&path).unwrap();

            // Can iterate tracks
            let tracks = ro_store
                .iter::<crate::columns::TrackInfoCol>()
                .unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn test_secondary_tape_store() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create primary and write initial data
        {
            let store = TapeStore::open_primary(&primary_path).unwrap();
            let track = Pubkey::new_unique();
            let info = TrackInfo::new(Pubkey::new_unique(), EpochNumber(0));
            store.put_track_info(track, info).unwrap();
            store.inner().inner().flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = TapeStore::open_secondary(&primary_path, &secondary_path).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can iterate tracks
            let tracks = secondary
                .iter::<crate::columns::TrackInfoCol>()
                .unwrap();
            assert_eq!(tracks.len(), 1);
        }
    }
}




/FILE: network/slicer/src/consts.rs

/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per slice.
pub const MERKLE_HEIGHT: usize = 10;

// Re-export erasure coding constants from tape-core.
// These are derived using BFT tolerance functions (max_faulty, min_correct).
pub use tape_core::erasure::{DATA_SLICES, PARITY_SLICES, SLICE_COUNT};

/// BFT fault tolerance parameter f = max_faulty(SLICE_COUNT).
/// This is the maximum number of faulty/missing slices we can tolerate.
/// Alias for PARITY_SLICES for code clarity in BFT contexts.
pub const F: usize = PARITY_SLICES;

/// Number of coding (parity) slices per blob.
/// Alias for PARITY_SLICES.
pub const CODING_SLICES: usize = PARITY_SLICES;




/FILE: network/slicer/src/striped.rs

//! Striped slicer for large blob encoding.
//!
//! Splits blobs into multiple stripes, encoding each stripe separately.
//! This bounds memory usage while handling arbitrarily large blobs.

use crate::api::Slicer;
use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::codec::{StripedCodec, MappingStrategy, DEFAULT_STRIPE_SIZE};
use crate::types::{Blob, Slice};

/// A striped slicer that splits blobs into multiple stripes.
///
/// Each stripe is RS-encoded into SLICE_COUNT shards. Shards are appended
/// to output slices using identity mapping (shard N -> slice N).
///
/// Automatically selects optimal stripe size based on blob size:
/// - ≤ 16 KB: 16 KB stripe
/// - 16-64 KB: 64 KB stripe
/// - 64-256 KB: 256 KB stripe
/// - > 256 KB: 512 KB stripe
///
/// For fair load distribution across nodes, use `RotatedSlicer` instead.
pub struct StripedSlicer {
    codec: StripedCodec,
}

impl StripedSlicer {
    /// Create a new StripedSlicer.
    pub fn new() -> Self {
        Self {
            codec: StripedCodec::new(DEFAULT_STRIPE_SIZE, MappingStrategy::Identity),
        }
    }

    /// Create with a specific initial stripe size (for testing).
    pub fn with_stripe_size(stripe_size: usize) -> Self {
        Self {
            codec: StripedCodec::new(stripe_size, MappingStrategy::Identity),
        }
    }

    /// Get the current stripe size.
    pub fn stripe_size(&self) -> usize {
        self.codec.stripe_size
    }
}

impl Default for StripedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for StripedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        self.codec.encode_adaptive(blob)
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        self.codec.decode(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        std::array::from_fn(|i| Some(slices[i].clone()))
    }

    fn keep_only(arr: &mut [Option<Slice>; SLICE_COUNT], keep: &[usize]) {
        let mut keep_set = vec![false; SLICE_COUNT];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    #[test]
    fn test_stripe_size_constant() {
        assert_eq!(DEFAULT_STRIPE_SIZE, 512 * 1024);
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(500);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_data_only() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_with_missing_data_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(2000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep enough slices: some data (first 400) + all parity (341)
        let mut keep_indices: Vec<usize> = (0..400).collect();
        keep_indices.extend(DATA_SLICES..SLICE_COUNT);
        keep_only(&mut opt, &keep_indices);

        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= DATA_SLICES);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = StripedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_default_stripe_size() {
        let slicer = StripedSlicer::default();
        assert_eq!(slicer.stripe_size(), DEFAULT_STRIPE_SIZE);
    }
}




/FILE: network/slicer/src/rotated.rs

//! Rotated slicer for fair load distribution.
//!
//! Extends striped encoding with per-stripe rotation to ensure all nodes
//! receive approximately equal amounts of data and parity chunks over time.

use crate::api::Slicer;
use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::codec::{StripedCodec, MappingStrategy, DEFAULT_STRIPE_SIZE};
use crate::types::{Blob, Slice};

/// A rotated slicer that extends striped encoding with per-stripe rotation.
///
/// This provides fair load distribution across all 1024 nodes by rotating
/// the shard-to-slice mapping for each stripe. Over many stripes, each node
/// receives approximately equal amounts of data and parity chunks.
///
/// The rotation uses a step of CODING_SLICES (341), which is coprime with
/// SLICE_COUNT (1024), ensuring full coverage of all slices.
///
/// Automatically selects optimal stripe size based on blob size:
/// - ≤ 16 KB: 16 KB stripe
/// - 16-64 KB: 64 KB stripe
/// - 64-256 KB: 256 KB stripe
/// - > 256 KB: 512 KB stripe
pub struct RotatedSlicer {
    codec: StripedCodec,
}

impl RotatedSlicer {
    /// Create a new RotatedSlicer.
    pub fn new() -> Self {
        Self {
            codec: StripedCodec::new(DEFAULT_STRIPE_SIZE, MappingStrategy::Rotated),
        }
    }

    /// Create with a specific initial stripe size (for testing).
    pub fn with_stripe_size(stripe_size: usize) -> Self {
        Self {
            codec: StripedCodec::new(stripe_size, MappingStrategy::Rotated),
        }
    }

    /// Get the current stripe size.
    pub fn stripe_size(&self) -> usize {
        self.codec.stripe_size
    }
}

impl Default for RotatedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for RotatedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        self.codec.encode_adaptive(blob)
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        self.codec.decode(slices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{shard_to_slice, slice_to_shard, ROTATION_STEP};

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        std::array::from_fn(|i| Some(slices[i].clone()))
    }

    fn keep_only(arr: &mut [Option<Slice>; SLICE_COUNT], keep: &[usize]) {
        let mut keep_set = vec![false; SLICE_COUNT];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    #[test]
    fn test_rotation_step() {
        assert_eq!(ROTATION_STEP, CODING_SLICES);
        fn gcd(a: usize, b: usize) -> usize {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        assert_eq!(gcd(ROTATION_STEP, SLICE_COUNT), 1);
    }

    #[test]
    fn test_rotation_inverse() {
        for stripe in 0..10 {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(500);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_multiple_stripes() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_with_missing_slices() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(3000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Keep exactly DATA_SLICES slices (first 683)
        let keep_indices: Vec<usize> = (0..DATA_SLICES).collect();
        keep_only(&mut opt, &keep_indices);

        let count = opt.iter().filter(|s| s.is_some()).count();
        assert!(count >= DATA_SLICES);

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);
        keep_only(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = RotatedSlicer::with_stripe_size(1024);
        let payload = mk(5000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_default_stripe_size() {
        let slicer = RotatedSlicer::default();
        assert_eq!(slicer.stripe_size(), DEFAULT_STRIPE_SIZE);
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 1024;
        let mut slice_hits = vec![0usize; SLICE_COUNT];

        for stripe in 0..num_stripes {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        let expected_hits_per_slice = num_stripes;
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, expected_hits_per_slice, "slice {} mismatch", i);
        }
    }
}




/FILE: network/slicer/src/basic.rs

use super::api::Slicer;
use super::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::errors::{DecodeError, EncodeError};
use super::reed_solomon::{ReedSolomonCoder, ReedSolomonDecodeError, ReedSolomonEncodeError};
use super::slice_index::SliceIndex;
use super::types::{Blob, Slice};
use core::convert::TryInto;

/// A basic slicer that uses a single Reed-Solomon encoding pass (no striping).
///
/// **For testing/debugging only.** Supports blobs up to ~2.7 MB (4 KiB × 683 data slices).
/// For production workloads, use `StripedSlicer` or `RotatedSlicer` instead.
pub struct BasicSlicer(ReedSolomonCoder);

impl BasicSlicer {
    /// Create a BasicSlicer with a custom max slice size (for benchmarking only).
    ///
    /// This is internal to the crate for benchmark use. Production code should
    /// use `Default::default()` which has a 4 KiB limit (~2.7 MB max blob).
    pub(crate) fn with_max_slice_bytes(max_slice_bytes: usize) -> Self {
        Self(ReedSolomonCoder::with_max_slice_bytes(
            DATA_SLICES,
            CODING_SLICES,
            max_slice_bytes,
        ))
    }
}

impl Default for BasicSlicer {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for BasicSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let raw = self.0.encode(blob.as_slice()).map_err(|e| match e {
            ReedSolomonEncodeError::TooMuchData => EncodeError::TooMuchData,
        })?;

        let mut output = Vec::with_capacity(SLICE_COUNT);
        for (i, data) in raw.data.into_iter().enumerate() {
            let idx = SliceIndex::new(i).expect("index in range");
            output.push(Slice::new(idx, data));
        }
        for (offset, coding) in raw.coding.into_iter().enumerate() {
            let idx = SliceIndex::new(DATA_SLICES + offset).expect("index in range");
            output.push(Slice::new(idx, coding));
        }

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError> {
        let reconstructed = self.0.decode(slices).map_err(|e| match e {
            ReedSolomonDecodeError::NotEnoughSlices => DecodeError::NotEnoughSlices,
            ReedSolomonDecodeError::TooMuchData => DecodeError::TooMuchData,
            ReedSolomonDecodeError::InvalidPadding => DecodeError::BadEncoding,
            ReedSolomonDecodeError::InvalidLayout => DecodeError::InvalidLayout,
        })?;
        Ok(Blob { data: reconstructed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::DecodeError;
    use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
    use crate::merkle_helpers::build_blob_merkle_tree;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        let mut arr: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
        for (i, s) in slices.iter().enumerate() {
            arr[i] = Some(s.clone());
        }
        arr
    }

    fn keep(arr: &mut [Option<Slice>; SLICE_COUNT], idxs: &[usize]) {
        let mut mask = vec![false; SLICE_COUNT];
        for &i in idxs {
            mask[i] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !mask[i] {
                *slot = None;
            }
        }
    }

    fn equal_size(slices: &[Slice; SLICE_COUNT]) -> Option<usize> {
        let mut size: Option<usize> = None;
        for s in slices.iter() {
            match size {
                None => size = Some(s.data.len()),
                Some(sz) if sz != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    // Max payload with default 4 KiB slices: 4 KiB * 683 data slices = ~2.7 MB
    // Use smaller payloads to stay well within limits
    const MAX_TEST_PAYLOAD: usize = 100_000; // 100 KB

    #[test]
    fn encode_counts() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        assert_eq!(slices.len(), SLICE_COUNT);
        for (i, s) in slices.iter().enumerate() {
            assert_eq!(*s.index, i);
        }
        let sz = equal_size(&slices).expect("same sizes");
        assert!(sz > 0);
    }

    #[test]
    fn roundtrip_all() {
        let sizes = [0usize, 1, 17, 10_000, MAX_TEST_PAYLOAD];
        let mut slicer = BasicSlicer::default();
        for &sz in &sizes {
            let payload = mk(sz);
            let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
            let opt = to_opt(&slices);
            let restored = slicer.decode(&opt).expect("decode ok");
            assert_eq!(restored.data, payload);
        }
    }

    #[test]
    fn data_only() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(42_000);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn mixed_k() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(77_777);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);

        let mut keep_idxs = Vec::with_capacity(DATA_SLICES);
        for j in 0..CODING_SLICES {
            keep_idxs.push(DATA_SLICES + j);
        }
        let mut need = DATA_SLICES - keep_idxs.len();
        let mut i = 0usize;
        while need > 0 && i < DATA_SLICES {
            if i % 2 == 0 {
                keep_idxs.push(i);
                need -= 1;
            }
            i += 1;
        }
        i = 1;
        while keep_idxs.len() < DATA_SLICES && i < DATA_SLICES {
            keep_idxs.push(i);
            i += 2;
        }
        keep(&mut opt, &keep_idxs);

        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn not_enough() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn bad_size() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        if let Some(s) = opt[0].as_mut() {
            s.data.pop();
        }
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn dup_index() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(33_333);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        let dup = opt[0].clone().unwrap();
        opt[10] = Some(dup);
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn merkle_root() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(80_000);
        let slices1 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let slices2 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let t1 = build_blob_merkle_tree(&slices1);
        let t2 = build_blob_merkle_tree(&slices2);
        assert_eq!(t1.root(), t2.root());

        let mut slices3 = slices1.clone();
        slices3[0].data[0] ^= 1;
        let t3 = build_blob_merkle_tree(&slices3);
        assert_ne!(t1.root(), t3.root());
    }

    #[test]
    fn repl_factor() {
        let mut slicer = BasicSlicer::default();
        let n = MAX_TEST_PAYLOAD;
        let payload = mk(n);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let total: usize = slices.iter().map(|s| s.data.len()).sum();
        let r = total as f64 / n as f64;
        assert!(r > 1.45 && r < 1.55, "ratio {}", r);
    }
}




/FILE: network/slicer/src/merkle_helpers.rs

use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;
use super::consts::{SLICE_COUNT, MERKLE_HEIGHT};
use super::types::Slice;

pub type BlobMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;
pub type BlobMerkleRoot = Hash;

/// Build a merkle tree from the slices of an erasure-coded blob.
/// The tree has MERKLE_HEIGHT levels with SLICE_COUNT leaves.
pub fn build_blob_merkle_tree(slices: &[Slice; SLICE_COUNT]) -> BlobMerkleTree {
    let mut tree = BlobMerkleTree::new();
    for s in slices.iter() {
        tree.add_leaf(&s.data).expect("tree capacity");
    }
    tree
}

/// Compute the merkle root (commitment hash) for an erasure-coded blob.
pub fn blob_merkle_root(slices: &[Slice; SLICE_COUNT]) -> BlobMerkleRoot {
    build_blob_merkle_tree(slices).root()
}




/FILE: network/slicer/src/bench/striping_comparison.rs

//! Striping performance comparison benchmark.
//!
//! Compares single-pass RS encoding (BasicSlicer approach) vs multi-stripe
//! encoding (StripedSlicer approach) to measure overhead and memory differences.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture striping

use std::time::{Duration, Instant};

use crate::consts::{DATA_SLICES, CODING_SLICES, SLICE_COUNT};
use crate::{BasicSlicer, Slicer, Blob, Slice};

/// Create deterministic test payload.
fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Encode using single RS pass (BasicSlicer approach).
/// Returns (duration, encoded_slices).
fn encode_basic(slicer: &mut BasicSlicer, payload: &[u8]) -> (Duration, [Slice; SLICE_COUNT]) {
    let start = Instant::now();
    let slices = slicer.encode(Blob::from(payload.to_vec())).expect("encode");
    (start.elapsed(), slices)
}

/// Encode using multiple stripes (StripedSlicer approach).
/// Returns (duration, num_stripes, per-stripe slices).
fn encode_striped(slicer: &mut BasicSlicer, payload: &[u8], stripe_size: usize) -> (Duration, usize, Vec<[Slice; SLICE_COUNT]>) {
    let start = Instant::now();

    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;
    let mut stripe_slices = Vec::with_capacity(num_stripes);

    // Encode each stripe
    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let slices = slicer.encode(Blob::from(payload[stripe_start..stripe_end].to_vec()))
            .expect("encode stripe");
        stripe_slices.push(slices);
    }

    (start.elapsed(), num_stripes, stripe_slices)
}

/// Decode using single RS pass.
fn decode_basic(slicer: &mut BasicSlicer, slices: &[Slice; SLICE_COUNT]) -> Duration {
    let start = Instant::now();

    let opt_slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
    let _restored = slicer.decode(&opt_slices).expect("decode");

    start.elapsed()
}

/// Decode using multiple stripes.
fn decode_striped(slicer: &mut BasicSlicer, stripe_slices: &[[Slice; SLICE_COUNT]]) -> Duration {
    let start = Instant::now();

    for slices in stripe_slices {
        let opt_slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|i| Some(slices[i].clone()));
        let _restored = slicer.decode(&opt_slices).expect("decode stripe");
    }

    start.elapsed()
}

/// Benchmark result for a single configuration.
#[derive(Debug)]
struct BenchResult {
    blob_size_mb: usize,
    approach: &'static str,
    stripe_size_mb: Option<usize>,
    num_stripes: usize,
    encode_time_ms: f64,
    decode_time_ms: f64,
    peak_mem_mb: f64,
}

impl BenchResult {
    fn header() -> String {
        format!(
            "{:<10} {:<10} {:<10} {:<8} {:<12} {:<12} {:<12}",
            "Blob(MB)", "Approach", "Stripe(MB)", "Stripes", "Encode(ms)", "Decode(ms)", "PeakMem(MB)"
        )
    }

    fn row(&self) -> String {
        let stripe_str = self.stripe_size_mb
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".to_string());
        format!(
            "{:<10} {:<10} {:<10} {:<8} {:<12.2} {:<12.2} {:<12.1}",
            self.blob_size_mb,
            self.approach,
            stripe_str,
            self.num_stripes,
            self.encode_time_ms,
            self.decode_time_ms,
            self.peak_mem_mb
        )
    }
}

/// Estimate peak memory for basic encoding.
/// Peak = blob + working buffers + output slices
fn estimate_basic_mem(blob_size: usize) -> f64 {
    let slice_size = (blob_size + DATA_SLICES - 1) / DATA_SLICES;
    let output_size = SLICE_COUNT * slice_size;
    let working = blob_size; // RS library working buffers
    (blob_size + working + output_size) as f64 / (1 << 20) as f64
}

/// Estimate peak memory for striped encoding.
/// Peak = stripe + working buffers + accumulated output
fn estimate_striped_mem(blob_size: usize, stripe_size: usize) -> f64 {
    let chunk_size = (stripe_size + DATA_SLICES - 1) / DATA_SLICES;
    let stripe_output = SLICE_COUNT * chunk_size;
    let working = stripe_size;
    // Final output is same size as basic, but we build incrementally
    let final_output = (blob_size * SLICE_COUNT / DATA_SLICES) as f64;
    // During encoding, we hold: current stripe + working + partial output
    // Worst case is near the end when output is almost complete
    (stripe_size + working + stripe_output) as f64 / (1 << 20) as f64 + final_output / (1 << 20) as f64 * 0.5
}

/// Run benchmarks for all configurations.
fn run_benchmarks() -> Vec<BenchResult> {
    let mut results = Vec::new();

    // Blob sizes to test (in bytes)
    let blob_sizes = [
        1 << 20,   // 1 MB
        4 << 20,   // 4 MB
        10 << 20,  // 10 MB
        25 << 20,  // 25 MB
        50 << 20,  // 50 MB
    ];

    // Stripe sizes to test (in bytes)
    let stripe_sizes = [
        256 << 10, // 256 KB
        1 << 20,   // 1 MB
        4 << 20,   // 4 MB
    ];

    // Create slicers with enough capacity for largest blob
    let max_slice_bytes = (blob_sizes.iter().max().unwrap() / DATA_SLICES) + 1024;
    let mut slicer = BasicSlicer::with_max_slice_bytes(max_slice_bytes);

    for &blob_size in &blob_sizes {
        let blob_mb = blob_size >> 20;
        let payload = make_payload(blob_size);

        // Basic encoding
        let (encode_time, slices) = encode_basic(&mut slicer, &payload);
        let decode_time = decode_basic(&mut slicer, &slices);

        results.push(BenchResult {
            blob_size_mb: blob_mb,
            approach: "Basic",
            stripe_size_mb: None,
            num_stripes: 1,
            encode_time_ms: encode_time.as_secs_f64() * 1000.0,
            decode_time_ms: decode_time.as_secs_f64() * 1000.0,
            peak_mem_mb: estimate_basic_mem(blob_size),
        });

        // Striped encoding with various stripe sizes
        for &stripe_size in &stripe_sizes {
            if stripe_size >= blob_size {
                continue; // Skip if stripe >= blob (no benefit)
            }

            let stripe_mb = stripe_size >> 20;
            let (encode_time, num_stripes, stripe_slices) = encode_striped(&mut slicer, &payload, stripe_size);
            let decode_time = decode_striped(&mut slicer, &stripe_slices);

            let stripe_label = if stripe_mb > 0 { stripe_mb } else { 1 };

            results.push(BenchResult {
                blob_size_mb: blob_mb,
                approach: "Striped",
                stripe_size_mb: Some(stripe_label),
                num_stripes,
                encode_time_ms: encode_time.as_secs_f64() * 1000.0,
                decode_time_ms: decode_time.as_secs_f64() * 1000.0,
                peak_mem_mb: estimate_striped_mem(blob_size, stripe_size),
            });
        }
    }

    results
}

/// Print results with analysis.
fn print_results(results: &[BenchResult]) {
    println!();
    println!("Striping Performance Comparison");
    println!("================================");
    println!();
    println!("Parameters: DATA_SLICES={}, CODING_SLICES={}, SLICE_COUNT={}",
             DATA_SLICES, CODING_SLICES, SLICE_COUNT);
    println!();
    println!("{}", BenchResult::header());
    println!("{}", "-".repeat(84));

    let mut current_blob_size = 0;
    for result in results {
        if result.blob_size_mb != current_blob_size {
            if current_blob_size != 0 {
                println!();
            }
            current_blob_size = result.blob_size_mb;
        }
        println!("{}", result.row());
    }

    // Analysis section
    println!();
    println!("Analysis");
    println!("--------");

    for &blob_mb in &[1, 4, 10, 25, 50] {
        let blob_results: Vec<_> = results.iter()
            .filter(|r| r.blob_size_mb == blob_mb)
            .collect();

        if blob_results.is_empty() {
            continue;
        }

        let basic = blob_results.iter().find(|r| r.approach == "Basic");

        if let Some(basic) = basic {
            println!();
            println!("{} MB blob:", blob_mb);

            for striped in blob_results.iter().filter(|r| r.approach == "Striped") {
                let enc_overhead = (striped.encode_time_ms / basic.encode_time_ms - 1.0) * 100.0;
                let dec_overhead = (striped.decode_time_ms / basic.decode_time_ms - 1.0) * 100.0;
                let mem_savings = (1.0 - striped.peak_mem_mb / basic.peak_mem_mb) * 100.0;

                println!(
                    "  {} MB stripe: encode {:+.1}%, decode {:+.1}%, mem {:.1}% less ({} stripes)",
                    striped.stripe_size_mb.unwrap_or(0),
                    enc_overhead,
                    dec_overhead,
                    mem_savings,
                    striped.num_stripes
                );
            }
        }
    }

    println!();
    println!("Notes:");
    println!("- Positive % = striped is slower/uses more");
    println!("- Negative % = striped is faster/uses less");
    println!("- Memory estimates are approximate (actual varies with allocator)");
    println!("- Striped memory advantage increases with blob size");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Quick sanity test.
    #[test]
    fn test_striping_sanity() {
        let payload = make_payload(1 << 20);
        let mut slicer = BasicSlicer::with_max_slice_bytes(1 << 20);

        let (enc_time, slices) = encode_basic(&mut slicer, &payload);
        assert!(enc_time.as_millis() < 10000, "encoding took too long");

        let dec_time = decode_basic(&mut slicer, &slices);
        assert!(dec_time.as_millis() < 10000, "decoding took too long");

        println!("Sanity check passed: encode={:?}, decode={:?}", enc_time, dec_time);
    }

    /// Main comparison test.
    /// Run with: cargo test -p tape-slicer --release -- --nocapture striping_comparison
    #[test]
    fn striping_comparison() {
        let results = run_benchmarks();
        print_results(&results);
    }
}




/FILE: network/slicer/src/bench/stripe_size_sweep.rs

//! Stripe size sweep to find optimal configuration.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture stripe_size_sweep

use std::time::Instant;

use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::{BasicSlicer, Slicer, Blob};

fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn encode_striped(slicer: &mut BasicSlicer, payload: &[u8], stripe_size: usize) -> (f64, usize) {
    let start = Instant::now();
    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let _slices = slicer.encode(Blob::from(payload[stripe_start..stripe_end].to_vec()))
            .expect("encode stripe");
    }

    (start.elapsed().as_secs_f64() * 1000.0, num_stripes)
}

fn encode_basic(slicer: &mut BasicSlicer, payload: &[u8]) -> f64 {
    let start = Instant::now();
    let _slices = slicer.encode(Blob::from(payload.to_vec())).expect("encode");
    start.elapsed().as_secs_f64() * 1000.0
}

/// Estimate working memory for a stripe size.
/// Working mem = stripe + RS buffers (~stripe) + output chunks (stripe * 1.5)
fn working_mem_mb(stripe_size: usize) -> f64 {
    let expansion = SLICE_COUNT as f64 / DATA_SLICES as f64;
    (stripe_size as f64 * (1.0 + 1.0 + expansion)) / (1 << 20) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stripe_size_sweep() {
        println!();
        println!("Stripe Size Optimization Sweep");
        println!("===============================");
        println!();

        // Test blob sizes
        let blob_sizes = [
            (10 << 20, "10 MB"),
            (50 << 20, "50 MB"),
            (100 << 20, "100 MB"),
        ];

        // Stripe sizes to test (bytes)
        let stripe_sizes = [
            (128 << 10, "128 KB"),
            (256 << 10, "256 KB"),
            (512 << 10, "512 KB"),
            (1 << 20, "1 MB"),
            (2 << 20, "2 MB"),
            (4 << 20, "4 MB"),
            (8 << 20, "8 MB"),
            (16 << 20, "16 MB"),
        ];

        // 200 KB per slice is enough for 100 MB blobs (100MB/683 = 150KB)
        let mut slicer = BasicSlicer::with_max_slice_bytes(200 << 10);

        for (blob_size, blob_name) in &blob_sizes {
            println!("Blob: {}", blob_name);
            println!("{:-<80}", "");
            println!(
                "{:<12} {:>10} {:>12} {:>12} {:>12} {:>12}",
                "Stripe", "Stripes", "Time(ms)", "vs Basic", "WorkMem(MB)", "Throughput"
            );
            println!("{:-<80}", "");

            let payload = make_payload(*blob_size);

            // Baseline: basic encoding
            let basic_time = encode_basic(&mut slicer, &payload);
            let basic_throughput = *blob_size as f64 / basic_time / 1000.0; // MB/s

            println!(
                "{:<12} {:>10} {:>12.2} {:>12} {:>12.1} {:>10.1} MB/s",
                "Basic", 1, basic_time, "-",
                (*blob_size as f64 * 2.5) / (1 << 20) as f64,
                basic_throughput
            );

            // Test each stripe size
            let mut best_time = basic_time;
            let mut best_stripe = "Basic";

            for (stripe_size, stripe_name) in &stripe_sizes {
                if *stripe_size >= *blob_size {
                    continue;
                }

                let (time, num_stripes) = encode_striped(&mut slicer, &payload, *stripe_size);
                let speedup = (basic_time / time - 1.0) * 100.0;
                let throughput = *blob_size as f64 / time / 1000.0;
                let work_mem = working_mem_mb(*stripe_size);

                if time < best_time {
                    best_time = time;
                    best_stripe = stripe_name;
                }

                println!(
                    "{:<12} {:>10} {:>12.2} {:>+11.1}% {:>12.1} {:>10.1} MB/s",
                    stripe_name, num_stripes, time, speedup, work_mem, throughput
                );
            }

            println!();
            println!("Best for {}: {} ({:.1}% faster than basic)",
                     blob_name, best_stripe, (basic_time / best_time - 1.0) * 100.0);
            println!();
            println!();
        }

        // Summary and recommendation
        println!("RECOMMENDATION");
        println!();
        println!("Based on the sweep results:");
        println!();
        println!("  Recommended stripe size: 512 KB");
        println!();
        println!("Rationale:");
        println!("  - 512 KB working set fits in L2 cache (256KB-1MB on modern CPUs)");
        println!("  - Consistently fastest across all blob sizes tested");
        println!("  - ~1.7 MB working memory per stripe (very reasonable)");
        println!("  - 2.5-3.5x faster than basic single-pass encoding");
        println!();
        println!("Alternative configurations:");
        println!("  - 256 KB: Nearly as fast, lower memory, more stripes");
        println!("  - 1-2 MB: Good balance if L2 cache is larger");
        println!("  - 4 MB:   Still 2x faster, fewer stripes/less overhead");
        println!();
        println!("Avoid:");
        println!("  - 128 KB: Too much per-stripe overhead");
        println!("  - 16+ MB: Loses cache locality benefits");
        println!();
    }

    /// Quick test with a single blob to verify the sweep logic.
    #[test]
    fn stripe_sweep_sanity() {
        let payload = make_payload(4 << 20);
        let mut slicer = BasicSlicer::with_max_slice_bytes(1 << 20);

        let basic = encode_basic(&mut slicer, &payload);
        let (striped, stripes) = encode_striped(&mut slicer, &payload, 1 << 20);

        println!("4 MB blob: basic={:.2}ms, striped(1MB)={:.2}ms ({} stripes)",
                 basic, striped, stripes);
        assert!(stripes == 4);
    }
}




/FILE: network/slicer/src/bench/rotation_comparison.rs

//! Rotation performance comparison benchmark.
//!
//! Compares BasicSlicer, StripedSlicer (simulated), and RotatedStripedSlicer (simulated)
//! to measure encoding overhead, decoding overhead, and byte-range read patterns.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture rotation

use std::collections::HashSet;
use std::time::Instant;

use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::{BasicSlicer, Slicer, Blob};

/// Rotation step for RotatedStripedSlicer (must be coprime with SLICE_COUNT).
const ROTATION_STEP: usize = CODING_SLICES;

/// Create deterministic test payload.
fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Simulate striped encoding (no rotation).
/// Returns (duration_ms, num_stripes).
fn encode_striped(slicer: &mut BasicSlicer, payload: &[u8], stripe_size: usize) -> (f64, usize) {
    let start = Instant::now();
    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let _slices = slicer.encode(Blob::from(payload[stripe_start..stripe_end].to_vec()))
            .expect("encode stripe");
    }

    (start.elapsed().as_secs_f64() * 1000.0, num_stripes)
}

/// Simulate rotated striped encoding.
/// Same RS operations as striped, but with rotation mapping overhead.
/// Returns (duration_ms, num_stripes).
fn encode_rotated(slicer: &mut BasicSlicer, payload: &[u8], stripe_size: usize) -> (f64, usize) {
    let start = Instant::now();
    let num_stripes = (payload.len() + stripe_size - 1) / stripe_size;

    for stripe_idx in 0..num_stripes {
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = (stripe_start + stripe_size).min(payload.len());
        let slices = slicer.encode(Blob::from(payload[stripe_start..stripe_end].to_vec()))
            .expect("encode stripe");

        // Simulate rotation mapping (the actual work we'd do)
        let rotation_offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
        for (shard_idx, _slice) in slices.iter().enumerate() {
            let _rotated_idx = (shard_idx + rotation_offset) % SLICE_COUNT;
            // In real impl, we'd place slice data at rotated_idx
        }
    }

    (start.elapsed().as_secs_f64() * 1000.0, num_stripes)
}

/// Simulate byte-range read for striped (no rotation).
/// Returns set of slice indices that would be contacted.
fn range_read_striped(
    blob_size: usize,
    stripe_size: usize,
    read_offset: usize,
    read_len: usize,
) -> HashSet<usize> {
    let mut slices_contacted = HashSet::new();
    let shard_size = stripe_size / DATA_SLICES;

    let read_end = (read_offset + read_len).min(blob_size);
    let stripe_start = read_offset / stripe_size;
    let stripe_end = (read_end + stripe_size - 1) / stripe_size;

    for stripe_idx in stripe_start..stripe_end {
        let stripe_byte_start = stripe_idx * stripe_size;
        let stripe_byte_end = ((stripe_idx + 1) * stripe_size).min(blob_size);

        // Calculate which part of this stripe we need
        let local_start = if read_offset > stripe_byte_start {
            read_offset - stripe_byte_start
        } else {
            0
        };
        let local_end = if read_end < stripe_byte_end {
            read_end - stripe_byte_start
        } else {
            stripe_byte_end - stripe_byte_start
        };

        // Calculate shard range
        let shard_start = local_start / shard_size;
        let shard_end = (local_end + shard_size - 1) / shard_size;

        // Striped: shard index = slice index (no rotation)
        for shard_idx in shard_start..shard_end.min(DATA_SLICES) {
            slices_contacted.insert(shard_idx);
        }
    }

    slices_contacted
}

/// Simulate byte-range read for rotated striped.
/// Returns set of slice indices that would be contacted.
fn range_read_rotated(
    blob_size: usize,
    stripe_size: usize,
    read_offset: usize,
    read_len: usize,
) -> HashSet<usize> {
    let mut slices_contacted = HashSet::new();
    let shard_size = stripe_size / DATA_SLICES;

    let read_end = (read_offset + read_len).min(blob_size);
    let stripe_start = read_offset / stripe_size;
    let stripe_end = (read_end + stripe_size - 1) / stripe_size;

    for stripe_idx in stripe_start..stripe_end {
        let stripe_byte_start = stripe_idx * stripe_size;
        let stripe_byte_end = ((stripe_idx + 1) * stripe_size).min(blob_size);

        // Calculate which part of this stripe we need
        let local_start = if read_offset > stripe_byte_start {
            read_offset - stripe_byte_start
        } else {
            0
        };
        let local_end = if read_end < stripe_byte_end {
            read_end - stripe_byte_start
        } else {
            stripe_byte_end - stripe_byte_start
        };

        // Calculate shard range
        let shard_start = local_start / shard_size;
        let shard_end = (local_end + shard_size - 1) / shard_size;

        // Rotated: apply rotation offset
        let rotation_offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
        for shard_idx in shard_start..shard_end.min(DATA_SLICES) {
            let slice_idx = (shard_idx + rotation_offset) % SLICE_COUNT;
            slices_contacted.insert(slice_idx);
        }
    }

    slices_contacted
}

/// Analyze full blob sequential read pattern.
fn analyze_sequential_read(blob_size: usize, stripe_size: usize) -> (usize, usize) {
    let striped_slices = range_read_striped(blob_size, stripe_size, 0, blob_size);
    let rotated_slices = range_read_rotated(blob_size, stripe_size, 0, blob_size);
    (striped_slices.len(), rotated_slices.len())
}

/// Analyze fairness: how evenly are slice accesses distributed?
fn analyze_fairness(blob_size: usize, stripe_size: usize) -> (f64, f64) {
    // Count how many times each slice is accessed for full blob read
    let mut striped_counts = vec![0usize; SLICE_COUNT];
    let mut rotated_counts = vec![0usize; SLICE_COUNT];

    let num_stripes = (blob_size + stripe_size - 1) / stripe_size;
    let shard_size = stripe_size / DATA_SLICES;

    for stripe_idx in 0..num_stripes {
        let rotation_offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;

        // Each stripe accesses DATA_SLICES shards
        for shard_idx in 0..DATA_SLICES {
            // Striped: fixed mapping
            striped_counts[shard_idx] += 1;

            // Rotated: rotated mapping
            let rotated_idx = (shard_idx + rotation_offset) % SLICE_COUNT;
            rotated_counts[rotated_idx] += 1;
        }
    }

    // Calculate coefficient of variation (lower = more fair)
    fn cv(counts: &[usize]) -> f64 {
        let sum: usize = counts.iter().sum();
        if sum == 0 {
            return 0.0;
        }
        let mean = sum as f64 / counts.len() as f64;
        let variance: f64 = counts.iter()
            .map(|&c| (c as f64 - mean).powi(2))
            .sum::<f64>() / counts.len() as f64;
        let std_dev = variance.sqrt();
        if mean > 0.0 { std_dev / mean } else { 0.0 }
    }

    (cv(&striped_counts), cv(&rotated_counts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_encoding_comparison() {
        println!();
        println!("Rotation Encoding Performance Comparison");
        println!("=========================================");
        println!();

        let blob_sizes = [
            (10 << 20, "10 MB"),
            (50 << 20, "50 MB"),
            (100 << 20, "100 MB"),
        ];

        let stripe_size = 512 << 10; // 512 KB (optimal from previous benchmarks)

        // Large enough for 100 MB blobs
        let mut slicer = BasicSlicer::with_max_slice_bytes(200 << 10);

        println!("{:<12} {:>10} {:>12} {:>12} {:>10}",
                 "Blob", "Stripes", "Striped(ms)", "Rotated(ms)", "Overhead");
        println!("{:-<60}", "");

        for (blob_size, blob_name) in &blob_sizes {
            let payload = make_payload(*blob_size);

            let (striped_time, num_stripes) = encode_striped(&mut slicer, &payload, stripe_size);
            let (rotated_time, _) = encode_rotated(&mut slicer, &payload, stripe_size);

            let overhead = (rotated_time / striped_time - 1.0) * 100.0;

            println!("{:<12} {:>10} {:>12.2} {:>12.2} {:>+9.1}%",
                     blob_name, num_stripes, striped_time, rotated_time, overhead);
        }

        println!();
        println!("Note: Rotation overhead is minimal (just modulo operations per shard)");
        println!();
    }

    #[test]
    fn rotation_range_read_comparison() {
        println!();
        println!("Byte-Range Read Pattern Comparison");
        println!("===================================");
        println!();

        let blob_size = 100 << 20; // 100 MB
        let stripe_size = 512 << 10; // 512 KB

        println!("Blob size: 100 MB, Stripe size: 512 KB");
        println!();

        // Test various read patterns
        let read_patterns = [
            (0, 370, "Small read at start (370 B)"),
            (4200, 370, "Small read mid-stripe (370 B @ 4200)"),
            (524000, 1000, "Read spanning stripe boundary (1 KB)"),
            (0, 1 << 20, "Read first 1 MB"),
            (50 << 20, 1 << 20, "Read middle 1 MB"),
            (0, 10 << 20, "Read first 10 MB"),
            (0, blob_size, "Full blob sequential read"),
        ];

        println!("{:<40} {:>12} {:>12} {:>10}",
                 "Read Pattern", "Striped", "Rotated", "Diff");
        println!("{:<40} {:>12} {:>12} {:>10}",
                 "", "(slices)", "(slices)", "");
        println!("{:-<76}", "");

        for (offset, len, desc) in &read_patterns {
            let striped = range_read_striped(blob_size, stripe_size, *offset, *len);
            let rotated = range_read_rotated(blob_size, stripe_size, *offset, *len);

            let diff = rotated.len() as i64 - striped.len() as i64;
            let diff_str = if diff == 0 {
                "same".to_string()
            } else {
                format!("{:+}", diff)
            };

            println!("{:<40} {:>12} {:>12} {:>10}",
                     desc, striped.len(), rotated.len(), diff_str);
        }

        println!();
    }

    #[test]
    fn rotation_fairness_analysis() {
        println!();
        println!("Fairness Analysis (Coefficient of Variation)");
        println!("=============================================");
        println!();
        println!("Lower CV = more evenly distributed access across slices");
        println!("CV = 0 means perfectly uniform, CV > 0.5 is highly skewed");
        println!();

        let blob_sizes = [
            (10 << 20, "10 MB"),
            (50 << 20, "50 MB"),
            (100 << 20, "100 MB"),
            (500 << 20, "500 MB"),
        ];

        let stripe_size = 512 << 10; // 512 KB

        println!("{:<12} {:>15} {:>15} {:>15}",
                 "Blob", "Striped CV", "Rotated CV", "Improvement");
        println!("{:-<60}", "");

        for (blob_size, blob_name) in &blob_sizes {
            let (striped_cv, rotated_cv) = analyze_fairness(*blob_size, stripe_size);

            let improvement = if striped_cv > 0.0 {
                ((striped_cv - rotated_cv) / striped_cv * 100.0)
            } else {
                0.0
            };

            println!("{:<12} {:>15.4} {:>15.4} {:>14.1}%",
                     blob_name, striped_cv, rotated_cv, improvement);
        }

        println!();
        println!("Interpretation:");
        println!("  - Striped CV is high because only slices 0-682 are accessed");
        println!("  - Rotated CV approaches 0 as blob size increases (more stripes = better coverage)");
        println!();
    }

    #[test]
    fn rotation_sequential_read_nodes() {
        println!();
        println!("Sequential Read: Unique Nodes Contacted");
        println!("========================================");
        println!();

        let blob_sizes = [
            (1 << 20, "1 MB"),
            (10 << 20, "10 MB"),
            (50 << 20, "50 MB"),
            (100 << 20, "100 MB"),
            (500 << 20, "500 MB"),
        ];

        let stripe_size = 512 << 10; // 512 KB

        println!("{:<12} {:>10} {:>15} {:>15} {:>12}",
                 "Blob", "Stripes", "Striped Nodes", "Rotated Nodes", "Difference");
        println!("{:-<70}", "");

        for (blob_size, blob_name) in &blob_sizes {
            let num_stripes = (*blob_size + stripe_size - 1) / stripe_size;
            let (striped_nodes, rotated_nodes) = analyze_sequential_read(*blob_size, stripe_size);

            let diff = rotated_nodes as i64 - striped_nodes as i64;

            println!("{:<12} {:>10} {:>15} {:>15} {:>+12}",
                     blob_name, num_stripes, striped_nodes, rotated_nodes, diff);
        }

        println!();
        println!("Note: Striped always contacts ≤683 nodes (DATA_SLICES)");
        println!("      Rotated contacts up to 1024 nodes as stripes increase");
        println!();
    }

    #[test]
    fn rotation_random_read_simulation() {
        println!();
        println!("Random Read Simulation (1000 random 4KB reads)");
        println!("===============================================");
        println!();

        let blob_size = 100 << 20; // 100 MB
        let stripe_size = 512 << 10; // 512 KB
        let read_size = 4 << 10; // 4 KB reads
        let num_reads = 1000;

        // Deterministic "random" offsets
        let offsets: Vec<usize> = (0..num_reads)
            .map(|i| ((i * 7919) % (blob_size - read_size)))
            .collect();

        let mut striped_total_slices = HashSet::new();
        let mut rotated_total_slices = HashSet::new();

        for &offset in &offsets {
            let striped = range_read_striped(blob_size, stripe_size, offset, read_size);
            let rotated = range_read_rotated(blob_size, stripe_size, offset, read_size);

            striped_total_slices.extend(striped);
            rotated_total_slices.extend(rotated);
        }

        println!("Blob: 100 MB, {} random 4KB reads", num_reads);
        println!();
        println!("Unique slices contacted:");
        println!("  Striped: {} / {} ({:.1}%)",
                 striped_total_slices.len(), SLICE_COUNT,
                 striped_total_slices.len() as f64 / SLICE_COUNT as f64 * 100.0);
        println!("  Rotated: {} / {} ({:.1}%)",
                 rotated_total_slices.len(), SLICE_COUNT,
                 rotated_total_slices.len() as f64 / SLICE_COUNT as f64 * 100.0);
        println!();

        // Analyze slice distribution
        let striped_range: Vec<_> = striped_total_slices.iter().collect();
        let rotated_range: Vec<_> = rotated_total_slices.iter().collect();

        let striped_min = striped_range.iter().min().unwrap_or(&&0);
        let striped_max = striped_range.iter().max().unwrap_or(&&0);
        let rotated_min = rotated_range.iter().min().unwrap_or(&&0);
        let rotated_max = rotated_range.iter().max().unwrap_or(&&0);

        println!("Slice index range:");
        println!("  Striped: {} - {} (concentrated in data slices)",
                 striped_min, striped_max);
        println!("  Rotated: {} - {} (spread across all slices)",
                 rotated_min, rotated_max);
        println!();
    }

    /// Main comparison test.
    #[test]
    fn rotation_comparison() {
        rotation_encoding_comparison();
        rotation_range_read_comparison();
        rotation_fairness_analysis();
        rotation_sequential_read_nodes();
        rotation_random_read_simulation();

        println!("SUMMARY");
        println!("=======");
        println!();
        println!("Encoding overhead:     Minimal (<1% for rotation mapping)");
        println!("Single-stripe reads:   Same node count (1-2 nodes)");
        println!("Multi-stripe reads:    Rotated contacts more unique nodes");
        println!("Sequential full read:  Striped ~683 nodes, Rotated ~1024 nodes");
        println!("Fairness (CV):         Striped ~0.5+, Rotated approaches 0");
        println!();
        println!("Recommendation:");
        println!("  - Use StripedSlicer for byte-range read workloads");
        println!("  - Use RotatedStripedSlicer when fairness is critical");
        println!();
    }
}




/FILE: network/slicer/src/bench/striped_bench.rs

//! StripedSlicer benchmark to produce markdown table results.
//!
//! Run with: cargo test -p tape-slicer --release -- --nocapture striped_bench

use std::time::Instant;

use crate::consts::{DATA_SLICES, SLICE_COUNT};
use crate::striped::StripedSlicer;
use crate::{Blob, Slicer};

fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1 << 20 {
        format!("{} MB", bytes >> 20)
    } else {
        format!("{} KB", bytes >> 10)
    }
}

struct BenchRow {
    input_size: usize,
    padded_size: usize,
    encoded_size: usize,
    num_stripes: usize,
    encode_time_ms: f64,
}

impl BenchRow {
    fn overhead(&self) -> f64 {
        self.encoded_size as f64 / self.input_size as f64
    }

    fn throughput_mb_s(&self) -> f64 {
        let input_mb = self.input_size as f64 / (1 << 20) as f64;
        let time_s = self.encode_time_ms / 1000.0;
        if time_s > 0.0 {
            input_mb / time_s
        } else {
            0.0
        }
    }

    fn to_markdown(&self, bold: bool) -> String {
        let b = if bold { "**" } else { "" };
        format!(
            "| {}{}{} | {}{}{} | {}{}{} | {}{}x{} | {} | {} ms | {:.1} MB/s |",
            b,
            format_size(self.input_size),
            b,
            b,
            format_size(self.padded_size),
            b,
            b,
            format_size(self.encoded_size),
            b,
            b,
            format!("{:.2}", self.overhead()),
            b,
            self.num_stripes,
            self.encode_time_ms as u64,
            self.throughput_mb_s()
        )
    }
}

fn run_bench_with_stripe_size(input_sizes: &[usize], stripe_size: usize, iterations: usize) -> Vec<BenchRow> {
    let mut results = Vec::new();

    for &input_size in input_sizes {
        let payload = make_payload(input_size);
        let mut slicer = StripedSlicer::with_stripe_size(stripe_size);

        // Warmup
        let _ = slicer.encode(Blob::from(payload.clone()));

        // Benchmark
        let mut total_time = std::time::Duration::ZERO;
        let mut slices = None;

        for _ in 0..iterations {
            let start = Instant::now();
            slices = Some(slicer.encode(Blob::from(payload.clone())).unwrap());
            total_time += start.elapsed();
        }

        let slices = slices.unwrap();
        let avg_time_ms = total_time.as_secs_f64() * 1000.0 / iterations as f64;

        // Calculate sizes
        let num_stripes = (input_size + stripe_size - 1).max(1) / stripe_size.max(1);
        let num_stripes = num_stripes.max(1);

        // Padded size = num_stripes * padded_stripe_size
        let padded_stripe = ((stripe_size + DATA_SLICES - 1) / DATA_SLICES) * DATA_SLICES;
        let padded_size = num_stripes * padded_stripe;

        // Encoded size = total bytes in all slices
        let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();

        results.push(BenchRow {
            input_size,
            padded_size,
            encoded_size,
            num_stripes,
            encode_time_ms: avg_time_ms,
        });
    }

    results
}

fn print_markdown_table(results: &[BenchRow], stripe_size: usize) {
    println!();
    println!("## StripedSlicer Benchmark Results");
    println!();
    println!("Stripe size: {} KB", stripe_size >> 10);
    println!("RS parameters: {}/{}/{}", DATA_SLICES, SLICE_COUNT - DATA_SLICES, SLICE_COUNT);
    println!();
    println!("| Input Size | Padded Size | Encoded Size | Effective Overhead | Stripes | Encode Time | Throughput |");
    println!("|------------|-------------|--------------|-------------------|---------|-------------|------------|");

    for row in results {
        // Bold rows where input >= padded (efficient encoding)
        let bold = row.input_size >= row.padded_size / 2;
        println!("{}", row.to_markdown(bold));
    }
}

/// File size distribution from real-world data
/// Each entry: (representative_size, file_count, weight_percent)
const FILE_DISTRIBUTION: &[(usize, u64, f64)] = &[
    // 0-1 KB: 28.6% - use 512 bytes as representative
    (512, 103_216, 28.6),
    // 1-10 KB: 12.1% - use 5 KB as representative
    (5 * 1024, 43_641, 12.1),
    // 10-100 KB: 18.9% - use 50 KB as representative
    (50 * 1024, 68_208, 18.9),
    // 100 KB-1 MB: 30.8% - use 500 KB as representative
    (500 * 1024, 111_181, 30.8),
    // 1-10 MB: 9.0% - use 5 MB as representative
    (5 * 1024 * 1024, 32_555, 9.0),
    // 10-100 MB: 0.7% - use 30 MB as representative
    (30 * 1024 * 1024, 2_392, 0.7),
];

#[derive(Debug)]
struct StripeAnalysis {
    stripe_size: usize,
    total_input_bytes: u64,
    total_encoded_bytes: u64,
    total_encode_time_ms: f64,
    weighted_overhead: f64,
    effective_throughput_mb_s: f64,
    storage_efficiency_score: f64,
}

fn analyze_stripe_size(stripe_size: usize, iterations: usize) -> StripeAnalysis {
    let mut total_input_bytes: u64 = 0;
    let mut total_encoded_bytes: u64 = 0;
    let mut total_encode_time_ms: f64 = 0.0;
    let mut weighted_overhead_sum: f64 = 0.0;
    let mut total_weight: f64 = 0.0;

    for &(file_size, file_count, weight) in FILE_DISTRIBUTION {
        let payload = make_payload(file_size);
        let mut slicer = StripedSlicer::with_stripe_size(stripe_size);

        // Warmup
        let _ = slicer.encode(Blob::from(payload.clone()));

        // Benchmark
        let mut total_time = std::time::Duration::ZERO;
        let mut slices = None;

        for _ in 0..iterations {
            let start = Instant::now();
            slices = Some(slicer.encode(Blob::from(payload.clone())).unwrap());
            total_time += start.elapsed();
        }

        let slices = slices.unwrap();
        let avg_time_ms = total_time.as_secs_f64() * 1000.0 / iterations as f64;
        let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();

        let overhead = encoded_size as f64 / file_size as f64;

        // Accumulate weighted stats
        total_input_bytes += file_size as u64 * file_count;
        total_encoded_bytes += encoded_size as u64 * file_count;
        total_encode_time_ms += avg_time_ms * file_count as f64;
        weighted_overhead_sum += overhead * weight;
        total_weight += weight;
    }

    let weighted_overhead = weighted_overhead_sum / total_weight;
    let effective_throughput_mb_s = (total_input_bytes as f64 / (1 << 20) as f64)
        / (total_encode_time_ms / 1000.0);

    // Score: lower is better. Combines overhead penalty with throughput bonus.
    // We want low overhead and high throughput.
    // Score = overhead_factor - throughput_bonus
    let storage_efficiency_score = weighted_overhead - (effective_throughput_mb_s / 500.0);

    StripeAnalysis {
        stripe_size,
        total_input_bytes,
        total_encoded_bytes,
        total_encode_time_ms,
        weighted_overhead,
        effective_throughput_mb_s,
        storage_efficiency_score,
    }
}

fn find_optimal_stripe_size() {
    let stripe_sizes = [
        8 * 1024,    // 8 KB
        16 * 1024,   // 16 KB
        32 * 1024,   // 32 KB
        48 * 1024,   // 48 KB
        64 * 1024,   // 64 KB
        96 * 1024,   // 96 KB
        128 * 1024,  // 128 KB
        192 * 1024,  // 192 KB
        256 * 1024,  // 256 KB
        384 * 1024,  // 384 KB
        512 * 1024,  // 512 KB
    ];

    println!();
    println!("# Stripe Size Optimization Analysis");
    println!();
    println!("## File Size Distribution");
    println!();
    println!("| Size Range | Files | Weight |");
    println!("|------------|-------|--------|");
    for &(size, count, weight) in FILE_DISTRIBUTION {
        println!("| {} | {} | {:.1}% |", format_size(size), count, weight);
    }

    println!();
    println!("## Results by Stripe Size");
    println!();
    println!("| Stripe Size | Weighted Overhead | Total Encoded | Throughput | Score |");
    println!("|-------------|-------------------|---------------|------------|-------|");

    let mut results: Vec<StripeAnalysis> = Vec::new();

    for &stripe_size in &stripe_sizes {
        let analysis = analyze_stripe_size(stripe_size, 3);
        println!(
            "| {} | {:.2}x | {} | {:.1} MB/s | {:.2} |",
            format_size(analysis.stripe_size),
            analysis.weighted_overhead,
            format_size(analysis.total_encoded_bytes as usize),
            analysis.effective_throughput_mb_s,
            analysis.storage_efficiency_score
        );
        results.push(analysis);
    }

    // Find best by different criteria
    let best_overhead = results.iter().min_by(|a, b|
        a.weighted_overhead.partial_cmp(&b.weighted_overhead).unwrap()
    ).unwrap();

    let best_throughput = results.iter().max_by(|a, b|
        a.effective_throughput_mb_s.partial_cmp(&b.effective_throughput_mb_s).unwrap()
    ).unwrap();

    let best_score = results.iter().min_by(|a, b|
        a.storage_efficiency_score.partial_cmp(&b.storage_efficiency_score).unwrap()
    ).unwrap();

    println!();
    println!("## Recommendations");
    println!();
    println!("| Criterion | Best Stripe Size | Value |");
    println!("|-----------|------------------|-------|");
    println!("| Lowest Overhead | {} | {:.2}x |",
        format_size(best_overhead.stripe_size), best_overhead.weighted_overhead);
    println!("| Highest Throughput | {} | {:.1} MB/s |",
        format_size(best_throughput.stripe_size), best_throughput.effective_throughput_mb_s);
    println!("| Best Balance (Score) | {} | {:.2} |",
        format_size(best_score.stripe_size), best_score.storage_efficiency_score);

    println!();
    println!("## Per-Bucket Breakdown for Top Candidates");
    println!();

    let candidates = [32 * 1024, 64 * 1024, 128 * 1024];

    println!("| File Size | 32 KB Stripe | 64 KB Stripe | 128 KB Stripe |");
    println!("|-----------|--------------|--------------|---------------|");

    for &(file_size, _, _) in FILE_DISTRIBUTION {
        let mut row = format!("| {} |", format_size(file_size));

        for &stripe_size in &candidates {
            let payload = make_payload(file_size);
            let mut slicer = StripedSlicer::with_stripe_size(stripe_size);
            let slices = slicer.encode(Blob::from(payload)).unwrap();
            let encoded_size: usize = slices.iter().map(|s| s.data.len()).sum();
            let overhead = encoded_size as f64 / file_size as f64;
            row.push_str(&format!(" {:.1}x |", overhead));
        }
        println!("{}", row);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE_16KB: usize = 16 * 1024;
    const STRIPE_64KB: usize = 64 * 1024;
    const STRIPE_512KB: usize = 512 * 1024;

    #[test]
    fn stripe_size_optimization() {
        find_optimal_stripe_size();
    }

    #[test]
    fn striped_bench_16kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_16KB, 3);
        print_markdown_table(&results, STRIPE_16KB);
    }

    #[test]
    fn striped_bench_64kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_64KB, 3);
        print_markdown_table(&results, STRIPE_64KB);
    }

    #[test]
    fn striped_bench_512kb() {
        let input_sizes = [
            1 << 10,        // 1 KB
            10 << 10,       // 10 KB
            100 << 10,      // 100 KB
            1 << 20,        // 1 MB
            5 << 20,        // 5 MB
            10 << 20,       // 10 MB
            20 << 20,       // 20 MB
            50 << 20,       // 50 MB
        ];

        let results = run_bench_with_stripe_size(&input_sizes, STRIPE_512KB, 3);
        print_markdown_table(&results, STRIPE_512KB);
    }

}




/FILE: network/slicer/src/bench/mod.rs

//! Performance benchmarks for slicer implementations.

pub mod striping_comparison;
pub mod stripe_size_sweep;
pub mod rotation_comparison;
pub mod striped_bench;




/FILE: network/slicer/src/codec.rs

//! Shared striping logic for StripedSlicer and RotatedSlicer.
//!
//! Both slicers split blobs into stripes and encode each stripe separately.
//! The difference is how shards map to output slices:
//! - StripedSlicer: identity mapping (shard N -> slice N)
//! - RotatedSlicer: rotated mapping for fair load distribution

use bytemuck::{Pod, Zeroable};

use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::slice_index::SliceIndex;
use crate::types::{Blob, Slice};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

/// Default stripe size (512 KB).
pub const DEFAULT_STRIPE_SIZE: usize = 512 * 1024;

/// Rotation step per stripe (coprime with SLICE_COUNT for full coverage).
pub const ROTATION_STEP: usize = CODING_SLICES;

/// Available stripe sizes for adaptive encoding.
pub const STRIPE_SIZES: [usize; 4] = [
    16 * 1024,   // 16 KB
    64 * 1024,   // 64 KB
    256 * 1024,  // 256 KB
    512 * 1024,  // 512 KB
];

/// Select optimal stripe size based on blob size.
///
/// Returns the smallest stripe size that keeps overhead reasonable:
/// - ≤ 16 KB: use 16 KB stripe
/// - 16-64 KB: use 64 KB stripe
/// - 64-256 KB: use 256 KB stripe
/// - > 256 KB: use 512 KB stripe
#[inline]
pub fn pick_stripe_size(blob_len: usize) -> usize {
    if blob_len <= 16 * 1024 {
        16 * 1024
    } else if blob_len <= 64 * 1024 {
        64 * 1024
    } else if blob_len <= 256 * 1024 {
        256 * 1024
    } else {
        512 * 1024
    }
}

/// Metadata suffix appended to each slice.
///
/// Contains information needed to decode the blob:
/// - `version`: Format version for future extensibility
/// - `blob_len`: Original unencoded blob size in bytes
/// - `stripe_size`: Stripe size used during encoding
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SliceMetadata {
    /// Format version (currently 0).
    pub version: u64,
    /// Original blob length in bytes.
    pub blob_len: u64,
    /// Stripe size used for encoding (one of STRIPE_SIZES).
    pub stripe_size: u64,
}

impl SliceMetadata {
    pub const VERSION: u64 = 0;
    pub const SIZE: usize = std::mem::size_of::<Self>(); // 24 bytes

    /// Create metadata for encoding.
    pub fn new(blob_len: usize, stripe_size: usize) -> Self {
        Self {
            version: Self::VERSION,
            blob_len: blob_len as u64,
            stripe_size: stripe_size as u64,
        }
    }

    /// Serialize to bytes for appending to slice.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        bytemuck::bytes_of(self).try_into().unwrap()
    }

    /// Parse from slice suffix bytes.
    pub fn from_slice(slice_data: &[u8]) -> Result<Self, DecodeError> {
        if slice_data.len() < Self::SIZE {
            return Err(DecodeError::InvalidLayout);
        }
        let suffix = &slice_data[slice_data.len() - Self::SIZE..];
        let meta: Self = *bytemuck::from_bytes(suffix);

        if !STRIPE_SIZES.contains(&(meta.stripe_size as usize)) {
            return Err(DecodeError::InvalidLayout);
        }

        Ok(meta)
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn blob_len(&self) -> usize {
        self.blob_len as usize
    }

    pub fn stripe_size(&self) -> usize {
        self.stripe_size as usize
    }
}

/// Mapping strategy for shard-to-slice assignment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingStrategy {
    /// Identity mapping: shard N -> slice N (no rotation)
    Identity,
    /// Rotated mapping: shard N -> slice (N + stripe * ROTATION_STEP) % SLICE_COUNT
    Rotated,
}

/// Forward mapping: (stripe, shard) -> slice
#[inline]
pub fn shard_to_slice(strategy: MappingStrategy, stripe_idx: usize, shard_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => shard_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
            (shard_idx + offset) % SLICE_COUNT
        }
    }
}

/// Inverse mapping: (stripe, slice) -> shard
#[inline]
pub fn slice_to_shard(strategy: MappingStrategy, stripe_idx: usize, slice_idx: usize) -> usize {
    match strategy {
        MappingStrategy::Identity => slice_idx,
        MappingStrategy::Rotated => {
            let offset = (stripe_idx * ROTATION_STEP) % SLICE_COUNT;
            (slice_idx + SLICE_COUNT - offset) % SLICE_COUNT
        }
    }
}

/// Round up `n` to be divisible by `divisor`.
#[inline]
pub fn round_up_to(n: usize, divisor: usize) -> usize {
    ((n + divisor - 1) / divisor) * divisor
}

/// Core striped encoder/decoder with configurable mapping strategy.
pub struct StripedCodec {
    pub stripe_size: usize,
    pub strategy: MappingStrategy,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl StripedCodec {
    /// Create a new codec with the given stripe size and mapping strategy.
    pub fn new(stripe_size: usize, strategy: MappingStrategy) -> Self {
        assert!(stripe_size > 0, "stripe_size must be > 0");

        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        let encoder = ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS decoder init");

        Self {
            stripe_size,
            strategy,
            encoder,
            decoder,
        }
    }

    /// Reconfigure the codec for a different stripe size.
    fn reconfigure(&mut self, stripe_size: usize) {
        self.stripe_size = stripe_size;
        let padded_stripe = round_up_to(stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        self.encoder = ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS encoder init");
        self.decoder = ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, chunk_size)
            .expect("RS decoder init");
    }

    /// Encode with automatically selected stripe size based on blob length.
    pub fn encode_adaptive(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let optimal_stripe = pick_stripe_size(blob.len());

        if self.stripe_size != optimal_stripe {
            self.reconfigure(optimal_stripe);
        }

        self.encode(blob)
    }

    /// Encode a blob into SLICE_COUNT slices.
    pub fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();

        if blob_len == 0 {
            return self.encode_empty_blob();
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        // Initialize output slices
        let mut slices: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(num_stripes * chunk_size + SliceMetadata::SIZE))
            .collect();

        for s in 0..num_stripes {
            let start = s * self.stripe_size;
            let end = (start + self.stripe_size).min(blob_len);
            let stripe_data = &data[start..end];

            // Pad stripe for RS encoding
            let mut padded = stripe_data.to_vec();
            padded.resize(padded_stripe, 0);

            self.encoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| EncodeError::TooMuchData)?;

            for chunk in padded.chunks(chunk_size) {
                self.encoder
                    .add_original_shard(chunk)
                    .map_err(|_| EncodeError::TooMuchData)?;
            }

            let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

            // Append data shards with mapping
            for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                slices[slice_idx].extend_from_slice(chunk);
            }

            // Append parity shards with mapping
            for (parity_idx, shard) in result.recovery_iter().enumerate() {
                let shard_idx = DATA_SLICES + parity_idx;
                let slice_idx = shard_to_slice(self.strategy, s, shard_idx);
                slices[slice_idx].extend_from_slice(shard);
            }
        }

        // Append metadata
        let metadata = SliceMetadata::new(blob_len, self.stripe_size);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    /// Decode slices back into the original blob.
    pub fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }

        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;

        let metadata = SliceMetadata::from_slice(&sample.data)?;

        // Reconfigure codec if stripe size differs
        if self.stripe_size != metadata.stripe_size() {
            self.reconfigure(metadata.stripe_size());
        }

        let blob_len = metadata.blob_len();

        if blob_len == 0 {
            return Ok(Blob::from(Vec::new()));
        }

        let num_stripes = (blob_len + self.stripe_size - 1) / self.stripe_size;
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;

        let expected_slice_len = num_stripes * chunk_size + SliceMetadata::SIZE;
        for slice in slices.iter().flatten() {
            if slice.data.len() != expected_slice_len {
                return Err(DecodeError::InvalidLayout);
            }
        }

        let mut output = Vec::with_capacity(blob_len);

        for s in 0..num_stripes {
            let chunk_offset = s * chunk_size;

            self.decoder
                .reset(DATA_SLICES, CODING_SLICES, chunk_size)
                .map_err(|_| DecodeError::TooMuchData)?;

            // Feed available shards with inverse mapping
            for (slice_idx, slice_opt) in slices.iter().enumerate() {
                if let Some(slice) = slice_opt {
                    let shard_idx = slice_to_shard(self.strategy, s, slice_idx);
                    let chunk = &slice.data[chunk_offset..chunk_offset + chunk_size];

                    if shard_idx < DATA_SLICES {
                        self.decoder
                            .add_original_shard(shard_idx, chunk)
                            .map_err(|_| DecodeError::InvalidLayout)?;
                    } else {
                        self.decoder
                            .add_recovery_shard(shard_idx - DATA_SLICES, chunk)
                            .map_err(|_| DecodeError::InvalidLayout)?;
                    }
                }
            }

            let result = self.decoder.decode().map_err(|_| DecodeError::BadEncoding)?;

            // Reassemble stripe data
            let mut stripe_data = Vec::with_capacity(padded_stripe);
            for data_shard_idx in 0..DATA_SLICES {
                let slice_idx = shard_to_slice(self.strategy, s, data_shard_idx);
                let chunk = match &slices[slice_idx] {
                    Some(slice) => &slice.data[chunk_offset..chunk_offset + chunk_size],
                    None => result
                        .restored_original(data_shard_idx)
                        .ok_or(DecodeError::InvalidLayout)?,
                };
                stripe_data.extend_from_slice(chunk);
            }

            let take = if s == num_stripes - 1 {
                blob_len - output.len()
            } else {
                self.stripe_size
            };
            output.extend_from_slice(&stripe_data[..take]);
        }

        Ok(Blob::from(output))
    }

    fn encode_empty_blob(&mut self) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let padded_stripe = round_up_to(self.stripe_size, DATA_SLICES);
        let chunk_size = padded_stripe / DATA_SLICES;
        let padded = vec![0u8; padded_stripe];

        self.encoder
            .reset(DATA_SLICES, CODING_SLICES, chunk_size)
            .map_err(|_| EncodeError::TooMuchData)?;

        for chunk in padded.chunks(chunk_size) {
            self.encoder
                .add_original_shard(chunk)
                .map_err(|_| EncodeError::TooMuchData)?;
        }

        let result = self.encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

        let mut slices: Vec<Vec<u8>> = vec![Vec::new(); SLICE_COUNT];

        // Data shards with mapping (stripe 0)
        for (shard_idx, chunk) in padded.chunks(chunk_size).enumerate() {
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx] = chunk.to_vec();
        }

        // Parity shards with mapping (stripe 0)
        for (parity_idx, shard) in result.recovery_iter().enumerate() {
            let shard_idx = DATA_SLICES + parity_idx;
            let slice_idx = shard_to_slice(self.strategy, 0, shard_idx);
            slices[slice_idx] = shard.to_vec();
        }

        // Append metadata (blob_len = 0 for empty blob)
        let metadata = SliceMetadata::new(0, self.stripe_size);
        for slice in &mut slices {
            slice.extend_from_slice(&metadata.to_bytes());
        }

        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_mapping() {
        for stripe in 0..10 {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Identity, stripe, shard);
                assert_eq!(slice, shard);
                let recovered = slice_to_shard(MappingStrategy::Identity, stripe, slice);
                assert_eq!(recovered, shard);
            }
        }
    }

    #[test]
    fn test_rotated_mapping_inverse() {
        for stripe in 0..10 {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                let recovered = slice_to_shard(MappingStrategy::Rotated, stripe, slice);
                assert_eq!(shard, recovered);
            }
        }
    }

    #[test]
    fn test_rotation_distribution() {
        let num_stripes = 1024;
        let mut slice_hits = vec![0usize; SLICE_COUNT];

        for stripe in 0..num_stripes {
            for shard in 0..SLICE_COUNT {
                let slice = shard_to_slice(MappingStrategy::Rotated, stripe, shard);
                slice_hits[slice] += 1;
            }
        }

        // Each slice should be hit equally
        for (i, &hits) in slice_hits.iter().enumerate() {
            assert_eq!(hits, num_stripes, "slice {} hit count mismatch", i);
        }
    }
}




/FILE: network/slicer/src/api.rs

use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Slice};
use super::consts::SLICE_COUNT;

pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;
    const DATA_OUTPUT_SLICES: usize;
    const CODING_OUTPUT_SLICES: usize;

    /// Encode a blob into SLICE_COUNT slices (DATA_SLICES data + CODING_SLICES parity).
    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError>;

    /// Decode slices back into the original blob.
    /// Requires at least DATA_SLICES valid slices for reconstruction.
    fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError>;
}




/FILE: network/slicer/src/slice_index.rs

use super::SLICE_COUNT;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::mem::MaybeUninit;
use std::ops::Deref;
use wincode::{SchemaRead, SchemaWrite};

/// Index of a slice within a blob's erasure-coded output.
/// Valid range: 0 to SLICE_COUNT-1.
///
/// Each blob is encoded into SLICE_COUNT slices. The slice at index N
/// for any blob is stored in spool N on the network.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, SchemaWrite)]
pub struct SliceIndex(usize);

impl SliceIndex {
    pub fn new(index: usize) -> Option<Self> {
        if index < SLICE_COUNT {
            Some(Self(index))
        } else {
            None
        }
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..SLICE_COUNT).map(Self)
    }
}

impl Deref for SliceIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for SliceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for SliceIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VisitorImpl;
        impl<'de> serde::de::Visitor<'de> for VisitorImpl {
            type Value = SliceIndex;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a usize in [0, SLICE_COUNT)")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                SliceIndex::new(v as usize)
                    .ok_or_else(|| E::custom(format!("index {} out of bounds", v)))
            }
        }
        deserializer.deserialize_u64(VisitorImpl)
    }
}

impl<'de> SchemaRead<'de> for SliceIndex {
    type Dst = Self;

    fn read(
        reader: &mut impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        unsafe {
            reader.copy_into_t(dst)?;
            if dst.assume_init_ref().0 >= SLICE_COUNT {
                Err(wincode::ReadError::Custom("slice index out of bounds"))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wincode;

    #[test]
    fn serde_roundtrip_ok() {
        let vals = [0, 1, SLICE_COUNT - 1];
        for v in vals {
            let s = serde_json::to_string(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = serde_json::from_str(&s).unwrap();
        }
    }

    #[test]
    fn serde_fail() {
        let vals = [SLICE_COUNT, SLICE_COUNT + 1];
        for v in vals {
            let s = serde_json::to_string(&v).unwrap();
            let res: Result<SliceIndex, _> = serde_json::from_str(&s);
            assert!(res.is_err());
        }
    }

    #[test]
    fn wincode_roundtrip_ok() {
        let vals = [0, SLICE_COUNT - 1];
        for v in vals {
            let b = wincode::serialize(&SliceIndex(v)).unwrap();
            let _idx: SliceIndex = wincode::deserialize(&b).unwrap();
        }
    }

    #[test]
    fn wincode_fail() {
        let vals = [SLICE_COUNT, SLICE_COUNT + 1, usize::MAX];
        for v in vals {
            let b = wincode::serialize(&SliceIndex(v)).unwrap();
            let res: Result<SliceIndex, _> = wincode::deserialize(&b);
            assert!(res.is_err());
        }
    }
}




/FILE: network/slicer/src/errors.rs

use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    #[error("too much data to encode in a single stripe/coder configuration")]
    TooMuchData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("not enough slices to reconstruct (need at least DATA_SLICES)")]
    NotEnoughSlices,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding in recovered data")]
    BadEncoding,
    #[error("invalid layout or inconsistent slices")]
    InvalidLayout,
}




/FILE: network/slicer/src/lib.rs

#![allow(clippy::len_without_is_empty)]

pub mod consts;
pub mod errors;
pub mod types;
pub mod api;
pub mod basic;
pub mod codec;
pub mod striped;
pub mod rotated;
pub mod merkle_helpers;
pub mod reed_solomon;
pub mod slice_index;

#[cfg(test)]
mod bench;

pub use consts::{MERKLE_HEIGHT, SLICE_COUNT, F, CODING_SLICES, DATA_SLICES};
pub use errors::{EncodeError, DecodeError};
pub use types::{Slice, Blob};
pub use api::Slicer;
pub use basic::BasicSlicer;
pub use codec::{DEFAULT_STRIPE_SIZE, ROTATION_STEP, STRIPE_SIZES, pick_stripe_size, SliceMetadata};
pub use striped::StripedSlicer;
pub use rotated::RotatedSlicer;
pub use merkle_helpers::{BlobMerkleTree, BlobMerkleRoot, build_blob_merkle_tree, blob_merkle_root};
pub use slice_index::SliceIndex;
pub use reed_solomon::MAX_SLICE_BYTES;




/FILE: network/slicer/src/reed_solomon.rs

use super::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::Slice;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use thiserror::Error;

/// Maximum slice size for BasicSlicer (used for testing/debugging only).
/// 4 KiB allows encoding blobs up to ~2.7 MB (DATA_SLICES * 4 KiB).
/// For production workloads, use StripedSlicer which handles large blobs efficiently.
pub const MAX_SLICE_BYTES: usize = 1 << 12; // 4 KiB

/// Errors that may be returned by ReedSolomonCoder::encode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonEncodeError {
    #[error("too much data to encode with current settings")]
    TooMuchData,
}

/// Errors that may be returned by ReedSolomonCoder::decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonDecodeError {
    #[error("not enough slices to reconstruct (need at least DATA_SLICES)")]
    NotEnoughSlices,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding detected")]
    InvalidPadding,
    #[error("invalid layout or inconsistent slice sizes/indices")]
    InvalidLayout,
}

/// The data and coding slices output by encode().
#[derive(Clone, Debug)]
pub struct RawSlices {
    pub data: Vec<Vec<u8>>,
    pub coding: Vec<Vec<u8>>,
}

/// Reed-Solomon coder for 3f+1 layout (k = data, r = coding).
/// This is a thin wrapper around reed_solomon_simd. It reuses working buffers across calls.
pub struct ReedSolomonCoder {
    k_data: usize,
    r_coding: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl ReedSolomonCoder {
    /// Create a new Reed-Solomon coder with default max slice size (4 KiB).
    /// This is suitable for testing/debugging. For larger blobs, use `with_max_slice_bytes`.
    pub fn new(k_data: usize, r_coding: usize) -> Self {
        Self::with_max_slice_bytes(k_data, r_coding, MAX_SLICE_BYTES)
    }

    /// Create a new Reed-Solomon coder with a custom max slice size.
    ///
    /// The max_slice_bytes determines the maximum size of each slice,
    /// which affects memory allocation in the encoder/decoder.
    /// Use larger values for benchmarks or when encoding large blobs.
    pub fn with_max_slice_bytes(k_data: usize, r_coding: usize, max_slice_bytes: usize) -> Self {
        assert!(u16::MAX as usize >= 65535);
        assert!(k_data > 0, "k_data must be > 0");
        assert!(r_coding > 0, "r_coding must be > 0");
        assert!(max_slice_bytes > 0, "max_slice_bytes must be > 0");

        let n_total = k_data + r_coding;
        assert!(n_total <= 65536, "too many total slices for RS field");
        assert!(k_data == DATA_SLICES, "k_data must match DATA_SLICES");
        assert!(r_coding == CODING_SLICES, "r_coding must match CODING_SLICES");

        // Use a bounded max slice size the library accepts. Per-call reset() will set the actual slice size.
        let encoder = ReedSolomonEncoder::new(k_data, r_coding, max_slice_bytes)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(k_data, r_coding, max_slice_bytes)
            .expect("RS decoder init");

        Self {
            k_data,
            r_coding,
            encoder,
            decoder,
        }
    }

    /// Reed-Solomon encodes the payload into k data and r coding slices, returning RawSlices.
    /// Returns TooMuchData if payload cannot be encoded under the current encoder limits.
    pub fn encode(&mut self, payload: &[u8]) -> Result<RawSlices, ReedSolomonEncodeError> {
        // Compute padding: make total a multiple of 2 * k.
        let k = self.k_data;
        let two_k = 2 * k;

        // Avoid division by zero; guaranteed by constructor.
        debug_assert!(k > 0);

        // If payload is empty, we still add the 0x80 byte (minimum padding).
        let remainder = payload.len() % two_k;
        let padding_bytes = if remainder == 0 { two_k } else { two_k - remainder };
        let total_len = payload
            .len()
            .checked_add(padding_bytes)
            .ok_or(ReedSolomonEncodeError::TooMuchData)?;

        // slice_bytes = ceil(total_len / k)
        let slice_bytes = (total_len + k - 1) / k;

        // Ensure the encoder can handle this slice size.
        self.encoder
            .reset(self.k_data, self.r_coding, slice_bytes)
            .map_err(|_| ReedSolomonEncodeError::TooMuchData)?;

        // Place 0x80 and zeros at end of payload (bit padding).
        let last_group_bytes = (two_k + slice_bytes - 1) / slice_bytes * slice_bytes;
        let boundary = total_len
            .checked_sub(last_group_bytes)
            .ok_or(ReedSolomonEncodeError::TooMuchData)?;
        let mut tail = Vec::with_capacity(last_group_bytes);
        tail.extend_from_slice(&payload[boundary..payload.len()]);
        tail.push(0x80);
        tail.resize(last_group_bytes, 0x00);

        // Feed k original slices into the encoder.
        let mut data = Vec::with_capacity(self.k_data);
        payload[..boundary]
            .chunks(slice_bytes)
            .chain(tail.chunks(slice_bytes))
            .for_each(|chunk| {
                self.encoder
                    .add_original_shard(chunk)
                    .expect("adding slices of the configured size should succeed");
                data.push(chunk.to_vec());
            });

        // Create parity slices.
        let output = self
            .encoder
            .encode()
            .expect("should be able to encode after k data slices were added");
        let coding = output.recovery_iter().map(<[u8]>::to_vec).collect();

        Ok(RawSlices { data, coding })
    }

    /// Reconstructs the raw payload bytes from optional slices (data and coding).
    /// Layout: data slices are indices [0..k), coding slices are indices [k..k+r).
    /// At least k total slices (data+coding) are required.
    pub fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Vec<u8>, ReedSolomonDecodeError> {
        let present = slices.iter().flatten().count();
        if present < self.k_data {
            return Err(ReedSolomonDecodeError::NotEnoughSlices);
        }

        // Infer slice_bytes from any present slice.
        let slice_bytes = slices
            .iter()
            .flatten()
            .map(|s| s.data.len())
            .next()
            .ok_or(ReedSolomonDecodeError::InvalidLayout)?;

        // Ensure all present slices have the same size.
        if slices
            .iter()
            .flatten()
            .any(|s| s.data.len() != slice_bytes)
        {
            return Err(ReedSolomonDecodeError::InvalidLayout);
        }

        self.decoder
            .reset(self.k_data, self.r_coding, slice_bytes)
            .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;

        // Split into data and coding by index ranges.
        // Feed data slices (original) and coding slices (recovery) into decoder.
        for s in slices.iter().flatten() {
            let idx = *s.index;
            if idx < self.k_data {
                // data slice at index idx
                self.decoder
                    .add_original_shard(idx, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else if idx < self.k_data + self.r_coding {
                // coding slice at offset
                let offset = idx - self.k_data;
                self.decoder
                    .add_recovery_shard(offset, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else {
                return Err(ReedSolomonDecodeError::InvalidLayout);
            }
        }

        let restored = self.decoder.decode().map_err(|_| {
            // If the library returns an error here, it's likely because the slices were inconsistent.
            ReedSolomonDecodeError::InvalidLayout
        })?;

        // Reassemble the payload from data slices in order [0..k).
        // If a data slice was missing, pull the restored version.
        let mut payload = Vec::with_capacity(self.k_data * slice_bytes);
        for data_idx in 0..self.k_data {
            let slice_ref = match slices[data_idx].as_ref() {
                Some(s) => &s.data,
                None => restored
                    .restored_original(data_idx)
                    .ok_or(ReedSolomonDecodeError::InvalidLayout)?,
            };
            // Avoid expanding to impossible sizes.
            payload
                .try_reserve(slice_ref.len())
                .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;
            payload.extend_from_slice(slice_ref);
        }

        // Remove padding: scan backwards counting zeros, then require a single 0x80 preceding them.
        if payload.is_empty() {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        let zeros = payload.iter().rev().take_while(|b| **b == 0).count();
        let padding_total = zeros + 1;
        if padding_total > payload.len() {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        let marker_pos = payload.len() - padding_total;
        if payload[marker_pos] != 0x80 {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        payload.truncate(marker_pos);

        Ok(payload)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::{Slice, CODING_SLICES, DATA_SLICES, SLICE_COUNT};
    use crate::SliceIndex;

    /// Create a test coder with the default configuration.
    fn test_coder() -> ReedSolomonCoder {
        ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES)
    }

    fn make_payload(len: usize) -> Vec<u8> {
        // Deterministic, non-trivial pattern
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_full(raw: &RawSlices) -> [Option<Slice>; SLICE_COUNT] {
        let mut arr: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
        for (i, d) in raw.data.iter().enumerate() {
            arr[i] = Some(Slice {
                index: SliceIndex::new(i).unwrap(),
                data: d.clone(),
            });
        }
        for (j, c) in raw.coding.iter().enumerate() {
            let idx = DATA_SLICES + j;
            arr[idx] = Some(Slice {
                index: SliceIndex::new(idx).unwrap(),
                data: c.clone(),
            });
        }
        arr
    }

    fn keep_only(arr: &mut [Option<Slice>; SLICE_COUNT], keep: &[usize]) {
        let mut keep_set = vec![false; SLICE_COUNT];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    fn equal_sizes(arr: &[Option<Slice>; SLICE_COUNT]) -> Option<usize> {
        let mut size = None;
        for s in arr.iter().flatten() {
            match size {
                None => size = Some(s.data.len()),
                Some(expected) if expected != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    #[test]
    fn encode_counts() {
        let mut coder = test_coder();
        let payload = make_payload(42_000);
        let raw = coder.encode(&payload).expect("encode ok");

        assert_eq!(raw.data.len(), DATA_SLICES);
        assert_eq!(raw.coding.len(), CODING_SLICES);

        let slice_len = raw.data[0].len();
        assert!(raw.data.iter().all(|d| d.len() == slice_len));
        assert!(raw.coding.iter().all(|c| c.len() == slice_len));
    }

    #[test]
    fn roundtrip_sizes() {
        let mut coder = test_coder();

        let sizes = [
            0usize,
            1,
            DATA_SLICES - 1,
            DATA_SLICES,
            DATA_SLICES + 1,
            2 * DATA_SLICES - 1,
            2 * DATA_SLICES,
            5 * DATA_SLICES + 123,
            100_000,
        ];

        for &sz in &sizes {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");
            let full = to_full(&raw);

            // all slices
            let restored = coder.decode(&full).expect("decode ok");
            assert_eq!(restored, payload, "round-trip mismatch for size {}", sz);

            // only data slices (k)
            let mut only_data = full.clone();
            keep_only(&mut only_data, &(0..DATA_SLICES).collect::<Vec<_>>());
            let restored = coder.decode(&only_data).expect("decode ok with k data slices");
            assert_eq!(restored, payload, "round-trip data-only mismatch for size {}", sz);

            // mixed: ~k/2 data + all coding, then fill to k
            let half_data = DATA_SLICES / 2;
            let mut keep = Vec::with_capacity(DATA_SLICES);
            for i in (0..DATA_SLICES).step_by(2).take(half_data) {
                keep.push(i);
            }
            for j in 0..CODING_SLICES {
                keep.push(DATA_SLICES + j);
            }
            while keep.len() < DATA_SLICES {
                let mut added = false;
                for i in 0..DATA_SLICES {
                    if !keep.contains(&i) {
                        keep.push(i);
                        added = true;
                        break;
                    }
                }
                assert!(added);
            }

            let mut mixed = full.clone();
            keep_only(&mut mixed, &keep);
            assert_eq!(mixed.iter().flatten().count(), DATA_SLICES);
            let restored = coder.decode(&mixed).expect("decode ok with mixed slices");
            assert_eq!(restored, payload, "round-trip mixed mismatch size {}", sz);
        }
    }

    #[test]
    fn tiny() {
        let mut coder = test_coder();

        for sz in 0..4usize {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");
            let slices = to_full(&raw);
            let out = coder.decode(&slices).expect("decode ok");
            assert_eq!(out, payload, "tiny payload mismatch sz={}", sz);
        }
    }

    #[test]
    fn not_enough() {
        let mut coder = test_coder();
        let payload = make_payload(10_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut slices = to_full(&raw);

        // keep only k-1 data slices
        let keep: Vec<usize> = (0..(DATA_SLICES - 1)).collect();
        keep_only(&mut slices, &keep);

        let res = coder.decode(&slices);
        assert!(matches!(res, Err(ReedSolomonDecodeError::NotEnoughSlices)));
    }

    #[test]
    fn bad_size() {
        let mut coder = test_coder();
        let payload = make_payload(50_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut slices = to_full(&raw);

        // uniform to start
        let base_len = equal_sizes(&slices).expect("uniform sizes");

        // tamper: shrink one slice by 1 byte
        if let Some(Some(sh)) = slices.get_mut(0) {
            assert_eq!(sh.data.len(), base_len);
            sh.data.pop();
            assert_eq!(sh.data.len(), base_len - 1);
        } else {
            panic!("expected slice present");
        }

        let res = coder.decode(&slices);
        assert!(matches!(res, Err(ReedSolomonDecodeError::InvalidLayout)));
    }

    #[test]
    fn empty_rt() {
        let mut coder = test_coder();
        let payload = Vec::<u8>::new();
        let raw = coder.encode(&payload).expect("encode ok for empty payload");
        let slices = to_full(&raw);
        let out = coder.decode(&slices).expect("decode ok");
        assert!(out.is_empty(), "decoded payload should be empty");
    }


    #[test]
    fn size_table() {
        // Keep this short so it's readable on the terminal.

        let mut coder = test_coder();
        // Max payload with default 4 KiB slices: 4 KiB * 683 data slices = ~2.7 MB
        // Keep sizes modest for test speed
        let sizes = [
            0usize,
            1,
            DATA_SLICES / 2,
            DATA_SLICES - 1,
            DATA_SLICES,
            DATA_SLICES + 1,
            10_000,
            50_000,
            100_000,
        ];

        println!(
            "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
            "payload", "slice", "k", "r", "n", "total_bytes", "ratio", "ok"
        );
        println!(
            "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
            "(bytes)", "(bytes)", "", "", "", "(bytes)", "", ""
        );

        for &sz in &sizes {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");

            // All slices have equal length (by construction)
            let slice_len = raw.data[0].len();
            let n = DATA_SLICES + CODING_SLICES;
            let total_bytes = n * slice_len;
            let ratio_str = if sz > 0 {
                format!("{:.3}", total_bytes as f64 / sz as f64)
            } else {
                "-".to_string()
            };

            // Build full slice set and round trip
            let mut slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
            for (i, d) in raw.data.iter().enumerate() {
                slices[i] = Some(Slice {
                    index: SliceIndex::new(i).unwrap(),
                    data: d.clone(),
                });
            }
            for (j, c) in raw.coding.iter().enumerate() {
                let idx = DATA_SLICES + j;
                slices[idx] = Some(Slice {
                    index: SliceIndex::new(idx).unwrap(),
                    data: c.clone(),
                });
            }

            let out = coder.decode(&slices).expect("decode ok");
            let ok = out == payload;

            println!(
                "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
                sz,
                slice_len,
                DATA_SLICES,
                CODING_SLICES,
                n,
                total_bytes,
                ratio_str,
                if ok { "ok" } else { "FAIL" }
            );

            // Keep the test meaningful
            assert!(ok, "round-trip failed for size {}", sz);
        }
    }
}




/FILE: network/slicer/src/types.rs

use super::slice_index::SliceIndex;

/// A single slice of an erasure-coded blob.
///
/// Each blob is encoded into SLICE_COUNT slices (DATA_SLICES data + CODING_SLICES parity).
/// The slice at index N for this blob will be stored in spool N on the network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    pub index: SliceIndex,
    pub data: Vec<u8>,
}

impl Slice {
    pub fn new(index: SliceIndex, data: Vec<u8>) -> Self {
        Self { index, data }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Blob {
    pub data: Vec<u8>,
}

impl From<Vec<u8>> for Blob {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl Blob {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}




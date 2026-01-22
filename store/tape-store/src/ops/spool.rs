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

//! Spool operations

use crate::columns::{
    SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolStatusCol, SpoolSyncCursorCol,
};
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, SliceKey, SpoolIndexKey, SpoolState};
use crate::TapeStore;
use store::{Column, Store};
use tape_core::spooler::SpoolIndex;

/// Operations for spool management
pub trait SpoolOps {
    // Spool state (status + epoch entered)
    fn get_spool_state(&self, spool_id: SpoolIndex) -> Result<Option<SpoolState>>;
    fn set_spool_state(&self, spool_id: SpoolIndex, state: SpoolState) -> Result<()>;
    fn remove_spool_state(&self, spool_id: SpoolIndex) -> Result<()>;

    // Iterate all spools
    fn iter_all_spools(&self) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    // Pending repair
    fn add_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()>;
    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()>;
    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<bool>;

    // Iterate pending repairs for a spool (up to `limit`)
    fn iter_pending_repairs(
        &self,
        spool_id: SpoolIndex,
        limit: usize,
    ) -> Result<Vec<Pubkey>>;

    // Pending recovery
    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()>;
    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()>;
    fn has_pending_recovery(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<bool>;

    // Iterate pending recoveries for a spool (up to `limit`)
    fn iter_pending_recoveries(
        &self,
        spool_id: SpoolIndex,
        limit: usize,
    ) -> Result<Vec<Pubkey>>;

    // Sync progress
    fn get_spool_sync_cursor(&self, spool_id: SpoolIndex) -> Result<Option<Pubkey>>;
    fn set_spool_sync_cursor(
        &self,
        spool_id: SpoolIndex,
        last_synced_track: Pubkey,
    ) -> Result<()>;
    fn remove_spool_sync_cursor(&self, spool_id: SpoolIndex) -> Result<()>;

    // Bulk clear all pending repairs for a spool
    fn clear_all_pending_repairs(&self, spool_id: SpoolIndex) -> Result<()>;

    // Bulk clear all pending recoveries for a spool
    fn clear_all_pending_recoveries(&self, spool_id: SpoolIndex) -> Result<()>;
}

impl<S: Store> SpoolOps for TapeStore<S> {
    fn get_spool_state(&self, spool_id: SpoolIndex) -> Result<Option<SpoolState>> {
        let key = SpoolIndexKey::new(spool_id);
        Ok(self.get::<SpoolStatusCol>(&key)?)
    }

    fn set_spool_state(&self, spool_id: SpoolIndex, state: SpoolState) -> Result<()> {
        let key = SpoolIndexKey::new(spool_id);
        self.put::<SpoolStatusCol>(&key, &state)?;
        Ok(())
    }

    fn remove_spool_state(&self, spool_id: SpoolIndex) -> Result<()> {
        let key = SpoolIndexKey::new(spool_id);
        self.delete::<SpoolStatusCol>(&key)?;
        Ok(())
    }

    fn iter_all_spools(&self) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let iter = self.iter::<SpoolStatusCol>()?;
        Ok(iter
            .into_iter()
            .map(|(key, state)| (key.0, state))
            .collect())
    }

    fn add_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<SpoolPendingRepairCol>(&key, &())?;
        Ok(())
    }

    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<SpoolPendingRepairCol>(&key)?;
        Ok(())
    }

    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SpoolPendingRepairCol>(&key)?)
    }

    fn iter_pending_repairs(&self, spool_id: SpoolIndex, limit: usize) -> Result<Vec<Pubkey>> {
        iter_pending_by_spool(self, SpoolPendingRepairCol::CF_NAME, spool_id, limit)
    }

    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<SpoolPendingRecoveryCol>(&key, &())?;
        Ok(())
    }

    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Pubkey) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<SpoolPendingRecoveryCol>(&key)?;
        Ok(())
    }

    fn has_pending_recovery(
        &self,
        spool_id: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SpoolPendingRecoveryCol>(&key)?)
    }

    fn iter_pending_recoveries(
        &self,
        spool_id: SpoolIndex,
        limit: usize,
    ) -> Result<Vec<Pubkey>> {
        iter_pending_by_spool(self, SpoolPendingRecoveryCol::CF_NAME, spool_id, limit)
    }

    fn clear_all_pending_repairs(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_all_pending_by_spool(self, SpoolPendingRepairCol::CF_NAME, spool_id)
    }

    fn clear_all_pending_recoveries(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_all_pending_by_spool(self, SpoolPendingRecoveryCol::CF_NAME, spool_id)
    }

    fn get_spool_sync_cursor(&self, spool_id: SpoolIndex) -> Result<Option<Pubkey>> {
        let key = SpoolIndexKey::new(spool_id);
        Ok(self.get::<SpoolSyncCursorCol>(&key)?)
    }

    fn set_spool_sync_cursor(
        &self,
        spool_id: SpoolIndex,
        last_synced_track: Pubkey,
    ) -> Result<()> {
        let key = SpoolIndexKey::new(spool_id);
        self.put::<SpoolSyncCursorCol>(&key, &last_synced_track)?;
        Ok(())
    }

    fn remove_spool_sync_cursor(&self, spool_id: SpoolIndex) -> Result<()> {
        let key = SpoolIndexKey::new(spool_id);
        self.delete::<SpoolSyncCursorCol>(&key)?;
        Ok(())
    }

}

fn iter_pending_by_spool<S: Store>(
    store: &TapeStore<S>,
    cf_name: &str,
    spool_id: SpoolIndex,
    limit: usize,
) -> Result<Vec<Pubkey>> {
    let prefix = SliceKey::spool_prefix(spool_id);
    let iter = store.inner().inner().iter_prefix(cf_name, &prefix)?;

    let mut results = Vec::new();
    for (key_bytes, _value_bytes) in iter {
        let key: SliceKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("pending key: {}", e)))?;
        results.push(key.track_address);
        if results.len() >= limit {
            break;
        }
    }
    Ok(results)
}

fn clear_all_pending_by_spool<S: Store>(
    store: &TapeStore<S>,
    cf_name: &str,
    spool_id: SpoolIndex,
) -> Result<()> {
    let raw = store.inner().inner();
    let prefix = SliceKey::spool_prefix(spool_id);

    let keys: Vec<Vec<u8>> = raw
        .iter_prefix(cf_name, &prefix)?
        .map(|(key, _)| key)
        .collect();

    for key in keys {
        raw.delete(cf_name, &key)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::types::EpochNumber;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    use crate::types::SpoolStatus;

    fn active_state() -> SpoolState {
        SpoolState::new(SpoolStatus::Active, EpochNumber(0))
    }

    fn sync_state() -> SpoolState {
        SpoolState::new(SpoolStatus::Sync, EpochNumber(0))
    }

    fn recover_state() -> SpoolState {
        SpoolState::new(SpoolStatus::Recover, EpochNumber(0))
    }

    #[test]
    fn spool_state_roundtrip() {
        let store = test_store();
        let spool_id = 42;

        assert!(store.get_spool_state(spool_id).unwrap().is_none());

        store
            .set_spool_state(spool_id, active_state())
            .unwrap();

        assert!(store.get_spool_state(spool_id).unwrap().unwrap().is_active());
    }

    #[test]
    fn iter_all_spools() {
        let store = test_store();

        store
            .set_spool_state(10, active_state())
            .unwrap();
        store
            .set_spool_state(20, sync_state())
            .unwrap();
        store
            .set_spool_state(30, recover_state())
            .unwrap();

        let spools = store.iter_all_spools().unwrap();
        assert_eq!(spools.len(), 3);
    }

    #[test]
    fn test_pending_recovery() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        assert!(!store.has_pending_recovery(spool_id, track).unwrap());

        store.add_pending_recovery(spool_id, track).unwrap();
        assert!(store.has_pending_recovery(spool_id, track).unwrap());

        store.remove_pending_recovery(spool_id, track).unwrap();
        assert!(!store.has_pending_recovery(spool_id, track).unwrap());
    }

    #[test]
    fn test_pending_repair() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        assert!(!store.has_pending_repair(spool_id, track).unwrap());

        store.add_pending_repair(spool_id, track).unwrap();
        assert!(store.has_pending_repair(spool_id, track).unwrap());

        store.remove_pending_repair(spool_id, track).unwrap();
        assert!(!store.has_pending_repair(spool_id, track).unwrap());
    }

    #[test]
    fn test_iter_pending_recoveries() {
        let store = test_store();
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        store.add_pending_recovery(spool_id, track1).unwrap();
        store.add_pending_recovery(spool_id, track2).unwrap();
        store.add_pending_recovery(spool_id, track3).unwrap();

        // Different spool should not appear
        store
            .add_pending_recovery(99, Pubkey::new_unique())
            .unwrap();

        let pending = store.iter_pending_recoveries(spool_id, 100).unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn test_iter_pending_repairs() {
        let store = test_store();
        let spool_id = 42;

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        store.add_pending_repair(spool_id, track1).unwrap();
        store.add_pending_repair(spool_id, track2).unwrap();
        store.add_pending_repair(spool_id, track3).unwrap();

        store.add_pending_repair(99, Pubkey::new_unique()).unwrap();

        let pending = store.iter_pending_repairs(spool_id, 100).unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn clear_all_pending() {
        let store = test_store();

        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let t3 = Pubkey::new_unique();

        store.add_pending_recovery(42, t1).unwrap();
        store.add_pending_recovery(42, t2).unwrap();
        store.add_pending_recovery(99, t3).unwrap();

        store.clear_all_pending_recoveries(42).unwrap();

        assert!(store.iter_pending_recoveries(42, 100).unwrap().is_empty());
        assert_eq!(store.iter_pending_recoveries(99, 100).unwrap().len(), 1);
    }

    #[test]
    fn clear_all_pending_repairs() {
        let store = test_store();

        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let t3 = Pubkey::new_unique();

        store.add_pending_repair(42, t1).unwrap();
        store.add_pending_repair(42, t2).unwrap();
        store.add_pending_repair(99, t3).unwrap();

        store.clear_all_pending_repairs(42).unwrap();

        assert!(store.iter_pending_repairs(42, 100).unwrap().is_empty());
        assert_eq!(store.iter_pending_repairs(99, 100).unwrap().len(), 1);
    }

    #[test]
    fn test_sync_progress_roundtrip() {
        let store = test_store();
        let spool_id = 42;
        let track = Pubkey::new_unique();

        assert!(store.get_spool_sync_cursor(spool_id).unwrap().is_none());

        store.set_spool_sync_cursor(spool_id, track).unwrap();
        assert_eq!(store.get_spool_sync_cursor(spool_id).unwrap(), Some(track));

        store.remove_spool_sync_cursor(spool_id).unwrap();
        assert!(store.get_spool_sync_cursor(spool_id).unwrap().is_none());
    }
}

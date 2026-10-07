//! Spool operations

use tape_core::system::SpoolState;
use tape_core::types::{SlotNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store_api::{Column, Store};

use crate::columns::{SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolStatusCol};
use crate::error::{Result, TapeStoreError};
use crate::ops::MetaOps;
use crate::types::{SliceKey, SpoolIndexKey};
use crate::TapeStore;

/// Operations for spool management
pub trait SpoolOps {
    // Spool state (status + epoch entered)
    fn get_spool_state(&self, spool_id: SpoolIndex) -> Result<Option<SpoolState>>;
    fn set_spool_state(&self, spool_id: SpoolIndex, state: SpoolState) -> Result<()>;
    fn remove_spool_state(&self, spool_id: SpoolIndex) -> Result<()>;

    // Iterate all spools
    fn iter_all_spools(&self) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    // Pending repair, ready at once
    fn add_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    // Pending repair the repair path leaves alone until `ready_at`; an existing row keeps its slot
    fn defer_pending_repair(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        ready_at: SlotNumber,
    ) -> Result<()>;

    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;
    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    // Repairs a spool can run now: rows whose ready slot the applied cursor has reached (up to `limit`)
    fn iter_pending_repairs( &self, spool_id: SpoolIndex, limit: usize,) -> Result<Vec<Address>>;

    // Pending recovery
    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;
    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;
    fn has_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    // Iterate pending recoveries for a spool (up to `limit`)
    fn iter_pending_recoveries( &self, spool_id: SpoolIndex, limit: usize,) -> Result<Vec<Address>>;

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

    fn add_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<SpoolPendingRepairCol>(&key, &SlotNumber::zero())?;
        Ok(())
    }

    fn defer_pending_repair(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        ready_at: SlotNumber,
    ) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        if self.contains::<SpoolPendingRepairCol>(&key)? {
            return Ok(());
        }
        self.put::<SpoolPendingRepairCol>(&key, &ready_at)?;
        Ok(())
    }

    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<SpoolPendingRepairCol>(&key)?;
        Ok(())
    }

    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SpoolPendingRepairCol>(&key)?)
    }

    fn iter_pending_repairs(&self, spool_id: SpoolIndex, limit: usize) -> Result<Vec<Address>> {
        let applied = self.get_sync_cursor()?.unwrap_or_default();
        iter_ripe_repairs(self, spool_id, applied, limit)
    }

    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.put::<SpoolPendingRecoveryCol>(&key, &())?;
        Ok(())
    }

    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        self.delete::<SpoolPendingRecoveryCol>(&key)?;
        Ok(())
    }

    fn has_pending_recovery(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
    ) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SpoolPendingRecoveryCol>(&key)?)
    }

    fn iter_pending_recoveries(
        &self,
        spool_id: SpoolIndex,
        limit: usize,
    ) -> Result<Vec<Address>> {
        iter_pending_by_spool(self, SpoolPendingRecoveryCol::CF_NAME, spool_id, limit)
    }

    fn clear_all_pending_repairs(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_all_pending_by_spool(self, SpoolPendingRepairCol::CF_NAME, spool_id)
    }

    fn clear_all_pending_recoveries(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_all_pending_by_spool(self, SpoolPendingRecoveryCol::CF_NAME, spool_id)
    }

}

/// Walk one spool's repair rows, skipping the ones whose ready slot is still ahead
fn iter_ripe_repairs<S: Store>(
    store: &TapeStore<S>,
    spool_id: SpoolIndex,
    applied: SlotNumber,
    limit: usize,
) -> Result<Vec<Address>> {
    let prefix = SliceKey::spool_prefix(spool_id)?;
    let iter = store
        .inner()
        .inner()
        .iter_prefix(SpoolPendingRepairCol::CF_NAME, &prefix)?;

    let mut results = Vec::new();
    for (key_bytes, value_bytes) in iter {
        let ready_at: SlotNumber = wincode::deserialize(&value_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("repair ready slot: {}", e)))?;
        if ready_at > applied {
            continue;
        }
        let key: SliceKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("pending key: {}", e)))?;
        results.push(key.track_address);
        if results.len() >= limit {
            break;
        }
    }
    Ok(results)
}

fn iter_pending_by_spool<S: Store>(
    store: &TapeStore<S>,
    cf_name: &str,
    spool_id: SpoolIndex,
    limit: usize,
) -> Result<Vec<Address>> {
    let prefix = SliceKey::spool_prefix(spool_id)?;
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
    let prefix = SliceKey::spool_prefix(spool_id)?;

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
    use tape_store_memory::MemoryStore;
    use tape_core::types::EpochNumber;
    use tape_core::system::SpoolStatus;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }


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
        let spool_id = SpoolIndex(42);

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
            .set_spool_state(SpoolIndex(10), active_state())
            .unwrap();
        store
            .set_spool_state(SpoolIndex(20), sync_state())
            .unwrap();
        store
            .set_spool_state(SpoolIndex(30), recover_state())
            .unwrap();

        let spools = store.iter_all_spools().unwrap();
        assert_eq!(spools.len(), 3);
    }

    #[test]
    fn test_pending_recovery() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        assert!(!store.has_pending_recovery(spool_id, track).unwrap());

        store.add_pending_recovery(spool_id, track).unwrap();
        assert!(store.has_pending_recovery(spool_id, track).unwrap());

        store.remove_pending_recovery(spool_id, track).unwrap();
        assert!(!store.has_pending_recovery(spool_id, track).unwrap());
    }

    #[test]
    fn test_pending_repair() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        assert!(!store.has_pending_repair(spool_id, track).unwrap());

        store.add_pending_repair(spool_id, track).unwrap();
        assert!(store.has_pending_repair(spool_id, track).unwrap());

        store.remove_pending_repair(spool_id, track).unwrap();
        assert!(!store.has_pending_repair(spool_id, track).unwrap());
    }

    #[test]
    fn test_iter_pending_recoveries() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        let track1 = Address::new_unique();
        let track2 = Address::new_unique();
        let track3 = Address::new_unique();

        store.add_pending_recovery(spool_id, track1).unwrap();
        store.add_pending_recovery(spool_id, track2).unwrap();
        store.add_pending_recovery(spool_id, track3).unwrap();

        // Different spool should not appear
        store
            .add_pending_recovery(SpoolIndex(99), Address::new_unique())
            .unwrap();

        let pending = store.iter_pending_recoveries(spool_id, 100).unwrap();
        assert_eq!(pending.len(), 3);
    }

    // a deferred repair waits out its ready slot before the queue offers it
    #[test]
    fn deferred_repair_ripens() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        store.defer_pending_repair(spool_id, track, SlotNumber(30)).unwrap();

        assert!(store.has_pending_repair(spool_id, track).unwrap());
        assert!(store.iter_pending_repairs(spool_id, 10).unwrap().is_empty());

        store.set_sync_cursor(SlotNumber(30), None).unwrap();
        assert_eq!(store.iter_pending_repairs(spool_id, 10).unwrap(), vec![track]);
    }

    // a deferral leaves a repair the spool already owes ready
    #[test]
    fn deferral_keeps_queued() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        store.add_pending_repair(spool_id, track).unwrap();
        store.defer_pending_repair(spool_id, track, SlotNumber(30)).unwrap();

        assert_eq!(store.iter_pending_repairs(spool_id, 10).unwrap(), vec![track]);
    }

    #[test]
    fn test_iter_pending_repairs() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        let track1 = Address::new_unique();
        let track2 = Address::new_unique();
        let track3 = Address::new_unique();

        store.add_pending_repair(spool_id, track1).unwrap();
        store.add_pending_repair(spool_id, track2).unwrap();
        store.add_pending_repair(spool_id, track3).unwrap();

        store.add_pending_repair(SpoolIndex(99), Address::new_unique()).unwrap();

        let pending = store.iter_pending_repairs(spool_id, 100).unwrap();
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn clear_all_pending() {
        let store = test_store();

        let t1 = Address::new_unique();
        let t2 = Address::new_unique();
        let t3 = Address::new_unique();

        store.add_pending_recovery(SpoolIndex(42), t1).unwrap();
        store.add_pending_recovery(SpoolIndex(42), t2).unwrap();
        store.add_pending_recovery(SpoolIndex(99), t3).unwrap();

        store.clear_all_pending_recoveries(SpoolIndex(42)).unwrap();

        assert!(store.iter_pending_recoveries(SpoolIndex(42), 100).unwrap().is_empty());
        assert_eq!(store.iter_pending_recoveries(SpoolIndex(99), 100).unwrap().len(), 1);
    }

    #[test]
    fn clear_all_pending_repairs() {
        let store = test_store();

        let t1 = Address::new_unique();
        let t2 = Address::new_unique();
        let t3 = Address::new_unique();

        store.add_pending_repair(SpoolIndex(42), t1).unwrap();
        store.add_pending_repair(SpoolIndex(42), t2).unwrap();
        store.add_pending_repair(SpoolIndex(99), t3).unwrap();

        store.clear_all_pending_repairs(SpoolIndex(42)).unwrap();

        assert!(store.iter_pending_repairs(SpoolIndex(42), 100).unwrap().is_empty());
        assert_eq!(store.iter_pending_repairs(SpoolIndex(99), 100).unwrap().len(), 1);
    }
}

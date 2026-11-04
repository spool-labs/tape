//! Spool operations

use tape_core::system::SpoolState;
use tape_core::types::{SlotNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store_api::{Column, Direction, Store, WriteBatch};

use crate::columns::{
    SpoolPendingRecoveryCol, SpoolPendingRepairCol, SpoolRecoveryIndexCol, SpoolRepairIndexCol,
    SpoolStatusCol,
};
use crate::error::{Result, TapeStoreError};
use crate::ops::MetaOps;
use crate::types::{QueueKey, SliceKey, SpoolIndexKey};
use crate::TapeStore;

/// Slots past the applied slot that a row put back waits
///
/// One slot puts the row behind every row queued before it and past the
/// bound of the run that put it back.
pub const REQUEUE_SLOTS: SlotNumber = SlotNumber(1);

/// Rows a queue clear deletes per batch, so no batch outgrows a reel segment
const CLEAR_BATCH: usize = 512;

/// One row of a spool's repair or recovery queue
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuedTrack {
    /// The slot the row is due in
    pub ready: SlotNumber,
    /// The track the row is for
    pub track: Address,
}

/// A queue's rows in ready order, and its index of each row's ready slot by track
#[derive(Clone, Copy)]
struct Queue {
    rows: &'static str,
    index: &'static str,
}

const REPAIR: Queue = Queue {
    rows: SpoolPendingRepairCol::CF_NAME,
    index: SpoolRepairIndexCol::CF_NAME,
};

const RECOVERY: Queue = Queue {
    rows: SpoolPendingRecoveryCol::CF_NAME,
    index: SpoolRecoveryIndexCol::CF_NAME,
};

/// Operations for spool management
pub trait SpoolOps {
    // Spool state (status + epoch entered)
    fn get_spool_state(&self, spool_id: SpoolIndex) -> Result<Option<SpoolState>>;
    fn set_spool_state(&self, spool_id: SpoolIndex, state: SpoolState) -> Result<()>;
    fn remove_spool_state(&self, spool_id: SpoolIndex) -> Result<()>;

    // Iterate all spools
    fn iter_all_spools(&self) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    // Pending repair due at the applied slot; a track already queued keeps its row
    fn add_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    // Pending repair the repair path leaves alone until `ready_at`; a track already queued keeps its row
    fn defer_pending_repair(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        ready_at: SlotNumber,
    ) -> Result<()>;

    // Put a queued repair behind every row queued so far, due REQUEUE_SLOTS past the applied slot
    fn requeue_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;
    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    // Repair rows ready by `until`, in queue order from the row after `after` (up to `limit`)
    fn repairs_due(
        &self,
        spool_id: SpoolIndex,
        after: Option<QueuedTrack>,
        until: SlotNumber,
        limit: usize,
    ) -> Result<Vec<QueuedTrack>>;

    // Pending recovery due at the applied slot; a track already queued keeps its row
    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    // Put a queued recovery behind every row queued so far, due REQUEUE_SLOTS past the applied slot
    fn requeue_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;
    fn has_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    // Every recovery row of a spool, due or not, in queue order (up to `limit`)
    fn iter_pending_recoveries( &self, spool_id: SpoolIndex, limit: usize,) -> Result<Vec<Address>>;

    // Recovery rows ready by `until`, in queue order from the row after `after` (up to `limit`)
    fn recoveries_due(
        &self,
        spool_id: SpoolIndex,
        after: Option<QueuedTrack>,
        until: SlotNumber,
        limit: usize,
    ) -> Result<Vec<QueuedTrack>>;

    // Bulk clear all pending repairs for a spool
    fn clear_all_pending_repairs(&self, spool_id: SpoolIndex) -> Result<()>;

    // Bulk clear all pending recoveries for a spool
    fn clear_all_pending_recoveries(&self, spool_id: SpoolIndex) -> Result<()>;

    // Spools with any pending repair row, ready or waiting, in spool order
    fn pending_repair_spools(&self) -> Result<Vec<SpoolIndex>>;

    // Spools with any pending recovery row, in spool order
    fn pending_recovery_spools(&self) -> Result<Vec<SpoolIndex>>;
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
        let applied = applied_slot(self)?;
        enqueue(self, REPAIR, spool_id, track_address, applied)
    }

    fn defer_pending_repair(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        ready_at: SlotNumber,
    ) -> Result<()> {
        enqueue(self, REPAIR, spool_id, track_address, ready_at)
    }

    fn requeue_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let ready = applied_slot(self)?.saturating_add(REQUEUE_SLOTS);
        requeue(self, REPAIR, spool_id, track_address, ready)
    }

    fn remove_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        dequeue(self, REPAIR, spool_id, track_address)
    }

    fn has_pending_repair(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool> {
        is_queued(self, REPAIR, spool_id, track_address)
    }

    fn repairs_due(
        &self,
        spool_id: SpoolIndex,
        after: Option<QueuedTrack>,
        until: SlotNumber,
        limit: usize,
    ) -> Result<Vec<QueuedTrack>> {
        ready_tracks(self, REPAIR, spool_id, after, until, limit)
    }

    fn add_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let applied = applied_slot(self)?;
        enqueue(self, RECOVERY, spool_id, track_address, applied)
    }

    fn requeue_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let ready = applied_slot(self)?.saturating_add(REQUEUE_SLOTS);
        requeue(self, RECOVERY, spool_id, track_address, ready)
    }

    fn remove_pending_recovery(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        dequeue(self, RECOVERY, spool_id, track_address)
    }

    fn has_pending_recovery(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
    ) -> Result<bool> {
        is_queued(self, RECOVERY, spool_id, track_address)
    }

    fn iter_pending_recoveries(
        &self,
        spool_id: SpoolIndex,
        limit: usize,
    ) -> Result<Vec<Address>> {
        let rows = ready_tracks(self, RECOVERY, spool_id, None, SlotNumber(u64::MAX), limit)?;
        Ok(rows.into_iter().map(|row| row.track).collect())
    }

    fn recoveries_due(
        &self,
        spool_id: SpoolIndex,
        after: Option<QueuedTrack>,
        until: SlotNumber,
        limit: usize,
    ) -> Result<Vec<QueuedTrack>> {
        ready_tracks(self, RECOVERY, spool_id, after, until, limit)
    }

    fn clear_all_pending_repairs(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_spool(self, REPAIR, spool_id)
    }

    fn clear_all_pending_recoveries(&self, spool_id: SpoolIndex) -> Result<()> {
        clear_spool(self, RECOVERY, spool_id)
    }

    fn pending_repair_spools(&self) -> Result<Vec<SpoolIndex>> {
        spools_with_rows(self, REPAIR.index)
    }

    fn pending_recovery_spools(&self) -> Result<Vec<SpoolIndex>> {
        spools_with_rows(self, RECOVERY.index)
    }

}

/// The slot the node has applied the chain up to, the clock both queues run on
fn applied_slot<S: Store>(store: &TapeStore<S>) -> Result<SlotNumber> {
    Ok(store.get_sync_cursor()?.unwrap_or_default())
}

/// Queue a track at `ready`, unless it already has a row
fn enqueue<S: Store>(
    store: &TapeStore<S>,
    queue: Queue,
    spool_id: SpoolIndex,
    track_address: Address,
    ready: SlotNumber,
) -> Result<()> {
    let index_key = encode_index(spool_id, track_address)?;
    let raw = store.inner().inner();

    let _held = store.lock_queues();
    if raw.contains(queue.index, &index_key)? {
        return Ok(());
    }

    let mut batch = WriteBatch::new();
    batch.put_owned(queue.rows, encode_row(spool_id, ready, track_address)?, Vec::new());
    batch.put_owned(queue.index, index_key, encode_slot(ready)?);
    raw.write_batch(batch)?;
    Ok(())
}

/// Move a queued track's row to `ready`; a track with no row stays out
fn requeue<S: Store>(
    store: &TapeStore<S>,
    queue: Queue,
    spool_id: SpoolIndex,
    track_address: Address,
    ready: SlotNumber,
) -> Result<()> {
    let index_key = encode_index(spool_id, track_address)?;
    let raw = store.inner().inner();

    let _held = store.lock_queues();
    let Some(current) = ready_slot(store, queue, &index_key)? else {
        return Ok(());
    };
    if current == ready {
        return Ok(());
    }

    let mut batch = WriteBatch::new();
    batch.delete_owned(queue.rows, encode_row(spool_id, current, track_address)?);
    batch.put_owned(queue.rows, encode_row(spool_id, ready, track_address)?, Vec::new());
    batch.put_owned(queue.index, index_key, encode_slot(ready)?);
    raw.write_batch(batch)?;
    Ok(())
}

/// Drop a track's row and its index entry
///
/// A track with no row sends no delete, since on the reel a delete of a
/// missing key still leaves a grave.
fn dequeue<S: Store>(
    store: &TapeStore<S>,
    queue: Queue,
    spool_id: SpoolIndex,
    track_address: Address,
) -> Result<()> {
    let index_key = encode_index(spool_id, track_address)?;
    let raw = store.inner().inner();

    let _held = store.lock_queues();
    let Some(current) = ready_slot(store, queue, &index_key)? else {
        return Ok(());
    };

    let mut batch = WriteBatch::new();
    batch.delete_owned(queue.rows, encode_row(spool_id, current, track_address)?);
    batch.delete_owned(queue.index, index_key);
    raw.write_batch(batch)?;
    Ok(())
}

fn is_queued<S: Store>(
    store: &TapeStore<S>,
    queue: Queue,
    spool_id: SpoolIndex,
    track_address: Address,
) -> Result<bool> {
    let index_key = encode_index(spool_id, track_address)?;
    Ok(store.inner().inner().contains(queue.index, &index_key)?)
}

/// A row's ready slot, read from the queue's index
fn ready_slot<S: Store>(store: &TapeStore<S>, queue: Queue, index_key: &[u8]) -> Result<Option<SlotNumber>> {
    let Some(bytes) = store.inner().inner().get(queue.index, index_key)? else {
        return Ok(None);
    };
    let slot = wincode::deserialize(&bytes)
        .map_err(|e| TapeStoreError::Serialization(format!("queue ready slot: {}", e)))?;
    Ok(Some(slot))
}

/// One spool's rows ready by `until`, in key order from the row after `after`
///
/// Rows sort by ready slot, so the walk ends at the first row past `until` and
/// reads at most one row past what it returns.
fn ready_tracks<S: Store>(
    store: &TapeStore<S>,
    queue: Queue,
    spool_id: SpoolIndex,
    after: Option<QueuedTrack>,
    until: SlotNumber,
    limit: usize,
) -> Result<Vec<QueuedTrack>> {
    let mut rows = Vec::new();
    if limit == 0 {
        return Ok(rows);
    }

    let prefix = QueueKey::spool_prefix(spool_id)?;
    let start = match after {
        Some(row) => encode_row(spool_id, row.ready, row.track)?,
        None => prefix.to_vec(),
    };

    for (key_bytes, _) in store.inner().inner().iter_from(queue.rows, &start, Direction::Asc)? {
        if !key_bytes.starts_with(&prefix) {
            break;
        }
        let key: QueueKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("queue key: {}", e)))?;
        let row = QueuedTrack { ready: key.ready, track: key.track_address };
        if Some(row) == after {
            continue;
        }
        if row.ready > until {
            break;
        }
        rows.push(row);
        if rows.len() == limit {
            break;
        }
    }
    Ok(rows)
}

/// Delete every row of one spool's queue with its index entry, a batch at a time
fn clear_spool<S: Store>(store: &TapeStore<S>, queue: Queue, spool_id: SpoolIndex) -> Result<()> {
    let prefix = QueueKey::spool_prefix(spool_id)?;
    let raw = store.inner().inner();

    let _held = store.lock_queues();
    let keys = raw.iter_keys_prefix(queue.rows, &prefix)?;
    for chunk in keys.chunks(CLEAR_BATCH) {
        let mut batch = WriteBatch::new();
        for key_bytes in chunk {
            let key: QueueKey = wincode::deserialize(key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("queue key: {}", e)))?;
            batch.delete(queue.rows, key_bytes);
            batch.delete_owned(queue.index, encode_index(key.spool_id, key.track_address)?);
        }
        raw.write_batch(batch)?;
    }
    Ok(())
}

fn encode_row(spool_id: SpoolIndex, ready: SlotNumber, track_address: Address) -> Result<Vec<u8>> {
    wincode::serialize(&QueueKey::new(spool_id, ready, track_address))
        .map_err(|e| TapeStoreError::Serialization(format!("queue key: {}", e)))
}

fn encode_index(spool_id: SpoolIndex, track_address: Address) -> Result<Vec<u8>> {
    wincode::serialize(&SliceKey::new(spool_id, track_address))
        .map_err(|e| TapeStoreError::Serialization(format!("queue index key: {}", e)))
}

fn encode_slot(slot: SlotNumber) -> Result<Vec<u8>> {
    wincode::serialize(&slot)
        .map_err(|e| TapeStoreError::Serialization(format!("queue ready slot: {}", e)))
}

/// The spools with at least one row in a queue index, in spool order
///
/// A spool costs one seek however many rows it has: the walk reads the spool's
/// first key and jumps to the next spool's prefix. A node holds a row of spool
/// state for every stored spool, so asking each one for its queue costs
/// thousands of scans where this costs one for an empty column.
fn spools_with_rows<S: Store>(store: &TapeStore<S>, cf_name: &str) -> Result<Vec<SpoolIndex>> {
    let raw = store.inner().inner();
    let mut spools = Vec::new();
    let mut start = SliceKey::spool_prefix(SpoolIndex(0))?;

    loop {
        let first = raw.iter_from(cf_name, &start, Direction::Asc)?.next();
        let Some((key_bytes, _)) = first else {
            return Ok(spools);
        };
        let key: SliceKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("pending key: {}", e)))?;
        spools.push(key.spool_id);

        let (_, next) = SliceKey::spool_key_range(key.spool_id)?;
        let Some(next) = next else {
            return Ok(spools);
        };
        start = next;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use tape_store_api::{StoreEngine, StoreIter, Value};
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

    /// The repair rows due at the applied slot, in queue order
    fn due<S: Store>(store: &TapeStore<S>, spool_id: SpoolIndex) -> Vec<Address> {
        let applied = store.get_sync_cursor().expect("cursor").unwrap_or_default();
        let rows = store.repairs_due(spool_id, None, applied, 100).expect("due rows");
        rows.into_iter().map(|row| row.track).collect()
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
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
        assert!(store.iter_pending_recoveries(spool_id, 10).unwrap().is_empty());
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
        assert!(due(&store, spool_id).is_empty());
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
        assert!(due(&store, spool_id).is_empty());

        store.set_sync_cursor(SlotNumber(30), None).unwrap();
        assert_eq!(due(&store, spool_id), vec![track]);
    }

    // a deferral leaves a repair the spool already owes ready
    #[test]
    fn deferral_keeps_queued() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        store.add_pending_repair(spool_id, track).unwrap();
        store.defer_pending_repair(spool_id, track, SlotNumber(30)).unwrap();

        assert_eq!(due(&store, spool_id), vec![track]);
    }

    // an add on a deferred row keeps its slot, so scan and sync can't overwrite the grace
    #[test]
    fn no_overwrite() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        store.defer_pending_repair(spool_id, track, SlotNumber(30)).unwrap();
        store.add_pending_repair(spool_id, track).unwrap();
        store.add_pending_recovery(spool_id, track).unwrap();
        store.set_sync_cursor(SlotNumber(5), None).unwrap();
        store.add_pending_recovery(spool_id, track).unwrap();

        assert!(due(&store, spool_id).is_empty());
        let recovery = store.recoveries_due(spool_id, None, SlotNumber(0), 10).unwrap();
        assert_eq!(recovery, vec![QueuedTrack { ready: SlotNumber(0), track }]);
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

        assert_eq!(due(&store, spool_id).len(), 3);
    }

    // rows come back by ready slot, then by track
    #[test]
    fn ready_order() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        store.defer_pending_repair(spool_id, address(1), SlotNumber(9)).unwrap();
        store.set_sync_cursor(SlotNumber(7), None).unwrap();
        store.add_pending_repair(spool_id, address(3)).unwrap();
        store.add_pending_repair(spool_id, address(2)).unwrap();
        store.set_sync_cursor(SlotNumber(9), None).unwrap();

        assert_eq!(due(&store, spool_id), vec![address(2), address(3), address(1)]);
    }

    // a row put back goes behind every row queued before it, at its slot and at the same one
    #[test]
    fn requeue_behind() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        store.set_sync_cursor(SlotNumber(10), None).unwrap();

        for byte in 1..=3 {
            store.add_pending_repair(spool_id, address(byte)).unwrap();
            store.add_pending_recovery(spool_id, address(byte)).unwrap();
        }
        store.requeue_repair(spool_id, address(1)).unwrap();
        store.requeue_recovery(spool_id, address(1)).unwrap();

        assert_eq!(due(&store, spool_id), vec![address(2), address(3)]);
        let rows = store.repairs_due(spool_id, None, SlotNumber(11), 10).unwrap();
        let tracks: Vec<Address> = rows.iter().map(|row| row.track).collect();
        assert_eq!(tracks, vec![address(2), address(3), address(1)]);
        assert_eq!(rows[2].ready, SlotNumber(10).saturating_add(REQUEUE_SLOTS));
        assert_eq!(
            store.iter_pending_recoveries(spool_id, 10).unwrap(),
            vec![address(2), address(3), address(1)]
        );
        assert!(store.has_pending_repair(spool_id, address(1)).unwrap());
    }

    // putting back a track with no row leaves it out of the queue
    #[test]
    fn requeue_absent() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        store.requeue_repair(spool_id, address(1)).unwrap();
        store.requeue_recovery(spool_id, address(1)).unwrap();

        assert!(!store.has_pending_repair(spool_id, address(1)).unwrap());
        assert!(store.repairs_due(spool_id, None, SlotNumber(u64::MAX), 10).unwrap().is_empty());
        assert!(store.iter_pending_recoveries(spool_id, 10).unwrap().is_empty());
    }

    // a read resumes after the row it was handed and stops at its bound and its limit
    #[test]
    fn resume_after() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        for byte in 1..=5 {
            store.defer_pending_repair(spool_id, address(byte), SlotNumber(u64::from(byte))).unwrap();
        }

        let first = store.repairs_due(spool_id, None, SlotNumber(4), 2).unwrap();
        assert_eq!(first.len(), 2);
        let rest = store.repairs_due(spool_id, first.last().copied(), SlotNumber(4), 10).unwrap();
        let tracks: Vec<Address> = rest.iter().map(|row| row.track).collect();
        assert_eq!(tracks, vec![address(3), address(4)]);
        assert!(store.repairs_due(spool_id, rest.last().copied(), SlotNumber(4), 10).unwrap().is_empty());
    }

    /// A memory store that counts the rows its walks hand out
    struct CountingStore {
        inner: MemoryStore,
        pulled: AtomicUsize,
    }

    impl Store for CountingStore {
        fn store_engine(&self) -> StoreEngine {
            self.inner.store_engine()
        }
        fn get(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<Option<Value>> {
            self.inner.get(cf, key)
        }
        fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> tape_store_api::Result<()> {
            self.inner.put(cf, key, value)
        }
        fn delete(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<()> {
            self.inner.delete(cf, key)
        }
        fn contains(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<bool> {
            self.inner.contains(cf, key)
        }
        fn write_batch(&self, batch: WriteBatch) -> tape_store_api::Result<()> {
            self.inner.write_batch(batch)
        }
        fn iter(&self, cf: &str) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter(cf)
        }
        fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_prefix(cf, prefix)
        }
        fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> tape_store_api::Result<Option<u64>> {
            self.inner.bytes_prefix(cf, prefix)
        }
        fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> tape_store_api::Result<StoreIter<'_>> {
            let rows = self.inner.iter_from(cf, start, direction)?;
            Ok(Box::new(rows.inspect(|_| {
                self.pulled.fetch_add(1, Ordering::Relaxed);
            })))
        }
        fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_range(cf, start, end)
        }
        fn live_data_size_bytes(&self) -> tape_store_api::Result<Option<u64>> {
            self.inner.live_data_size_bytes()
        }
        fn key_count_estimate(&self, cf: &str) -> tape_store_api::Result<Option<u64>> {
            self.inner.key_count_estimate(cf)
        }
    }

    // with 276k rows queued, a due read takes at most one row past what it
    // returns, whether its limit or its slot bound ends it
    #[test]
    fn bounded_read() {
        const QUEUED: u64 = 276_074;
        let store = TapeStore::new(CountingStore { inner: MemoryStore::new(), pulled: AtomicUsize::new(0) });
        let spool_id = SpoolIndex(43);
        for slot in 0..QUEUED {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&slot.to_be_bytes());
            store.defer_pending_repair(spool_id, Address::new(bytes), SlotNumber(slot)).unwrap();
        }
        let pulled = |store: &TapeStore<CountingStore>| store.inner().inner().pulled.swap(0, Ordering::Relaxed);
        pulled(&store);

        let page = store.repairs_due(spool_id, None, SlotNumber(QUEUED), 64).unwrap();
        assert_eq!(page.len(), 64);
        assert_eq!(pulled(&store), 64);

        let next = store.repairs_due(spool_id, page.last().copied(), SlotNumber(QUEUED), 64).unwrap();
        assert_eq!(next.first().map(|row| row.ready), Some(SlotNumber(64)));
        assert_eq!(pulled(&store), 65);

        let early = store.repairs_due(spool_id, None, SlotNumber(9), 64).unwrap();
        assert_eq!(early.len(), 10);
        assert_eq!(pulled(&store), 11);
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
        assert!(!store.has_pending_recovery(SpoolIndex(42), t1).unwrap());
        assert_eq!(store.iter_pending_recoveries(SpoolIndex(99), 100).unwrap().len(), 1);
        assert_eq!(store.pending_recovery_spools().unwrap(), vec![SpoolIndex(99)]);
    }

    // a clear takes every row and index entry, over more rows than one batch holds
    #[test]
    fn clear_all_pending_repairs() {
        let store = test_store();

        let tracks: Vec<Address> = (0..CLEAR_BATCH + 3).map(|_| Address::new_unique()).collect();
        for track in &tracks {
            store.add_pending_repair(SpoolIndex(42), *track).unwrap();
        }
        let t3 = Address::new_unique();
        store.add_pending_repair(SpoolIndex(99), t3).unwrap();

        store.clear_all_pending_repairs(SpoolIndex(42)).unwrap();

        assert!(due(&store, SpoolIndex(42)).is_empty());
        for track in &tracks {
            assert!(!store.has_pending_repair(SpoolIndex(42), *track).unwrap());
        }
        assert_eq!(due(&store, SpoolIndex(99)), vec![t3]);
        assert_eq!(store.pending_repair_spools().unwrap(), vec![SpoolIndex(99)]);
    }

    // each spool with a queued row is listed once, in order, whatever its row count
    #[test]
    fn pending_spools() {
        let store = test_store();

        assert!(store.pending_repair_spools().unwrap().is_empty());
        assert!(store.pending_recovery_spools().unwrap().is_empty());

        for _ in 0..3 {
            store.add_pending_repair(SpoolIndex(300), Address::new_unique()).unwrap();
        }
        store.add_pending_repair(SpoolIndex(7), Address::new_unique()).unwrap();
        store.defer_pending_repair(SpoolIndex(4_099), Address::new_unique(), SlotNumber(30)).unwrap();
        store.add_pending_recovery(SpoolIndex(42), Address::new_unique()).unwrap();

        assert_eq!(
            store.pending_repair_spools().unwrap(),
            vec![SpoolIndex(7), SpoolIndex(300), SpoolIndex(4_099)]
        );
        assert_eq!(store.pending_recovery_spools().unwrap(), vec![SpoolIndex(42)]);
    }

    // the last spool id has no next prefix, and the walk ends on it
    #[test]
    fn pending_spools_last() {
        let store = test_store();
        let last = SpoolIndex(u64::from(u16::MAX));
        store.add_pending_recovery(last, Address::new_unique()).unwrap();
        store.add_pending_recovery(last, Address::new_unique()).unwrap();

        assert_eq!(store.pending_recovery_spools().unwrap(), vec![last]);
    }
}

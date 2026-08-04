//! Slice data operations (merged primary + recovery)

use store::{Column, Store, WriteBatch};
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;

use crate::columns::{SliceCol, SliceSizeCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{SliceKey, SliceValue};
use crate::TapeStore;

/// Entries staged before a rebuild flushes its batch
const REBUILD_BATCH_LEN: usize = 4096;

/// Operations for slice data storage
pub trait SliceOps {
    /// Get slice data
    fn get_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<Option<Vec<u8>>>;

    /// Store slice data
    fn put_slice(&self, spool_id: SpoolIndex, track_address: Address, data: Vec<u8>) -> Result<()>;

    /// Delete slice data
    fn delete_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    /// Check if a slice exists without loading data
    fn has_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    /// Iterate slices by spool
    fn iter_slices_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<(Address, Vec<u8>)>>;

    /// Paginated slice iteration by spool. Returns up to `limit` slices
    /// starting after `after_track` (or from the beginning if None).
    fn iter_slices_by_spool_from(
        &self,
        spool_id: SpoolIndex,
        after_track: Option<Address>,
        limit: usize,
    ) -> Result<Vec<(Address, Vec<u8>)>>;

    /// Iterate slice keys (track addresses) by spool without loading data.
    fn iter_slice_keys_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<Address>>;

    /// Count slices in a spool without loading data.
    fn count_slices_by_spool(&self, spool_id: SpoolIndex) -> Result<usize>;

    /// Slice count and total payload bytes for a spool, without loading data
    fn slice_totals_by_spool(&self, spool_id: SpoolIndex) -> Result<(u64, StorageUnits)>;

    /// Slice count and total payload bytes across every spool, without loading data
    fn slice_totals(&self) -> Result<(u64, StorageUnits)>;

    /// Delete all slices for a spool with a single range delete.
    fn delete_all_slices_for_spool(&self, spool_id: SpoolIndex) -> Result<()>;

    /// Rebuild the size index if it has drifted from the slice column
    ///
    /// Returns true when a rebuild ran
    fn ensure_slice_size_index(&self) -> Result<bool>;
}

impl<S: Store> SliceOps for TapeStore<S> {
    fn get_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<Option<Vec<u8>>> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.get::<SliceCol>(&key)?.map(|value| value.0))
    }

    fn put_slice(&self, spool_id: SpoolIndex, track_address: Address, data: Vec<u8>) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        let key_bytes = serialize_slice_key(&key)?;
        let size_bytes = serialize_size(StorageUnits(data.len() as u64))?;
        let value_bytes = wincode::serialize(&SliceValue(data))
            .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;

        // Both families share a volume, so one batch keeps a payload and its
        // recorded length from ever disagreeing. Hand the serialized bytes over
        // rather than copying the payload into the batch.
        let mut batch = WriteBatch::new();
        batch.put_owned(SliceCol::CF_NAME, key_bytes.clone(), value_bytes);
        batch.put_owned(SliceSizeCol::CF_NAME, key_bytes, size_bytes);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn delete_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = SliceKey::new(spool_id, track_address);
        let key_bytes = serialize_slice_key(&key)?;

        let mut batch = WriteBatch::new();
        batch.delete_owned(SliceCol::CF_NAME, key_bytes.clone());
        batch.delete_owned(SliceSizeCol::CF_NAME, key_bytes);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn has_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SliceCol>(&key)?)
    }

    fn iter_slices_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<(Address, Vec<u8>)>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SliceCol::CF_NAME, &prefix)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            let data: SliceValue = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;
            results.push((key.track_address, data.0));
        }
        Ok(results)
    }

    fn iter_slices_by_spool_from(
        &self,
        spool_id: SpoolIndex,
        after_track: Option<Address>,
        limit: usize,
    ) -> Result<Vec<(Address, Vec<u8>)>> {
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
            let data: SliceValue = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;
            results.push((key.track_address, data.0));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    fn iter_slice_keys_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<Address>> {
        let prefix = SliceKey::spool_prefix(spool_id);
        // Keys-only scan: the spool's slice values live in blob files, so reading
        // them just to extract the track address is wasted I/O.
        let keys = self
            .inner()
            .inner()
            .iter_keys_prefix(SliceCol::CF_NAME, &prefix)?;

        let mut results = Vec::with_capacity(keys.len());
        for key_bytes in keys {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            results.push(key.track_address);
        }
        Ok(results)
    }

    fn count_slices_by_spool(&self, spool_id: SpoolIndex) -> Result<usize> {
        let prefix = SliceKey::spool_prefix(spool_id);
        // Keys-only: never read/copy the (blob) values just to count them.
        Ok(self
            .inner()
            .inner()
            .iter_keys_prefix(SliceCol::CF_NAME, &prefix)?
            .len())
    }

    fn slice_totals_by_spool(&self, spool_id: SpoolIndex) -> Result<(u64, StorageUnits)> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SliceSizeCol::CF_NAME, &prefix)?;

        let mut count = 0u64;
        let mut bytes = 0u64;
        for (_, value_bytes) in iter {
            bytes = bytes.saturating_add(deserialize_size(&value_bytes)?.as_u64());
            count += 1;
        }
        Ok((count, StorageUnits(bytes)))
    }

    fn slice_totals(&self) -> Result<(u64, StorageUnits)> {
        let iter = self.inner().inner().iter(SliceSizeCol::CF_NAME)?;

        let mut count = 0u64;
        let mut bytes = 0u64;
        for (_, value_bytes) in iter {
            bytes = bytes.saturating_add(deserialize_size(&value_bytes)?.as_u64());
            count += 1;
        }
        Ok((count, StorageUnits(bytes)))
    }

    fn delete_all_slices_for_spool(&self, spool_id: SpoolIndex) -> Result<()> {
        let raw = self.inner().inner();

        // A spool's slices occupy the contiguous key range [spool, spool+1);
        // drop them with one range tombstone.
        let (start, end) = SliceKey::spool_key_range(spool_id);
        match end {
            Some(end) => {
                raw.delete_range(SliceCol::CF_NAME, &start, &end)?;
                raw.delete_range(SliceSizeCol::CF_NAME, &start, &end)?;
            }
            None => {
                // The max spool prefix has no exclusive successor; fall back to
                // collecting keys and batch-deleting them.
                let keys = raw.iter_keys_prefix(SliceCol::CF_NAME, &start)?;
                let mut batch = WriteBatch::new();
                for key in &keys {
                    batch.delete(SliceCol::CF_NAME, key);
                    batch.delete(SliceSizeCol::CF_NAME, key);
                }
                raw.write_batch(batch)?;
            }
        }
        Ok(())
    }

    fn ensure_slice_size_index(&self) -> Result<bool> {
        let raw = self.inner().inner();

        let slices = raw.iter_keys_prefix(SliceCol::CF_NAME, &[])?;
        let sizes = raw.iter_keys_prefix(SliceSizeCol::CF_NAME, &[])?;
        if slices.len() == sizes.len() {
            return Ok(false);
        }

        // Interrupting the rebuild leaves a short index, which the same count
        // check catches on the next open.
        let mut batch = WriteBatch::new();
        for key in &sizes {
            batch.delete(SliceSizeCol::CF_NAME, key);
        }
        raw.write_batch(batch)?;

        let mut batch = WriteBatch::new();
        let mut staged = 0usize;
        for (key_bytes, value_bytes) in raw.iter(SliceCol::CF_NAME)? {
            let value: SliceValue = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice value: {}", e)))?;
            let size_bytes = serialize_size(StorageUnits(value.0.len() as u64))?;
            batch.put_owned(SliceSizeCol::CF_NAME, key_bytes, size_bytes);
            staged += 1;
            if staged == REBUILD_BATCH_LEN {
                raw.write_batch(std::mem::replace(&mut batch, WriteBatch::new()))?;
                staged = 0;
            }
        }
        if !batch.is_empty() {
            raw.write_batch(batch)?;
        }
        Ok(true)
    }
}

fn serialize_slice_key(key: &SliceKey) -> Result<Vec<u8>> {
    wincode::serialize(key)
        .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))
}

fn serialize_size(size: StorageUnits) -> Result<Vec<u8>> {
    wincode::serialize(&size)
        .map_err(|e| TapeStoreError::Serialization(format!("slice size: {}", e)))
}

fn deserialize_size(bytes: &[u8]) -> Result<StorageUnits> {
    wincode::deserialize(bytes)
        .map_err(|e| TapeStoreError::Serialization(format!("slice size: {}", e)))
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
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        let data = vec![0xAB; 1024];

        assert!(store.get_slice(spool_id, track).unwrap().is_none());

        store
            .put_slice(spool_id, track, data.clone())
            .unwrap();

        let retrieved = store.get_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    // Validates that stored slices larger than the default wincode vector cap still roundtrip.
    #[test]
    fn slice_large() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();
        let data = vec![0xAB; (4 * 1024 * 1024) + 1];

        store.put_slice(spool_id, track, data.clone()).unwrap();

        let retrieved = store.get_slice(spool_id, track).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_delete_slice() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        let data = vec![0u8; 100];

        store.put_slice(spool_id, track, data).unwrap();
        assert!(store.get_slice(spool_id, track).unwrap().is_some());

        store.delete_slice(spool_id, track).unwrap();
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[test]
    fn test_iter_slices_by_spool() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        let track1 = Address::new_unique();
        let track2 = Address::new_unique();
        let track3 = Address::new_unique();

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
            .put_slice(SpoolIndex(99), Address::new_unique(), vec![99])
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
        let spool_id = SpoolIndex(42);
        let track = Address::new_unique();

        assert!(!store.has_slice(spool_id, track).unwrap());

        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();
        assert!(store.has_slice(spool_id, track).unwrap());

        store.delete_slice(spool_id, track).unwrap();
        assert!(!store.has_slice(spool_id, track).unwrap());
    }

    #[test]
    fn test_iter_slices_by_spool_from() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        let mut tracks = Vec::new();
        for i in 0..5 {
            let track = Address::new_unique();
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
        let empty = store.iter_slices_by_spool_from(SpoolIndex(99), None, 10).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_iter_slice_keys_by_spool() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        let track1 = Address::new_unique();
        let track2 = Address::new_unique();

        store.put_slice(spool_id, track1, vec![1; 1024]).unwrap();
        store.put_slice(spool_id, track2, vec![2; 1024]).unwrap();
        store.put_slice(SpoolIndex(99), Address::new_unique(), vec![3; 1024]).unwrap();

        let keys = store.iter_slice_keys_by_spool(spool_id).unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn delete_all_for_spool() {
        let store = test_store();

        let t1 = Address::new_unique();
        let t2 = Address::new_unique();
        let t3 = Address::new_unique();

        store.put_slice(SpoolIndex(42), t1, vec![1]).unwrap();
        store.put_slice(SpoolIndex(42), t2, vec![2]).unwrap();
        store.put_slice(SpoolIndex(99), t3, vec![3]).unwrap();

        store.delete_all_slices_for_spool(SpoolIndex(42)).unwrap();
        assert_eq!(store.count_slices_by_spool(SpoolIndex(42)).unwrap(), 0);
        assert_eq!(store.count_slices_by_spool(SpoolIndex(99)).unwrap(), 1);
    }

    #[test]
    fn delete_all_for_spool_keeps_neighbors() {
        // Deleting spool N must leave N-1 and N+1 intact — the range bounds must
        // be exact, not bleed past the [spool, spool+1) prefix.
        let store = test_store();
        let prev = Address::new_unique();
        let mid_a = Address::new_unique();
        let mid_b = Address::new_unique();
        let next = Address::new_unique();

        store.put_slice(SpoolIndex(41), prev, vec![0xAA]).unwrap();
        store.put_slice(SpoolIndex(42), mid_a, vec![0xBB]).unwrap();
        store.put_slice(SpoolIndex(42), mid_b, vec![0xCC]).unwrap();
        store.put_slice(SpoolIndex(43), next, vec![0xDD]).unwrap();

        store.delete_all_slices_for_spool(SpoolIndex(42)).unwrap();

        assert_eq!(store.count_slices_by_spool(SpoolIndex(41)).unwrap(), 1);
        assert_eq!(store.count_slices_by_spool(SpoolIndex(42)).unwrap(), 0);
        assert_eq!(store.count_slices_by_spool(SpoolIndex(43)).unwrap(), 1);
        assert_eq!(store.get_slice(SpoolIndex(41), prev).unwrap().unwrap(), vec![0xAA]);
        assert_eq!(store.get_slice(SpoolIndex(43), next).unwrap().unwrap(), vec![0xDD]);
    }

    #[test]
    fn test_count_slices_by_spool() {
        let store = test_store();
        let spool_id = SpoolIndex(42);

        assert_eq!(store.count_slices_by_spool(spool_id).unwrap(), 0);

        for i in 0..5 {
            store.put_slice(spool_id, Address::new_unique(), vec![i]).unwrap();
        }
        store.put_slice(SpoolIndex(99), Address::new_unique(), vec![99]).unwrap();

        assert_eq!(store.count_slices_by_spool(spool_id).unwrap(), 5);
        assert_eq!(store.count_slices_by_spool(SpoolIndex(99)).unwrap(), 1);
        assert_eq!(store.count_slices_by_spool(SpoolIndex(0)).unwrap(), 0);
    }

    /// Sum a spool's payload lengths the slow way, straight from the blobs.
    fn scanned_totals<S: Store>(store: &TapeStore<S>, spool_id: SpoolIndex) -> (u64, StorageUnits) {
        let slices = store
            .iter_slices_by_spool(spool_id)
            .expect("iter slices by spool");
        let bytes = slices.iter().map(|(_, data)| data.len() as u64).sum();
        (slices.len() as u64, StorageUnits(bytes))
    }

    // totals track the payloads through writes and deletes
    #[test]
    fn totals_follow_writes() {
        let store = test_store();
        let spool_id = SpoolIndex(42);
        let kept = Address::new_unique();
        let removed = Address::new_unique();

        store.put_slice(spool_id, kept, vec![0xAB; 300]).expect("put kept");
        store.put_slice(spool_id, removed, vec![0xCD; 700]).expect("put removed");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (2, StorageUnits(1000))
        );
        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            scanned_totals(&store, spool_id)
        );

        store.delete_slice(spool_id, removed).expect("delete removed");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (1, StorageUnits(300))
        );
        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            scanned_totals(&store, spool_id)
        );
    }

    // overwriting a slice replaces its recorded length rather than adding to it
    #[test]
    fn totals_after_overwrite() {
        let store = test_store();
        let spool_id = SpoolIndex(7);
        let track = Address::new_unique();

        store.put_slice(spool_id, track, vec![0u8; 900]).expect("first put");
        store.put_slice(spool_id, track, vec![0u8; 100]).expect("second put");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (1, StorageUnits(100))
        );
    }

    // a range delete clears the size index alongside the payloads
    #[test]
    fn range_delete_clears_sizes() {
        let store = test_store();

        store.put_slice(SpoolIndex(41), Address::new_unique(), vec![1; 10]).expect("put prev");
        store.put_slice(SpoolIndex(42), Address::new_unique(), vec![2; 20]).expect("put mid");
        store.put_slice(SpoolIndex(43), Address::new_unique(), vec![3; 30]).expect("put next");

        store
            .delete_all_slices_for_spool(SpoolIndex(42))
            .expect("delete spool");

        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(42)).expect("totals"),
            (0, StorageUnits(0))
        );
        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(41)).expect("totals"),
            (1, StorageUnits(10))
        );
        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(43)).expect("totals"),
            (1, StorageUnits(30))
        );
    }

    // the max spool prefix has no successor, so its fallback path must clear sizes too
    #[test]
    fn range_delete_max_spool() {
        let store = test_store();
        let spool_id = SpoolIndex(u16::MAX as u64);

        store.put_slice(spool_id, Address::new_unique(), vec![9; 50]).expect("put max");

        store
            .delete_all_slices_for_spool(spool_id)
            .expect("delete spool");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (0, StorageUnits(0))
        );
    }

    // a store whose index went missing rebuilds it on the invariant check
    #[test]
    fn rebuild_repairs_index() {
        let store = test_store();
        let spool_id = SpoolIndex(3);
        let track = Address::new_unique();

        store.put_slice(spool_id, track, vec![0xEE; 512]).expect("put slice");

        let key = serialize_slice_key(&SliceKey::new(spool_id, track)).expect("serialize key");
        store
            .inner()
            .inner()
            .delete(SliceSizeCol::CF_NAME, &key)
            .expect("drop size entry");
        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (0, StorageUnits(0))
        );

        assert!(store.ensure_slice_size_index().expect("ensure index"));

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (1, StorageUnits(512))
        );
    }

    // whole-store totals span every spool and follow deletes
    #[test]
    fn totals_across_spools() {
        let store = test_store();
        let dropped = Address::new_unique();

        store.put_slice(SpoolIndex(1), Address::new_unique(), vec![0; 100]).expect("put one");
        store.put_slice(SpoolIndex(2), Address::new_unique(), vec![0; 250]).expect("put two");
        store.put_slice(SpoolIndex(2), dropped, vec![0; 50]).expect("put dropped");

        assert_eq!(store.slice_totals().expect("totals"), (3, StorageUnits(400)));

        store.delete_slice(SpoolIndex(2), dropped).expect("delete");

        assert_eq!(store.slice_totals().expect("totals"), (2, StorageUnits(350)));
    }

    // a consistent index is left alone
    #[test]
    fn rebuild_skips_intact_index() {
        let store = test_store();
        store
            .put_slice(SpoolIndex(1), Address::new_unique(), vec![1; 64])
            .expect("put slice");

        assert!(!store.ensure_slice_size_index().expect("ensure index"));
    }
}

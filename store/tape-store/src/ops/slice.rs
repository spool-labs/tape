//! Slice data operations (merged primary + recovery)

use store::{Column, Store};
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;

use crate::columns::SliceCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{slice, SliceKey, SliceWrite};
use crate::TapeStore;

/// Operations for slice data storage
pub trait SliceOps {
    /// Get slice data
    fn get_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<Option<Vec<u8>>>;

    /// Store slice data with the sidecar its bytes imply
    ///
    /// Takes anything that converts into a `SliceWrite`, so a caller with raw
    /// bytes passes them straight in and one that already derived the sidecar to
    /// check the slice root hands that over instead of paying for it twice.
    fn put_slice(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        slice: impl Into<SliceWrite>,
    ) -> Result<()>;

    /// Delete slice data
    fn delete_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()>;

    /// Check if a slice exists without loading data
    fn has_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool>;

    /// The sidecar of a slice and a window of its payload, in two ranged reads
    ///
    /// What a storage challenge answers from: the nodes above the sample window
    /// and the window itself, never the whole slice. Nothing comes back when the
    /// slice is absent. A backend with no partial read answers each of the two
    /// with the whole value, which is what it did for the single read before.
    fn slice_window(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        offset: usize,
        len: usize,
    ) -> Result<Option<(Vec<tape_crypto::Hash>, Vec<u8>)>>;

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

    /// One page of a spool's slices, in no promised order, resumed by a mark.
    ///
    /// The spool is the slice key's shard prefix, so this is one shard's walk on
    /// either backend rather than a scan of the family.
    fn sweep_slices_by_spool(
        &self,
        spool_id: SpoolIndex,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Address, Vec<u8>)>, Option<Vec<u8>>)>;

    /// One page of a spool's slice keys, in no promised order, resumed by a mark.
    fn sweep_slice_keys_by_spool(
        &self,
        spool_id: SpoolIndex,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Address>, Option<Vec<u8>>)>;

    /// Iterate slice keys (track addresses) by spool without loading data.
    fn iter_slice_keys_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<Address>>;

    /// Iterate each track in a spool with the byte length of its slice.
    ///
    /// Reads the slices, so it is tooling rather than something to run on a
    /// request. Ordered by track address, which the spool-prefixed key gives for
    /// free.
    fn iter_slice_sizes_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<(Address, StorageUnits)>>;

    /// Count slices in a spool without loading data.
    fn count_slices_by_spool(&self, spool_id: SpoolIndex) -> Result<usize>;

    /// Slice count and stored bytes for a spool, without loading data
    ///
    /// Stored bytes are what the backend holds under those keys, sidecars and
    /// framing included, rather than what the caller handed it. Nothing comes back
    /// for the bytes from a backend that could only answer by reading payloads.
    fn slice_totals_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<(u64, Option<StorageUnits>)>;

    /// Slice count and stored bytes across every spool, without loading data
    fn slice_totals(&self) -> Result<(u64, Option<StorageUnits>)>;

    /// Delete all slices for a spool with a single range delete.
    fn delete_all_slices_for_spool(&self, spool_id: SpoolIndex) -> Result<()>;
}

impl<S: Store> SliceOps for TapeStore<S> {
    fn get_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<Option<Vec<u8>>> {
        let key = serialize_slice_key(&SliceKey::new(spool_id, track_address))?;
        let stored = self.inner().inner().get(SliceCol::CF_NAME, &key)?;
        Ok(stored.map(|value| slice::payload(&value).to_vec()))
    }

    fn put_slice(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        slice: impl Into<SliceWrite>,
    ) -> Result<()> {
        // A slice too large for the sub-leaf tree has no sidecar and no provable
        // sample either, so it is stored without one rather than refused here.
        let (data, sidecar) = slice.into().into_parts();

        let key = serialize_slice_key(&SliceKey::new(spool_id, track_address))?;
        let value = slice::fuse(&data, sidecar.as_deref());
        self.inner().inner().put(SliceCol::CF_NAME, &key, &value)?;
        Ok(())
    }

    fn delete_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<()> {
        let key = serialize_slice_key(&SliceKey::new(spool_id, track_address))?;
        self.inner().inner().delete(SliceCol::CF_NAME, &key)?;
        Ok(())
    }

    fn has_slice(&self, spool_id: SpoolIndex, track_address: Address) -> Result<bool> {
        let key = SliceKey::new(spool_id, track_address);
        Ok(self.contains::<SliceCol>(&key)?)
    }

    fn slice_window(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        offset: usize,
        len: usize,
    ) -> Result<Option<(Vec<tape_crypto::Hash>, Vec<u8>)>> {
        let key = serialize_slice_key(&SliceKey::new(spool_id, track_address))?;
        let raw = self.inner().inner();

        // The head is asked for at its widest and answers short, so one read has
        // the sidecar and where the payload begins whatever the slice's size.
        let Some(head) = raw.get_range(SliceCol::CF_NAME, &key, 0, slice::MAX_HEAD_BYTES)? else {
            return Ok(None);
        };
        let start = slice::payload_start(&head) as u64;
        let Some(window) = raw.get_range(SliceCol::CF_NAME, &key, start + offset as u64, len)?
        else {
            return Ok(None);
        };
        Ok(Some((slice::sidecar(&head), window.into_vec())))
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
            results.push((key.track_address, slice::payload(&value_bytes).to_vec()));
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
            Some(track) => serialize_slice_key(&SliceKey::new(spool_id, track))?,
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
            results.push((key.track_address, slice::payload(&value_bytes).to_vec()));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    fn sweep_slices_by_spool(
        &self,
        spool_id: SpoolIndex,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Address, Vec<u8>)>, Option<Vec<u8>>)> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let (rows, next) =
            self.inner()
                .inner()
                .sweep_prefix(SliceCol::CF_NAME, &prefix, from, limit)?;

        let mut slices = Vec::with_capacity(rows.len());
        for (key_bytes, value_bytes) in rows {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            slices.push((key.track_address, slice::payload(&value_bytes).to_vec()));
        }
        Ok((slices, next))
    }

    fn sweep_slice_keys_by_spool(
        &self,
        spool_id: SpoolIndex,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Address>, Option<Vec<u8>>)> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let (keys, next) =
            self.inner()
                .inner()
                .sweep_keys_prefix(SliceCol::CF_NAME, &prefix, from, limit)?;

        let mut tracks = Vec::with_capacity(keys.len());
        for key_bytes in keys {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))?;
            tracks.push(key.track_address);
        }
        Ok((tracks, next))
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

    fn iter_slice_sizes_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<Vec<(Address, StorageUnits)>> {
        let slices = self.iter_slices_by_spool(spool_id)?;
        Ok(slices
            .into_iter()
            .map(|(track, data)| (track, StorageUnits(data.len() as u64)))
            .collect())
    }

    fn count_slices_by_spool(&self, spool_id: SpoolIndex) -> Result<usize> {
        let prefix = SliceKey::spool_prefix(spool_id);
        Ok(self.inner().inner().count_prefix(SliceCol::CF_NAME, &prefix)? as usize)
    }

    fn slice_totals_by_spool(
        &self,
        spool_id: SpoolIndex,
    ) -> Result<(u64, Option<StorageUnits>)> {
        let prefix = SliceKey::spool_prefix(spool_id);
        let raw = self.inner().inner();
        let count = raw.count_prefix(SliceCol::CF_NAME, &prefix)?;
        let bytes = raw.bytes_prefix(SliceCol::CF_NAME, &prefix)?;
        Ok((count, bytes.map(StorageUnits)))
    }

    fn slice_totals(&self) -> Result<(u64, Option<StorageUnits>)> {
        let raw = self.inner().inner();
        let count = raw.count_prefix(SliceCol::CF_NAME, &[])?;
        let bytes = raw.bytes_prefix(SliceCol::CF_NAME, &[])?;
        Ok((count, bytes.map(StorageUnits)))
    }

    fn delete_all_slices_for_spool(&self, spool_id: SpoolIndex) -> Result<()> {
        let raw = self.inner().inner();

        // A spool's slices occupy the contiguous key range [spool, spool+1);
        // drop them with one range tombstone.
        let (start, end) = SliceKey::spool_key_range(spool_id);
        match end {
            Some(end) => raw.delete_range(SliceCol::CF_NAME, &start, &end)?,
            None => {
                // The max spool prefix has no exclusive successor; fall back to
                // collecting keys and batch-deleting them.
                let keys = raw.iter_keys_prefix(SliceCol::CF_NAME, &start)?;
                let mut batch = store::WriteBatch::new();
                for key in keys {
                    batch.delete_owned(SliceCol::CF_NAME, key);
                }
                raw.write_batch(batch)?;
            }
        }
        Ok(())
    }
}

fn serialize_slice_key(key: &SliceKey) -> Result<Vec<u8>> {
    wincode::serialize(key)
        .map_err(|e| TapeStoreError::Serialization(format!("slice key: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::erasure::{slice_root, slice_sidecar, SAMPLE_WINDOW_BYTES};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    /// What a slice occupies once stored, which is what the totals report
    fn stored_len(payload: usize) -> u64 {
        let nodes = slice_sidecar(&vec![0u8; payload]).map_or(0, |nodes| nodes.len());
        (slice::ENVELOPE_LEN + nodes * 32 + payload) as u64
    }

    // the root the write path checks and the sidecar the store keeps both fall
    // out of one hash of the slice, so a writer never hashes twice
    #[test]
    fn one_pass() {
        let data: Vec<u8> = (0..300_000).map(|byte| (byte * 13 % 249) as u8).collect();
        let write = SliceWrite::new(data.clone());

        assert_eq!(write.root(), slice_root(&data));
        assert_eq!(write.data(), data.as_slice());

        let store = test_store();
        let spool = SpoolIndex(5);
        let track = Address::new_unique();
        store.put_slice(spool, track, write).unwrap();

        let (sidecar, _) = store.slice_window(spool, track, 0, 1).unwrap().unwrap();
        assert_eq!(Some(sidecar), slice_sidecar(&data));
        assert_eq!(store.get_slice(spool, track).unwrap(), Some(data));
    }

    // a slice and its sidecar are one record, so a delete cannot leave half of it
    #[test]
    fn sidecar_life() {
        let store = test_store();
        let spool = SpoolIndex(3);
        let track = Address::new_unique();
        let data: Vec<u8> = (0..300_000).map(|byte| (byte * 7 % 251) as u8).collect();

        store.put_slice(spool, track, data.clone()).unwrap();
        let (sidecar, _) = store.slice_window(spool, track, 0, 1).unwrap().unwrap();
        assert_eq!(Some(sidecar), slice_sidecar(&data));

        store.delete_slice(spool, track).unwrap();
        assert!(store.slice_window(spool, track, 0, 1).unwrap().is_none());
    }

    // a window comes back from where the payload starts, not from the envelope
    #[test]
    fn windowed_read() {
        let store = test_store();
        let spool = SpoolIndex(6);
        let track = Address::new_unique();
        let data: Vec<u8> = (0..700_000).map(|byte| (byte * 17 % 241) as u8).collect();
        store.put_slice(spool, track, data.clone()).unwrap();

        // The second window, which is where an envelope left in the offset would
        // show up as bytes shifted by its width.
        let (_, window) = store
            .slice_window(spool, track, SAMPLE_WINDOW_BYTES, SAMPLE_WINDOW_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(window, data[SAMPLE_WINDOW_BYTES..2 * SAMPLE_WINDOW_BYTES]);

        // A window running past the end stops at the end, the way a pread does.
        let (_, tail) = store
            .slice_window(spool, track, 2 * SAMPLE_WINDOW_BYTES, SAMPLE_WINDOW_BYTES)
            .unwrap()
            .unwrap();
        assert_eq!(tail, data[2 * SAMPLE_WINDOW_BYTES..]);
    }

    // the sizes a spool reports pair each track with its payload length
    #[test]
    fn slice_sizes() {
        let store = test_store();
        let spool = SpoolIndex(7);
        let other = SpoolIndex(8);

        // Vary the fill. The recorded size is the payload length before any
        // storage-layer encoding, so a constant would pass either way, but a
        // sample set is read to decide what to challenge and a test that cannot
        // tell one slice from another is not worth much.
        let fill = |n: usize, len: usize| -> Vec<u8> {
            (0..len).map(|byte| (byte ^ (n * 37)) as u8).collect()
        };

        let mut expected: Vec<(Address, StorageUnits)> = (1..=3)
            .map(|n| {
                let track = Address::new_unique();
                let len = n * 512;
                store.put_slice(spool, track, fill(n, len)).unwrap();
                (track, StorageUnits::from_bytes(len as u64))
            })
            .collect();
        // A slice in a different spool must not leak into the sample set.
        store
            .put_slice(other, Address::new_unique(), fill(9, 99))
            .unwrap();

        // The order is the challenge's canonical one, so it must be by address.
        expected.sort_unstable_by_key(|(track, _)| *track);
        assert_eq!(store.iter_slice_sizes_by_spool(spool).unwrap(), expected);
        assert!(store.iter_slice_sizes_by_spool(SpoolIndex(99)).unwrap().is_empty());
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

    // a keys-only sweep hands out every track in the spool and nothing else
    #[test]
    fn sweep_keys_by_spool() {
        let store = test_store();
        let spool_id = SpoolIndex(11);

        let mut written = Vec::new();
        for _ in 0..5 {
            let track = Address::new_unique();
            store.put_slice(spool_id, track, vec![0xAB; 64]).unwrap();
            written.push(track);
        }
        store.put_slice(SpoolIndex(12), Address::new_unique(), vec![1]).unwrap();

        let mut swept = Vec::new();
        let mut mark = None;
        loop {
            let (page, next) = store.sweep_slice_keys_by_spool(spool_id, mark.as_deref(), 2).unwrap();
            swept.extend(page);
            match next {
                Some(next) => mark = Some(next),
                None => break,
            }
        }

        written.sort_unstable();
        swept.sort_unstable();
        assert_eq!(swept, written);
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

    // totals track the stored records through writes and deletes
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
            (2, Some(StorageUnits(stored_len(300) + stored_len(700))))
        );

        store.delete_slice(spool_id, removed).expect("delete removed");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (1, Some(StorageUnits(stored_len(300))))
        );
    }

    // overwriting a slice replaces its stored bytes rather than adding to them
    #[test]
    fn totals_after_overwrite() {
        let store = test_store();
        let spool_id = SpoolIndex(7);
        let track = Address::new_unique();

        store.put_slice(spool_id, track, vec![0u8; 900]).expect("first put");
        store.put_slice(spool_id, track, vec![0u8; 100]).expect("second put");

        assert_eq!(
            store.slice_totals_by_spool(spool_id).expect("totals"),
            (1, Some(StorageUnits(stored_len(100))))
        );
    }

    // a range delete clears the spool it names and leaves its neighbours weighed
    #[test]
    fn range_delete_clears_totals() {
        let store = test_store();

        store.put_slice(SpoolIndex(41), Address::new_unique(), vec![1; 10]).expect("put prev");
        store.put_slice(SpoolIndex(42), Address::new_unique(), vec![2; 20]).expect("put mid");
        store.put_slice(SpoolIndex(43), Address::new_unique(), vec![3; 30]).expect("put next");

        store
            .delete_all_slices_for_spool(SpoolIndex(42))
            .expect("delete spool");

        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(42)).expect("totals"),
            (0, Some(StorageUnits(0)))
        );
        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(41)).expect("totals"),
            (1, Some(StorageUnits(stored_len(10))))
        );
        assert_eq!(
            store.slice_totals_by_spool(SpoolIndex(43)).expect("totals"),
            (1, Some(StorageUnits(stored_len(30))))
        );
    }

    // the max spool prefix has no successor, so its fallback path must clear too
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
            (0, Some(StorageUnits(0)))
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

        assert_eq!(
            store.slice_totals().expect("totals"),
            (3, Some(StorageUnits(stored_len(100) + stored_len(250) + stored_len(50))))
        );

        store.delete_slice(SpoolIndex(2), dropped).expect("delete");

        assert_eq!(
            store.slice_totals().expect("totals"),
            (2, Some(StorageUnits(stored_len(100) + stored_len(250))))
        );
    }
}

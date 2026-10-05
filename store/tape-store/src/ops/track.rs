//! Canonical compressed-track catalog operations.

use tape_core::track::types::{CompressedTrack, PackedTrack};
use tape_store_api::{Column, Store};
use tape_crypto::address::Address;
use tape_core::types::TrackNumber;

use crate::columns::{TrackCol, TrackLookupCol};
use crate::error::{Result, TapeStoreError};
use crate::types::TrackLookupKey;
use crate::TapeStore;

/// Operations for the compressed-track catalog.
pub trait TrackOps {
    /// Get track by address.
    fn get_track(&self, track_address: Address) -> Result<Option<CompressedTrack>>;

    /// Get track metadata for several addresses, answered in the order asked.
    fn get_tracks(&self, track_addresses: &[Address]) -> Result<Vec<Option<CompressedTrack>>>;

    /// Store track metadata.
    fn put_track(&self, track_address: Address, track: CompressedTrack) -> Result<()>;

    /// Delete track metadata.
    fn delete_track(&self, track_address: Address) -> Result<()>;

    /// Check if track metadata exists without loading data.
    fn has_track(&self, track_address: Address) -> Result<bool>;

    /// Get the address of the track at one number on a tape.
    fn track_address_by_number(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> Result<Option<Address>>;

    /// Count all tracks without loading data.
    fn count_tracks(&self) -> Result<usize>;

    /// Paginated track iteration ordered by track address.
    fn iter_tracks_from(
        &self,
        after_track: Option<Address>,
        limit: usize,
    ) -> Result<Vec<(Address, CompressedTrack)>>;

    /// One page of tracks in no promised order, resumed by an opaque mark.
    ///
    /// What a maintenance pass wants: complete, resumable coverage without the
    /// key order it never reads. The mark is the backend's, so hand back whatever
    /// the last page answered and nothing to start.
    fn sweep_tracks(
        &self,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Address, CompressedTrack)>, Option<Vec<u8>>)>;

    /// Paginated track iteration ordered by (tape, track_number, key).
    fn iter_tracks_by_tape_from(
        &self,
        tape: Address,
        after_track_number: Option<TrackNumber>,
        limit: usize,
    ) -> Result<Vec<CompressedTrack>>;
}

impl<S: Store> TrackOps for TapeStore<S> {
    fn get_tracks(&self, track_addresses: &[Address]) -> Result<Vec<Option<CompressedTrack>>> {
        let packed = self.get_many::<TrackCol>(track_addresses)?;
        Ok(packed
            .into_iter()
            .map(|track| track.map(CompressedTrack::unpack))
            .collect())
    }

    fn get_track(&self, track_address: Address) -> Result<Option<CompressedTrack>> {
        Ok(self
            .get::<TrackCol>(&track_address)?
            .map(CompressedTrack::unpack))
    }

    fn put_track(&self, track_address: Address, track: CompressedTrack) -> Result<()> {
        self.put::<TrackCol>(&track_address, &track.pack())?;
        let lookup = TrackLookupKey::new(track.tape, track.track_number, track.key);
        self.put::<TrackLookupCol>(&lookup, &track_address)?;
        Ok(())
    }

    fn delete_track(&self, track_address: Address) -> Result<()> {
        if let Some(track) = self.get_track(track_address)? {
            let lookup = TrackLookupKey::new(track.tape, track.track_number, track.key);
            self.delete::<TrackLookupCol>(&lookup)?;
        }
        self.delete::<TrackCol>(&track_address)?;
        Ok(())
    }

    fn has_track(&self, track_address: Address) -> Result<bool> {
        Ok(self.contains::<TrackCol>(&track_address)?)
    }

    fn track_address_by_number(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> Result<Option<Address>> {
        // The range covers this one track number, so a miss reads nothing past it.
        let start = lookup_key_bytes(&TrackLookupKey::at_track_number(tape, track_number))?;
        let end = lookup_key_bytes(&TrackLookupKey::after_track_number(tape, track_number))?;
        let mut iter = self
            .inner()
            .inner()
            .iter_range(TrackLookupCol::CF_NAME, &start, &end)?;

        let Some((_, value_bytes)) = iter.next() else {
            return Ok(None);
        };
        let address = wincode::deserialize(&value_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("track lookup value: {}", e)))?;
        Ok(Some(address))
    }

    fn count_tracks(&self) -> Result<usize> {
        // The reel answers a whole-column count from its counters.
        Ok(self.inner().inner().count_prefix(TrackCol::CF_NAME, &[])? as usize)
    }

    fn sweep_tracks(
        &self,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Address, CompressedTrack)>, Option<Vec<u8>>)> {
        let (rows, next) = self.inner().inner().sweep(TrackCol::CF_NAME, from, limit)?;

        let mut tracks = Vec::with_capacity(rows.len());
        for (key_bytes, value_bytes) in rows {
            let key: Address = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?;
            let info: PackedTrack = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            tracks.push((key, CompressedTrack::unpack(info)));
        }
        Ok((tracks, next))
    }

    fn iter_tracks_from(
        &self,
        after_track: Option<Address>,
        limit: usize,
    ) -> Result<Vec<(Address, CompressedTrack)>> {
        let start_key = match after_track {
            Some(track) => wincode::serialize(&track)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?,
            None => Vec::new(),
        };

        let iter = self
            .inner()
            .inner()
            .iter_from(TrackCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: Address = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?;
            // Skip the cursor key if resuming
            if after_track.is_some() && Some(key) == after_track {
                continue;
            }
            let info: PackedTrack = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            results.push((key, CompressedTrack::unpack(info)));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    fn iter_tracks_by_tape_from(
        &self,
        tape: Address,
        after_track_number: Option<TrackNumber>,
        limit: usize,
    ) -> Result<Vec<CompressedTrack>> {
        let mut results = Vec::new();
        for track_address in tape_track_addresses(self, tape, after_track_number, limit)? {
            let track = self
                .get_track(track_address)?
                .ok_or_else(|| TapeStoreError::Serialization("missing track for lookup index".into()))?;
            results.push(track);
        }

        Ok(results)
    }
}

/// Addresses of a tape's tracks in track-number order, after an optional track number
pub fn tape_track_addresses<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<Address>> {
    let start = after_track_number
        .map(|track_number| TrackLookupKey::after_track_number(tape, track_number));
    let entries = lookup_entries(tape_store, tape, start, limit)?;
    Ok(entries.into_iter().map(|(_, address)| address).collect())
}

/// Numbers of a tape's tracks in order, after an optional track number
pub fn tape_track_numbers<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<TrackNumber>> {
    let start = after_track_number
        .map(|track_number| TrackLookupKey::after_track_number(tape, track_number));
    let entries = lookup_entries(tape_store, tape, start, limit)?;
    Ok(entries.into_iter().map(|(number, _)| number).collect())
}

/// A tape's lookup entries in track-number order, from an optional start key
fn lookup_entries<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    start: Option<TrackLookupKey>,
    limit: usize,
) -> Result<Vec<(TrackNumber, Address)>> {
    let prefix = TrackLookupKey::tape_prefix(tape);
    let start_key = match start {
        Some(start) => lookup_key_bytes(&start)?,
        None => prefix.to_vec(),
    };

    let iter = tape_store
        .inner()
        .inner()
        .iter_from(TrackLookupCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;

    let mut entries = Vec::new();
    for (key_bytes, value_bytes) in iter {
        if key_bytes.len() < 32 || key_bytes[..32] != prefix {
            break;
        }

        let key: TrackLookupKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))?;
        let address: Address = wincode::deserialize(&value_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("track lookup value: {}", e)))?;
        entries.push((key.track_number, address));
        if entries.len() >= limit {
            break;
        }
    }

    Ok(entries)
}

fn lookup_key_bytes(key: &TrackLookupKey) -> Result<Vec<u8>> {
    wincode::serialize(key)
        .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::track::types::TrackFlags;
    use tape_core::types::StorageUnits;
    use tape_core::spooler::TrackGroup;
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info() -> CompressedTrack {
        CompressedTrack {
            tape: Address::new_unique(),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: 0,
            state: 1,
            flags: TrackFlags::NONE,
            size: StorageUnits(1024 * 1024),
            group: TrackGroup(3),
            value_hash: Hash::new_unique(),
        }
    }

    #[test]
    fn test_track_roundtrip() {
        let store = test_store();
        let track = Address::new_unique();
        let info = make_track_info();

        assert!(store.get_track(track).unwrap().is_none());

        store.put_track(track, info).unwrap();

        let retrieved = store.get_track(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_delete() {
        let store = test_store();
        let track = Address::new_unique();
        let info = make_track_info();

        store.put_track(track, info).unwrap();
        assert!(store.get_track(track).unwrap().is_some());

        store.delete_track(track).unwrap();
        assert!(store.get_track(track).unwrap().is_none());
    }

    #[test]
    fn test_has_track() {
        let store = test_store();
        let track = Address::new_unique();

        assert!(!store.has_track(track).unwrap());

        store.put_track(track, make_track_info()).unwrap();
        assert!(store.has_track(track).unwrap());

        store.delete_track(track).unwrap();
        assert!(!store.has_track(track).unwrap());
    }

    #[test]
    fn count_tracks() {
        let store = test_store();
        assert_eq!(store.count_tracks().unwrap(), 0);

        for _ in 0..3 {
            store.put_track(Address::new_unique(), make_track_info()).unwrap();
        }
        assert_eq!(store.count_tracks().unwrap(), 3);
    }

    #[test]
    fn test_iter_tracks_from_all() {
        let store = test_store();

        for _ in 0..5 {
            store.put_track(Address::new_unique(), make_track_info()).unwrap();
        }

        let all = store.iter_tracks_from(None, 100).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_iter_tracks_from_pagination() {
        let store = test_store();

        for _ in 0..5 {
            store.put_track(Address::new_unique(), make_track_info()).unwrap();
        }

        // Get first 2
        let first = store.iter_tracks_from(None, 2).unwrap();
        assert_eq!(first.len(), 2);

        // Get remaining after cursor
        let cursor = first[1].0;
        let rest = store.iter_tracks_from(Some(cursor), 100).unwrap();
        assert_eq!(rest.len(), 3);

        // Verify no overlap
        assert!(rest.iter().all(|(k, _)| *k != cursor));
    }

    #[test]
    fn test_iter_tracks_from_empty() {
        let store = test_store();
        let result = store.iter_tracks_from(None, 100).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_iter_tracks_by_tape_from() {
        let store = test_store();
        let tape_a = Address::new_unique();
        let tape_b = Address::new_unique();

        let mut track0 = make_track_info();
        track0.tape = tape_a;
        track0.track_number = TrackNumber(0);
        let addr0 = Address::new_unique();
        store.put_track(addr0, track0).unwrap();

        let mut track1 = make_track_info();
        track1.tape = tape_a;
        track1.track_number = TrackNumber(1);
        let addr1 = Address::new_unique();
        store.put_track(addr1, track1).unwrap();

        let mut other = make_track_info();
        other.tape = tape_b;
        other.track_number = TrackNumber(0);
        let other_addr = Address::new_unique();
        store.put_track(other_addr, other).unwrap();

        let first = store.iter_tracks_by_tape_from(tape_a, None, 1).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].track_number, TrackNumber(0));

        let rest = store
            .iter_tracks_by_tape_from(tape_a, Some(TrackNumber(0)), 10)
            .unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].track_number, TrackNumber(1));
    }

    // a by-number read returns the address the track was stored under
    #[test]
    fn address_by_number() {
        let store = test_store();
        let tape = Address::new_unique();

        let mut track0 = make_track_info();
        track0.tape = tape;
        track0.track_number = TrackNumber(0);
        let addr0 = Address::new_unique();
        store.put_track(addr0, track0).unwrap();

        let mut track2 = make_track_info();
        track2.tape = tape;
        track2.track_number = TrackNumber(2);
        let addr2 = Address::new_unique();
        store.put_track(addr2, track2).unwrap();

        assert_eq!(store.track_address_by_number(tape, TrackNumber(0)).unwrap(), Some(addr0));
        assert_eq!(store.track_address_by_number(tape, TrackNumber(1)).unwrap(), None);
        assert_eq!(store.track_address_by_number(tape, TrackNumber(2)).unwrap(), Some(addr2));
        assert_eq!(store.track_address_by_number(tape, TrackNumber(3)).unwrap(), None);
        assert_eq!(
            store.track_address_by_number(Address::new_unique(), TrackNumber(0)).unwrap(),
            None
        );

        store.delete_track(addr0).unwrap();
        assert_eq!(store.track_address_by_number(tape, TrackNumber(0)).unwrap(), None);
    }
}

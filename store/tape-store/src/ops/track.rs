//! Canonical compressed-track catalog operations.

use tape_core::track::types::{CompressedTrack, PackedTrack};
use tape_store_api::{Column, Store};
use tape_crypto::address::Address;
use tape_core::types::{TapeNumber, TrackNumber};

use crate::columns::{TrackCol, TrackGroupIndexCol, TrackLookupCol};
use crate::error::{Result, TapeStoreError};
use crate::ops::spool::track_group_key;
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
        tape_number: TapeNumber,
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

    /// Unordered track iteration, returning the next cursor for resume.
    fn sweep_tracks(
        &self,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Address, CompressedTrack)>, Option<Vec<u8>>)>;

    /// Paginated iteration over one tape's tracks, ordered by track number.
    fn iter_tracks_by_tape_from(
        &self,
        tape: Address,
        tape_number: TapeNumber,
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
        let lookup = TrackLookupKey::new(track.tape, track.tape_number, track.track_number);
        self.put::<TrackLookupCol>(&lookup, &track_address)?;
        let by_group = track_group_key(track.group, track_address)?;
        self.inner().inner().put(TrackGroupIndexCol::CF_NAME, &by_group, &[])?;
        Ok(())
    }

    fn delete_track(&self, track_address: Address) -> Result<()> {
        if let Some(track) = self.get_track(track_address)? {
            let lookup = TrackLookupKey::new(track.tape, track.tape_number, track.track_number);
            self.delete::<TrackLookupCol>(&lookup)?;
            let by_group = track_group_key(track.group, track_address)?;
            self.inner().inner().delete(TrackGroupIndexCol::CF_NAME, &by_group)?;
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
        tape_number: TapeNumber,
        track_number: TrackNumber,
    ) -> Result<Option<Address>> {
        let lookup = TrackLookupKey::new(tape, tape_number, track_number);
        Ok(self.get::<TrackLookupCol>(&lookup)?)
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
        tape_number: TapeNumber,
        after_track_number: Option<TrackNumber>,
        limit: usize,
    ) -> Result<Vec<CompressedTrack>> {
        let mut results = Vec::new();
        let addresses = tape_track_addresses(self, tape, tape_number, after_track_number, limit)?;
        for track_address in addresses {
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
    tape_number: TapeNumber,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<Address>> {
    let entries = lookup_entries(tape_store, tape, tape_number, after_track_number, limit)?;
    Ok(entries.into_iter().map(|(_, address)| address).collect())
}

/// Numbers of a tape's tracks in order, after an optional track number
pub fn tape_track_numbers<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    tape_number: TapeNumber,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<TrackNumber>> {
    let entries = lookup_entries(tape_store, tape, tape_number, after_track_number, limit)?;
    Ok(entries.into_iter().map(|(number, _)| number).collect())
}

/// Numbers of the tapes with tracks under one address, in order. A closed
/// tape's tracks stay under its number until the sweep takes them, so an
/// address reserved again can hold more than one.
pub fn tape_numbers_at<S: Store>(tape_store: &TapeStore<S>, tape: Address) -> Result<Vec<TapeNumber>> {
    let mut numbers = Vec::new();
    let mut tape_number = TapeNumber(0);

    loop {
        // One seek for each tape: to its first track, or to the next tape's.
        let start_key = TrackLookupKey::tape_prefix(tape, tape_number);
        let mut iter = tape_store
            .inner()
            .inner()
            .iter_from(TrackLookupCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;
        let Some((key_bytes, _)) = iter.next() else {
            break;
        };

        let key: TrackLookupKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))?;
        if key.tape != tape {
            break;
        }
        numbers.push(key.tape_number);

        let Some(next) = key.tape_number.checked_add(TapeNumber(1)) else {
            break;
        };
        tape_number = next;
    }

    Ok(numbers)
}

/// A tape's lookup entries in track-number order, after an optional track number
fn lookup_entries<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    tape_number: TapeNumber,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<(TrackNumber, Address)>> {
    let prefix = TrackLookupKey::tape_prefix(tape, tape_number);
    let start_key = match after_track_number {
        Some(track_number) => {
            lookup_key_bytes(&TrackLookupKey::new(tape, tape_number, track_number))?
        }
        None => prefix.to_vec(),
    };

    let iter = tape_store
        .inner()
        .inner()
        .iter_from(TrackLookupCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;

    let mut entries = Vec::new();
    for (key_bytes, value_bytes) in iter {
        if !key_bytes.starts_with(&prefix) {
            break;
        }

        let key: TrackLookupKey = wincode::deserialize(&key_bytes)
            .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))?;
        // Skip the cursor key if resuming
        if Some(key.track_number) == after_track_number {
            continue;
        }
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
    use tape_core::types::{StorageUnits, TapeNumber};
    use tape_core::spooler::VirtualGroup;
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    const TAPE_NUMBER: TapeNumber = TapeNumber(1);
    const OTHER_TAPE_NUMBER: TapeNumber = TapeNumber(2);

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info() -> CompressedTrack {
        CompressedTrack {
            tape: Address::new_unique(),
            tape_number: TAPE_NUMBER,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: 0,
            state: 1,
            flags: TrackFlags::NONE,
            size: StorageUnits(1024 * 1024),
            group: VirtualGroup(3),
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
        other.tape_number = OTHER_TAPE_NUMBER;
        other.track_number = TrackNumber(0);
        let other_addr = Address::new_unique();
        store.put_track(other_addr, other).unwrap();

        let first = store.iter_tracks_by_tape_from(tape_a, TAPE_NUMBER, None, 1).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].track_number, TrackNumber(0));

        let rest = store
            .iter_tracks_by_tape_from(tape_a, TAPE_NUMBER, Some(TrackNumber(0)), 10)
            .unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].track_number, TrackNumber(1));
    }

    // a by-number read returns the address the track was stored under
    #[test]
    fn address_by_number() {
        let store = test_store();
        let tape = Address::new_unique();
        let by_number = |tape, tape_number, number| {
            store.track_address_by_number(tape, tape_number, TrackNumber(number)).unwrap()
        };

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

        assert_eq!(by_number(tape, TAPE_NUMBER, 0), Some(addr0));
        assert_eq!(by_number(tape, TAPE_NUMBER, 1), None);
        assert_eq!(by_number(tape, TAPE_NUMBER, 2), Some(addr2));
        assert_eq!(by_number(tape, TAPE_NUMBER, 3), None);
        assert_eq!(by_number(tape, OTHER_TAPE_NUMBER, 0), None);
        assert_eq!(by_number(Address::new_unique(), TAPE_NUMBER, 0), None);

        store.delete_track(addr0).unwrap();
        assert_eq!(by_number(tape, TAPE_NUMBER, 0), None);
    }

    // a tape reserved again at its old address has a new number, so its
    // track 0 and the old tape's track 0 keep separate rows
    #[test]
    fn reserved_again() {
        let store = test_store();
        let tape = Address::new_unique();

        let mut old = make_track_info();
        old.tape = tape;
        let old_address = Address::new_unique();
        store.put_track(old_address, old).unwrap();

        let mut new = make_track_info();
        new.tape = tape;
        new.tape_number = OTHER_TAPE_NUMBER;
        let new_address = Address::new_unique();
        store.put_track(new_address, new).unwrap();

        assert_eq!(
            store.track_address_by_number(tape, TAPE_NUMBER, TrackNumber(0)).unwrap(),
            Some(old_address)
        );
        assert_eq!(
            store.track_address_by_number(tape, OTHER_TAPE_NUMBER, TrackNumber(0)).unwrap(),
            Some(new_address)
        );

        store.delete_track(old_address).unwrap();

        assert_eq!(
            store.track_address_by_number(tape, TAPE_NUMBER, TrackNumber(0)).unwrap(),
            None
        );
        assert_eq!(
            store.iter_tracks_by_tape_from(tape, OTHER_TAPE_NUMBER, None, 10).unwrap(),
            vec![new]
        );
    }

    // the program gives every tape its own number, and two tapes that share
    // one all the same keep separate rows
    #[test]
    fn shared_number() {
        let store = test_store();
        let mine = make_track_info();
        let theirs = make_track_info();
        let my_address = Address::new_unique();
        let their_address = Address::new_unique();
        store.put_track(my_address, mine).unwrap();
        store.put_track(their_address, theirs).unwrap();

        assert_eq!(
            store.track_address_by_number(mine.tape, TAPE_NUMBER, TrackNumber(0)).unwrap(),
            Some(my_address)
        );
        assert_eq!(
            store.iter_tracks_by_tape_from(mine.tape, TAPE_NUMBER, None, 10).unwrap(),
            vec![mine]
        );

        store.delete_track(my_address).unwrap();

        assert_eq!(
            store.track_address_by_number(theirs.tape, TAPE_NUMBER, TrackNumber(0)).unwrap(),
            Some(their_address)
        );
    }

    // an address lists each tape that has tracks under it once, in order, and
    // nothing of the address next to it
    #[test]
    fn numbers_at_address() {
        let store = test_store();
        let tape = Address::from([7u8; 32]);
        let neighbour = Address::from([8u8; 32]);

        for (address, tape_number, track_number) in [
            (tape, OTHER_TAPE_NUMBER, 0),
            (tape, TAPE_NUMBER, 0),
            (tape, TAPE_NUMBER, 1),
            (neighbour, TapeNumber(u64::MAX), 0),
        ] {
            let mut track = make_track_info();
            track.tape = address;
            track.tape_number = tape_number;
            track.track_number = TrackNumber(track_number);
            store.put_track(Address::new_unique(), track).unwrap();
        }

        let mut expected = vec![TAPE_NUMBER, OTHER_TAPE_NUMBER];
        expected.sort();
        assert_eq!(tape_numbers_at(&store, tape).unwrap(), expected);
        assert_eq!(tape_numbers_at(&store, neighbour).unwrap(), vec![TapeNumber(u64::MAX)]);
        assert!(tape_numbers_at(&store, Address::from([9u8; 32])).unwrap().is_empty());
    }
}

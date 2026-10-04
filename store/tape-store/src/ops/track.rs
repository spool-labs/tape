//! Canonical compressed-track catalog operations.

use tape_core::track::types::{latest_track, CompressedTrack, PackedTrack};
use tape_store_api::{Column, Store};
use tape_crypto::address::Address;
use tape_core::types::{SpoolIndex, TrackNumber};

use crate::columns::{TrackCol, TrackLookupCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{TrackKey, TrackLookupKey};
use crate::TapeStore;

/// Operations for the compressed-track catalog.
///
/// A track is stored under its address and its track number. Tracks written
/// with the same content share an address and differ by number.
pub trait TrackOps {
    /// Get the track at one address and track number.
    fn get_track(
        &self,
        track_address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<CompressedTrack>>;

    /// Get several tracks by address and track number, answered in the order asked.
    fn get_tracks(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<CompressedTrack>>>;

    /// Every track stored under one address, in track-number order.
    fn tracks_by_address(&self, track_address: Address) -> Result<Vec<CompressedTrack>>;

    /// The certified track with the highest track number under one address.
    ///
    /// When no track under the address is certified, the one with the highest
    /// track number answers.
    fn latest_track(&self, track_address: Address) -> Result<Option<CompressedTrack>>;

    /// The track a slice in one spool belongs to.
    ///
    /// A spool holds one copy of a slice per address, so this is the latest
    /// track under the address whose group holds the spool.
    fn spool_track(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
    ) -> Result<Option<CompressedTrack>>;

    /// Store track metadata.
    fn put_track(&self, track_address: Address, track: CompressedTrack) -> Result<()>;

    /// Delete track metadata.
    fn delete_track(&self, track_address: Address, track_number: TrackNumber) -> Result<()>;

    /// Check if track metadata exists without loading data.
    fn has_track(&self, track_address: Address, track_number: TrackNumber) -> Result<bool>;

    /// Get the address of the track at one number on a tape.
    fn track_address_by_number(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> Result<Option<Address>>;

    /// Count all tracks without loading data.
    fn count_tracks(&self) -> Result<usize>;

    /// Paginated track iteration ordered by track address, then track number.
    fn iter_tracks_from(
        &self,
        after_track: Option<(Address, TrackNumber)>,
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
    fn get_tracks(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<CompressedTrack>>> {
        let keys: Vec<TrackKey> = tracks
            .iter()
            .map(|(track_address, track_number)| TrackKey::new(*track_address, *track_number))
            .collect();
        let packed = self.get_many::<TrackCol>(&keys)?;
        Ok(packed
            .into_iter()
            .map(|track| track.map(CompressedTrack::unpack))
            .collect())
    }

    fn get_track(
        &self,
        track_address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<CompressedTrack>> {
        Ok(self
            .get::<TrackCol>(&TrackKey::new(track_address, track_number))?
            .map(CompressedTrack::unpack))
    }

    fn tracks_by_address(&self, track_address: Address) -> Result<Vec<CompressedTrack>> {
        let prefix = TrackKey::track_prefix(track_address);
        let iter = self.inner().inner().iter_prefix(TrackCol::CF_NAME, &prefix)?;

        let mut tracks = Vec::new();
        for (_, value_bytes) in iter {
            let info: PackedTrack = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            tracks.push(CompressedTrack::unpack(info));
        }
        Ok(tracks)
    }

    fn latest_track(&self, track_address: Address) -> Result<Option<CompressedTrack>> {
        Ok(latest_track(&self.tracks_by_address(track_address)?))
    }

    fn spool_track(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
    ) -> Result<Option<CompressedTrack>> {
        let mut tracks = self.tracks_by_address(track_address)?;
        tracks.retain(|track| track.group.contains(spool_id));
        Ok(latest_track(&tracks))
    }

    fn put_track(&self, track_address: Address, track: CompressedTrack) -> Result<()> {
        self.put::<TrackCol>(&TrackKey::new(track_address, track.track_number), &track.pack())?;
        let lookup = TrackLookupKey::new(track.tape, track.track_number, track.key);
        self.put::<TrackLookupCol>(&lookup, &track_address)?;
        Ok(())
    }

    fn delete_track(&self, track_address: Address, track_number: TrackNumber) -> Result<()> {
        if let Some(track) = self.get_track(track_address, track_number)? {
            let lookup = TrackLookupKey::new(track.tape, track.track_number, track.key);
            self.delete::<TrackLookupCol>(&lookup)?;
        }
        self.delete::<TrackCol>(&TrackKey::new(track_address, track_number))?;
        Ok(())
    }

    fn has_track(&self, track_address: Address, track_number: TrackNumber) -> Result<bool> {
        Ok(self.contains::<TrackCol>(&TrackKey::new(track_address, track_number))?)
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
            let key: TrackKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?;
            let info: PackedTrack = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            tracks.push((key.track, CompressedTrack::unpack(info)));
        }
        Ok((tracks, next))
    }

    fn iter_tracks_from(
        &self,
        after_track: Option<(Address, TrackNumber)>,
        limit: usize,
    ) -> Result<Vec<(Address, CompressedTrack)>> {
        let after_key = after_track
            .map(|(track_address, track_number)| TrackKey::new(track_address, track_number));
        let start_key = match after_key {
            Some(key) => wincode::serialize(&key)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?,
            None => Vec::new(),
        };

        let iter = self
            .inner()
            .inner()
            .iter_from(TrackCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: TrackKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?;
            // Skip the cursor key if resuming
            if Some(key) == after_key {
                continue;
            }
            let info: PackedTrack = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            results.push((key.track, CompressedTrack::unpack(info)));
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
        for (track_address, track_number) in tape_tracks(self, tape, after_track_number, limit)? {
            let track = self
                .get_track(track_address, track_number)?
                .ok_or_else(|| TapeStoreError::Serialization("missing track for lookup index".into()))?;
            results.push(track);
        }

        Ok(results)
    }
}

/// Address and number of a tape's tracks in track-number order, after an optional track number
pub fn tape_tracks<S: Store>(
    tape_store: &TapeStore<S>,
    tape: Address,
    after_track_number: Option<TrackNumber>,
    limit: usize,
) -> Result<Vec<(Address, TrackNumber)>> {
    let start = after_track_number
        .map(|track_number| TrackLookupKey::after_track_number(tape, track_number));
    let entries = lookup_entries(tape_store, tape, start, limit)?;
    Ok(entries.into_iter().map(|(number, address)| (address, number)).collect())
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
    use tape_core::track::types::{TrackFlags, TrackState};
    use tape_core::types::StorageUnits;
    use tape_core::spooler::GroupIndex;
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
            group: GroupIndex(3),
            value_hash: Hash::new_unique(),
        }
    }

    #[test]
    fn test_track_roundtrip() {
        let store = test_store();
        let track = Address::new_unique();
        let info = make_track_info();

        assert!(store.get_track(track, info.track_number).unwrap().is_none());

        store.put_track(track, info).unwrap();

        let retrieved = store.get_track(track, info.track_number).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_delete() {
        let store = test_store();
        let track = Address::new_unique();
        let info = make_track_info();

        store.put_track(track, info).unwrap();
        assert!(store.get_track(track, info.track_number).unwrap().is_some());

        store.delete_track(track, info.track_number).unwrap();
        assert!(store.get_track(track, info.track_number).unwrap().is_none());
    }

    #[test]
    fn test_has_track() {
        let store = test_store();
        let track = Address::new_unique();
        let info = make_track_info();

        assert!(!store.has_track(track, info.track_number).unwrap());

        store.put_track(track, info).unwrap();
        assert!(store.has_track(track, info.track_number).unwrap());

        store.delete_track(track, info.track_number).unwrap();
        assert!(!store.has_track(track, info.track_number).unwrap());
    }

    fn numbered(tape: Address, number: u64, state: TrackState) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(number),
            state: state as u64,
            ..make_track_info()
        }
    }

    // tracks written with the same content share an address and stay separate rows
    #[test]
    fn shared_address() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let first = numbered(tape, 2, TrackState::Certified);
        let second = numbered(tape, 5, TrackState::Certified);

        store.put_track(track, first).unwrap();
        store.put_track(track, second).unwrap();
        store.put_track(Address::new_unique(), numbered(tape, 3, TrackState::Certified)).unwrap();

        assert_eq!(store.get_track(track, TrackNumber(2)).unwrap(), Some(first));
        assert_eq!(store.get_track(track, TrackNumber(5)).unwrap(), Some(second));
        assert_eq!(store.get_track(track, TrackNumber(3)).unwrap(), None);
        assert_eq!(store.tracks_by_address(track).unwrap(), vec![first, second]);
        assert_eq!(store.count_tracks().unwrap(), 3);

        store.delete_track(track, TrackNumber(2)).unwrap();

        assert_eq!(store.tracks_by_address(track).unwrap(), vec![second]);
        assert_eq!(store.track_address_by_number(tape, TrackNumber(2)).unwrap(), None);
        assert_eq!(store.track_address_by_number(tape, TrackNumber(5)).unwrap(), Some(track));
    }

    // a read by address answers with the highest-numbered certified track
    #[test]
    fn latest_certified() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();

        assert_eq!(store.latest_track(track).unwrap(), None);

        let registered = numbered(tape, 1, TrackState::Registered);
        store.put_track(track, registered).unwrap();
        assert_eq!(store.latest_track(track).unwrap(), Some(registered));

        let certified = numbered(tape, 4, TrackState::Certified);
        let newer = numbered(tape, 300, TrackState::Certified);
        let abandoned = numbered(tape, 700, TrackState::Registered);
        for info in [certified, newer, abandoned] {
            store.put_track(track, info).unwrap();
        }

        assert_eq!(store.latest_track(track).unwrap(), Some(newer));

        store.delete_track(track, TrackNumber(300)).unwrap();
        assert_eq!(store.latest_track(track).unwrap(), Some(certified));
    }

    // a slice belongs to the track under its address whose group holds the spool
    #[test]
    fn slice_owner() {
        let store = test_store();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let near = CompressedTrack { group: GroupIndex(0), ..numbered(tape, 1, TrackState::Certified) };
        let far = CompressedTrack { group: GroupIndex(4), ..numbered(tape, 6, TrackState::Certified) };
        store.put_track(track, near).unwrap();
        store.put_track(track, far).unwrap();

        assert_eq!(store.spool_track(GroupIndex(0).spool_at(2), track).unwrap(), Some(near));
        assert_eq!(store.spool_track(GroupIndex(4).spool_at(0), track).unwrap(), Some(far));
        assert_eq!(store.spool_track(GroupIndex(2).spool_at(0), track).unwrap(), None);
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
        let rest = store
            .iter_tracks_from(Some((cursor, first[1].1.track_number)), 100)
            .unwrap();
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

        store.delete_track(addr0, TrackNumber(0)).unwrap();
        assert_eq!(store.track_address_by_number(tape, TrackNumber(0)).unwrap(), None);
    }
}

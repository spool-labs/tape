//! Canonical compressed-track catalog operations.

use tape_api::program::tapedrive::track_pda;
use tape_core::track::types::{CompressedTrack, PackedTrack};
use crate::columns::{TrackCol, TrackLookupCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, TrackLookupKey, TrackNumber, UnitKey};
use crate::TapeStore;
use store::{Column, Store};

/// Operations for the compressed-track catalog.
pub trait TrackOps {
    /// Get track by address.
    fn get_track(&self, track_address: Pubkey) -> Result<Option<CompressedTrack>>;

    /// Store track metadata.
    fn put_track(&self, track_address: Pubkey, track: CompressedTrack) -> Result<()>;

    /// Delete track metadata.
    fn delete_track(&self, track_address: Pubkey) -> Result<()>;

    /// Check if track metadata exists without loading data.
    fn has_track(&self, track_address: Pubkey) -> Result<bool>;

    /// Count all tracks without loading data.
    fn count_tracks(&self) -> Result<usize>;

    /// Paginated track iteration ordered by track address.
    fn iter_tracks_from(
        &self,
        after_track: Option<Pubkey>,
        limit: usize,
    ) -> Result<Vec<(Pubkey, CompressedTrack)>>;

    /// Paginated track iteration ordered by (tape, track_number, key).
    fn iter_tracks_by_tape_from(
        &self,
        tape: Pubkey,
        after_track_number: Option<TrackNumber>,
        limit: usize,
    ) -> Result<Vec<CompressedTrack>>;
}

impl<S: Store> TrackOps for TapeStore<S> {
    fn get_track(&self, track_address: Pubkey) -> Result<Option<CompressedTrack>> {
        Ok(self
            .get::<TrackCol>(&track_address)?
            .map(CompressedTrack::unpack))
    }

    fn put_track(&self, track_address: Pubkey, track: CompressedTrack) -> Result<()> {
        self.put::<TrackCol>(&track_address, &track.pack())?;
        let lookup = TrackLookupKey::new(track.tape.into(), track.track_number, track.key);
        self.put::<TrackLookupCol>(&lookup, &UnitKey)?;
        Ok(())
    }

    fn delete_track(&self, track_address: Pubkey) -> Result<()> {
        if let Some(track) = self.get_track(track_address)? {
            let lookup = TrackLookupKey::new(track.tape.into(), track.track_number, track.key);
            self.delete::<TrackLookupCol>(&lookup)?;
        }
        self.delete::<TrackCol>(&track_address)?;
        Ok(())
    }

    fn has_track(&self, track_address: Pubkey) -> Result<bool> {
        Ok(self.contains::<TrackCol>(&track_address)?)
    }

    fn count_tracks(&self) -> Result<usize> {
        let iter = self
            .inner()
            .inner()
            .iter_from(TrackCol::CF_NAME, &[], store::Direction::Asc)?;
        Ok(iter.count())
    }

    fn iter_tracks_from(
        &self,
        after_track: Option<Pubkey>,
        limit: usize,
    ) -> Result<Vec<(Pubkey, CompressedTrack)>> {
        let start_key = match after_track {
            Some(track) => wincode::serialize(&track)
                .map_err(|e| TapeStoreError::Serialization(format!("track key: {}", e)))?,
            None => Vec::new(),
        };

        let iter = self
            .inner()
            .inner()
            .iter_from(TrackCol::CF_NAME, &start_key, store::Direction::Asc)?;

        let mut results = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: Pubkey = wincode::deserialize(&key_bytes)
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
        tape: Pubkey,
        after_track_number: Option<TrackNumber>,
        limit: usize,
    ) -> Result<Vec<CompressedTrack>> {
        let prefix = TrackLookupKey::tape_prefix(tape);
        let start_key = match after_track_number {
            Some(track_number) => wincode::serialize(&TrackLookupKey::after_track_number(tape, track_number))
                .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))?,
            None => prefix.to_vec(),
        };

        let iter = self
            .inner()
            .inner()
            .iter_from(TrackLookupCol::CF_NAME, &start_key, store::Direction::Asc)?;

        let mut results = Vec::new();
        for (key_bytes, _value_bytes) in iter {
            if key_bytes.len() < 32 || key_bytes[..32] != prefix {
                break;
            }

            let key: TrackLookupKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track lookup key: {}", e)))?;
            let track_address = key::track_address(tape, key.track_number).into();
            let track = self
                .get_track(track_address)?
                .ok_or_else(|| TapeStoreError::Serialization("missing track for lookup index".into()))?;
            results.push(track);
            if results.len() >= limit {
                break;
            }
        }

        Ok(results)
    }
}

mod key {
    use tape_api::program::tapedrive::track_pda;
    use tape_core::types::TrackNumber;

    use crate::types::Pubkey;

    pub fn track_address(tape: Pubkey, track_number: TrackNumber) -> Pubkey {
        track_pda(tape.into(), track_number).0.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SpoolGroup;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info() -> CompressedTrack {
        CompressedTrack {
            tape: Pubkey::new_unique().into(),
            key: tape_crypto::Hash::new_unique(),
            track_number: tape_core::types::TrackNumber(0),
            kind: 0,
            state: 1,
            size: tape_core::types::StorageUnits(1024 * 1024),
            spool_group: SpoolGroup(3),
            value_hash: tape_crypto::Hash::new_unique(),
        }
    }

    #[test]
    fn test_track_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let info = make_track_info();

        assert!(store.get_track(track).unwrap().is_none());

        store.put_track(track, info.clone()).unwrap();

        let retrieved = store.get_track(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_delete() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let info = make_track_info();

        store.put_track(track, info).unwrap();
        assert!(store.get_track(track).unwrap().is_some());

        store.delete_track(track).unwrap();
        assert!(store.get_track(track).unwrap().is_none());
    }

    #[test]
    fn test_has_track() {
        let store = test_store();
        let track = Pubkey::new_unique();

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
            store.put_track(Pubkey::new_unique(), make_track_info()).unwrap();
        }
        assert_eq!(store.count_tracks().unwrap(), 3);
    }

    #[test]
    fn test_iter_tracks_from_all() {
        let store = test_store();

        for _ in 0..5 {
            store.put_track(Pubkey::new_unique(), make_track_info()).unwrap();
        }

        let all = store.iter_tracks_from(None, 100).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn test_iter_tracks_from_pagination() {
        let store = test_store();

        for _ in 0..5 {
            store.put_track(Pubkey::new_unique(), make_track_info()).unwrap();
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
        let tape_a = Pubkey::new_unique();
        let tape_b = Pubkey::new_unique();

        let mut track0 = make_track_info();
        track0.tape = tape_a.into();
        track0.track_number = TrackNumber(0);
        let addr0 = track_pda(track0.tape, track0.track_number).0.into();
        store.put_track(addr0, track0).unwrap();

        let mut track1 = make_track_info();
        track1.tape = tape_a.into();
        track1.track_number = TrackNumber(1);
        let addr1 = track_pda(track1.tape, track1.track_number).0.into();
        store.put_track(addr1, track1).unwrap();

        let mut other = make_track_info();
        other.tape = tape_b.into();
        other.track_number = TrackNumber(0);
        let other_addr = track_pda(other.tape, other.track_number).0.into();
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
}

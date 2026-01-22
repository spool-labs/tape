//! TrackInfo operations for track metadata
//!
//! Provides storage for track (blob) information.

use crate::columns::TrackInfoCol;
use crate::error::Result;
use crate::types::{EpochNumber, Pubkey, TrackInfo};
use crate::TapeStore;
use store::Store;

/// Operations for track info
pub trait TrackInfoOps {
    /// Get track info by address
    fn get_track_info(&self, track_address: Pubkey) -> Result<Option<TrackInfo>>;

    /// Store track info
    fn put_track_info(&self, track_address: Pubkey, info: TrackInfo) -> Result<()>;

    /// Delete track info
    fn delete_track_info(&self, track_address: Pubkey) -> Result<()>;

    /// Mark a track as certified
    fn certify_track(&self, track_address: Pubkey, epoch: EpochNumber) -> Result<()>;

    /// Iterate over tracks belonging to a tape
    fn iter_tracks_for_tape(
        &self,
        tape_address: Pubkey,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>>;
}

impl<S: Store> TrackInfoOps for TapeStore<S> {
    fn get_track_info(&self, track_address: Pubkey) -> Result<Option<TrackInfo>> {
        Ok(self.get::<TrackInfoCol>(&track_address)?)
    }

    fn put_track_info(&self, track_address: Pubkey, info: TrackInfo) -> Result<()> {
        self.put::<TrackInfoCol>(&track_address, &info)?;
        Ok(())
    }

    fn delete_track_info(&self, track_address: Pubkey) -> Result<()> {
        self.delete::<TrackInfoCol>(&track_address)?;
        Ok(())
    }

    fn certify_track(&self, track_address: Pubkey, epoch: EpochNumber) -> Result<()> {
        if let Some(mut info) = self.get::<TrackInfoCol>(&track_address)? {
            info.certified_epoch = Some(epoch);
            self.put::<TrackInfoCol>(&track_address, &info)?;
        }
        Ok(())
    }

    fn iter_tracks_for_tape(
        &self,
        tape_address: Pubkey,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>> {
        let iter = self.iter::<TrackInfoCol>()?;
        Ok(iter.into_iter().filter_map(move |(addr, info)| {
            if info.tape_address == tape_address {
                Some(Ok(addr))
            } else {
                None
            }
        }))
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
    fn test_track_info_roundtrip() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(100), [0xAB; 64]);

        assert!(store.get_track_info(track).unwrap().is_none());

        store.put_track_info(track, info.clone()).unwrap();

        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_track_info_delete() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(50), [0xCD; 64]);

        store.put_track_info(track, info).unwrap();
        assert!(store.get_track_info(track).unwrap().is_some());

        store.delete_track_info(track).unwrap();
        assert!(store.get_track_info(track).unwrap().is_none());
    }

    #[test]
    fn test_certify_track() {
        let store = test_store();
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();

        let info = TrackInfo::new(tape, EpochNumber(100), [0; 64]);

        store.put_track_info(track, info).unwrap();

        // Initially not certified
        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert!(retrieved.certified_epoch.is_none());

        // Certify
        store.certify_track(track, EpochNumber(101)).unwrap();

        // Now certified
        let retrieved = store.get_track_info(track).unwrap().unwrap();
        assert_eq!(retrieved.certified_epoch, Some(EpochNumber(101)));
    }

    #[test]
    fn test_iter_tracks_for_tape() {
        let store = test_store();

        let tape1 = Pubkey::new_unique();
        let tape2 = Pubkey::new_unique();

        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let track3 = Pubkey::new_unique();

        // Two tracks for tape1
        store
            .put_track_info(track1, TrackInfo::new(tape1, EpochNumber(0), [0; 64]))
            .unwrap();
        store
            .put_track_info(track2, TrackInfo::new(tape1, EpochNumber(0), [0; 64]))
            .unwrap();

        // One track for tape2
        store
            .put_track_info(track3, TrackInfo::new(tape2, EpochNumber(0), [0; 64]))
            .unwrap();

        // Get tracks for tape1
        let tracks: Vec<Pubkey> = store
            .iter_tracks_for_tape(tape1)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(tracks.len(), 2);
        assert!(tracks.contains(&track1));
        assert!(tracks.contains(&track2));
        assert!(!tracks.contains(&track3));
    }
}

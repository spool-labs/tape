//! TrackInfo operations for track metadata

use crate::columns::TrackCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{Pubkey, TrackInfo};
use crate::TapeStore;
use store::{Column, Store};

/// Operations for track info
pub trait TrackOps {
    /// Get track info by address
    fn get_track(&self, track_address: Pubkey) -> Result<Option<TrackInfo>>;

    /// Store track info
    fn put_track(&self, track_address: Pubkey, info: TrackInfo) -> Result<()>;

    /// Delete track info
    fn delete_track(&self, track_address: Pubkey) -> Result<()>;

    /// Check if track metadata exists without loading data
    fn has_track(&self, track_address: Pubkey) -> Result<bool>;

    /// Count all tracks without loading data.
    fn count_tracks(&self) -> Result<usize>;

    /// Paginated track iteration. Returns up to `limit` tracks starting after
    /// `after_track` (or from the beginning if None). Ordered by Pubkey.
    fn iter_tracks_from(
        &self,
        after_track: Option<Pubkey>,
        limit: usize,
    ) -> Result<Vec<(Pubkey, TrackInfo)>>;
}

impl<S: Store> TrackOps for TapeStore<S> {
    fn get_track(&self, track_address: Pubkey) -> Result<Option<TrackInfo>> {
        Ok(self.get::<TrackCol>(&track_address)?)
    }

    fn put_track(&self, track_address: Pubkey, info: TrackInfo) -> Result<()> {
        self.put::<TrackCol>(&track_address, &info)?;
        Ok(())
    }

    fn delete_track(&self, track_address: Pubkey) -> Result<()> {
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
    ) -> Result<Vec<(Pubkey, TrackInfo)>> {
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
            let info: TrackInfo = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("track info: {}", e)))?;
            results.push((key, info));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_track_info() -> TrackInfo {
        TrackInfo {
            tape_address: Pubkey::new_unique(),
            spool_group: 3,
            original_size: 1024 * 1024,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 2, // Clay
            encoding_params: 0,
            commitment: vec![],
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
}

//! The sample set a challenge round draws from

use store::{Column, Store};
use tape_core::spooler::GroupIndex;
use tape_core::types::SlotNumber;
use tape_crypto::address::Address;

use crate::columns::TrackSampleCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{TrackSample, TrackSampleKey};
use crate::TapeStore;

/// Operations over the per-group sample set
///
/// Every write happens on replay, so two owners of a group hold the same rows
/// whatever either of them managed to store.
pub trait SampleOps {
    /// Record a track's standing in its group's set.
    fn put_track_sample(
        &self,
        group: GroupIndex,
        track: Address,
        sample: TrackSample,
    ) -> Result<()>;

    /// One track's row, when it belongs to the group at all.
    fn track_sample(&self, group: GroupIndex, track: Address) -> Result<Option<TrackSample>>;

    /// Note a deletion, keeping the row while a round can still reference it.
    fn mark_track_sample_deleted(
        &self,
        group: GroupIndex,
        track: Address,
        slot: SlotNumber,
    ) -> Result<()>;

    /// One group's set, in the track order the draw is defined over.
    fn iter_track_samples_by_group(&self, group: GroupIndex)
        -> Result<Vec<(Address, TrackSample)>>;

    /// Drop rows whose deletion no round can reference any more.
    fn prune_track_samples_before(&self, slot: SlotNumber) -> Result<usize>;
}

impl<S: Store> SampleOps for TapeStore<S> {
    fn put_track_sample(
        &self,
        group: GroupIndex,
        track: Address,
        sample: TrackSample,
    ) -> Result<()> {
        self.put::<TrackSampleCol>(&TrackSampleKey::new(group, track), &sample)?;
        Ok(())
    }

    fn track_sample(&self, group: GroupIndex, track: Address) -> Result<Option<TrackSample>> {
        Ok(self.get::<TrackSampleCol>(&TrackSampleKey::new(group, track))?)
    }

    fn mark_track_sample_deleted(
        &self,
        group: GroupIndex,
        track: Address,
        slot: SlotNumber,
    ) -> Result<()> {
        // First deletion wins. A row can only be deleted once, and re-stamping
        // it would move the cut a round is judged against.
        let Some(sample) = self.track_sample(group, track)? else {
            return Ok(());
        };
        if sample.deleted_slot.is_some() {
            return Ok(());
        }

        self.put_track_sample(
            group,
            track,
            TrackSample {
                deleted_slot: Some(slot),
                ..sample
            },
        )
    }

    fn iter_track_samples_by_group(
        &self,
        group: GroupIndex,
    ) -> Result<Vec<(Address, TrackSample)>> {
        let prefix = TrackSampleKey::group_prefix(group);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(TrackSampleCol::CF_NAME, &prefix)?;

        // Group first then track, both big-endian, so the scan already arrives
        // in the canonical order and needs no sort of its own.
        let mut entries = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: TrackSampleKey = wincode::deserialize(&key_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("sample key: {e}")))?;
            let sample: TrackSample = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("sample row: {e}")))?;
            entries.push((key.track, sample));
        }
        Ok(entries)
    }

    fn prune_track_samples_before(&self, slot: SlotNumber) -> Result<usize> {
        let raw = self.inner().inner();
        let mut batch = store::WriteBatch::new();
        let mut dropped = 0usize;

        for (key_bytes, value_bytes) in raw.iter_prefix(TrackSampleCol::CF_NAME, &[])? {
            let sample: TrackSample = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("sample row: {e}")))?;
            if sample.deleted_slot.is_some_and(|deleted| deleted < slot) {
                batch.delete_owned(TrackSampleCol::CF_NAME, key_bytes);
                dropped += 1;
            }
        }

        if dropped > 0 {
            raw.write_batch(batch)?;
        }
        Ok(dropped)
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::StorageUnits;

    use super::*;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn sample(registered: u64) -> TrackSample {
        TrackSample {
            slice_len: StorageUnits::from_bytes(1_024),
            registered_slot: SlotNumber(registered),
            deleted_slot: None,
        }
    }

    // a row round-trips under its group
    #[test]
    fn round_trip() {
        let store = test_store();
        let track = Address::new_unique();

        assert_eq!(store.track_sample(GroupIndex(1), track).unwrap(), None);
        store.put_track_sample(GroupIndex(1), track, sample(10)).unwrap();
        assert_eq!(store.track_sample(GroupIndex(1), track).unwrap(), Some(sample(10)));

        // The same track under another group is a different row.
        assert_eq!(store.track_sample(GroupIndex(2), track).unwrap(), None);
    }

    // a group's set holds only its own tracks, in track order
    #[test]
    fn by_group() {
        let store = test_store();
        let mut mine: Vec<Address> = (0..4).map(|_| Address::new_unique()).collect();
        for track in &mine {
            store.put_track_sample(GroupIndex(3), *track, sample(1)).unwrap();
        }
        store
            .put_track_sample(GroupIndex(4), Address::new_unique(), sample(1))
            .unwrap();

        let listed: Vec<Address> = store
            .iter_track_samples_by_group(GroupIndex(3))
            .unwrap()
            .into_iter()
            .map(|(track, _)| track)
            .collect();

        mine.sort_unstable();
        assert_eq!(listed, mine);
    }

    // the cut is what decides membership, so a track registered after the
    // window opened is out and one deleted after it is still in
    #[test]
    fn cut_at_the_window() {
        let live = sample(10);
        assert!(live.in_set_at(SlotNumber(11)));
        assert!(!live.in_set_at(SlotNumber(10)), "registered at the cut is not before it");

        let deleted = TrackSample {
            deleted_slot: Some(SlotNumber(20)),
            ..live
        };
        assert!(deleted.in_set_at(SlotNumber(20)), "deleted at the cut still belongs");
        assert!(deleted.in_set_at(SlotNumber(15)));
        assert!(!deleted.in_set_at(SlotNumber(21)));
    }

    // the first deletion is the one a round is judged against
    #[test]
    fn first_deletion() {
        let store = test_store();
        let track = Address::new_unique();
        store.put_track_sample(GroupIndex(1), track, sample(5)).unwrap();

        store.mark_track_sample_deleted(GroupIndex(1), track, SlotNumber(30)).unwrap();
        store.mark_track_sample_deleted(GroupIndex(1), track, SlotNumber(40)).unwrap();

        let row = store.track_sample(GroupIndex(1), track).unwrap().expect("row");
        assert_eq!(row.deleted_slot, Some(SlotNumber(30)));
    }

    // marking a track no group holds is not an error
    #[test]
    fn delete_unknown() {
        let store = test_store();
        let track = Address::new_unique();

        store.mark_track_sample_deleted(GroupIndex(1), track, SlotNumber(30)).unwrap();
        assert_eq!(store.track_sample(GroupIndex(1), track).unwrap(), None);
    }

    // pruning drops deletions no round can reach and keeps live rows
    #[test]
    fn prune_deleted() {
        let store = test_store();
        let live = Address::new_unique();
        let old = Address::new_unique();
        let recent = Address::new_unique();

        for track in [live, old, recent] {
            store.put_track_sample(GroupIndex(1), track, sample(1)).unwrap();
        }
        store.mark_track_sample_deleted(GroupIndex(1), old, SlotNumber(10)).unwrap();
        store.mark_track_sample_deleted(GroupIndex(1), recent, SlotNumber(90)).unwrap();

        assert_eq!(store.prune_track_samples_before(SlotNumber(50)).unwrap(), 1);
        assert!(store.track_sample(GroupIndex(1), old).unwrap().is_none());
        assert!(store.track_sample(GroupIndex(1), live).unwrap().is_some());
        assert!(store.track_sample(GroupIndex(1), recent).unwrap().is_some());
    }
}

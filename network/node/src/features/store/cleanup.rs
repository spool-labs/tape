use tape_store_api::Store;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::VirtualGroup;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store::ops::{
    ObjectInfoOps, ObjectListOps, ObjectMetadataOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::types::EntryKey;
use tape_store::TapeStore;

use crate::core::error::NodeError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupStats {
    pub tapes_cleared: usize,
    pub tracks_deleted: usize,
}

/// Delete a track's rows. Its slices stay for the gc's orphan slice sweep.
pub fn delete_track_local<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    profile: ErasureCode,
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();

    if let Some(info) = store.get_track(track).map_err(store_error)? {
        remove_object_listing_for_track(store, track, &info)?;
        remove_track_entries(store, track, info.group, profile)?;
        stats.tracks_deleted += 1;
    }

    store.delete_track(track).map_err(store_error)?;
    store.delete_track_data(track).map_err(store_error)?;
    store.delete_object_info(track).map_err(store_error)?;
    store.delete_object_metadata(track).map_err(store_error)?;

    Ok(stats)
}

/// Delete the tracks of the tape at an address, if it has expired as of
/// `current_epoch`. The tape's row stays: it goes when the tape is closed, and
/// a renew needs it until then.
///
/// The store keeps applying blocks while this runs. The row is read here and
/// the walk stops at the track counter it shows, so a tape renewed since the
/// caller looked, or one reserved again at the address, keeps its tracks: an
/// expired tape takes no writes, and a renewed tape's tracks are numbered from
/// that counter on.
pub fn delete_tape_tracks<Db: Store>(
    store: &TapeStore<Db>,
    tape: Address,
    current_epoch: EpochNumber,
    track_batch: usize,
    profile: ErasureCode,
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();
    let Some(expired) = store.get_tape(tape).map_err(store_error)? else {
        return Ok(stats);
    };
    if expired.end_epoch > current_epoch {
        return Ok(stats);
    }
    let mut cursor = None;

    'walk: loop {
        let tracks = store
            .iter_tracks_by_tape_from(tape, expired.id, cursor, track_batch)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        cursor = tracks.last().map(|info| info.track_number);

        for info in &tracks {
            if info.track_number >= expired.next_track_number {
                break 'walk;
            }

            let track = info.address();
            remove_object_listing_for_track(store, track, info)?;
            remove_track_entries(store, track, info.group, profile)?;
            store.delete_track(track).map_err(store_error)?;
            store.delete_track_data(track).map_err(store_error)?;
            store.delete_object_info(track).map_err(store_error)?;
            store.delete_object_metadata(track).map_err(store_error)?;
            stats.tracks_deleted += 1;
        }
    }

    if stats.tracks_deleted > 0 {
        stats.tapes_cleared = 1;
    }
    Ok(stats)
}

/// Remove a track's missing_list entries on every spool of its group
pub fn remove_track_entries<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: VirtualGroup,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    // A delete of a missing row still leaves a grave in the store's index, and
    // the gc sweep clears a dead track's rows again every epoch, so look first.
    for position in 0..usize::from(profile.n()) {
        let spool = group.spool_at(position);
        if !store.has_entry(spool, track).map_err(store_error)? {
            continue;
        }
        for is_handover in [false, true] {
            let key = EntryKey { spool, is_handover, track };
            store.delete_entry(key).map_err(store_error)?;
        }
    }

    Ok(())
}

/// Delete a stored spool's slices and its missing_list, a page at a time,
/// then its row
pub fn purge_spool_local<Db: Store>(
    store: &TapeStore<Db>,
    spool_id: SpoolIndex,
    page: usize,
) -> Result<(), NodeError> {
    store
        .delete_all_slices_for_spool(spool_id)
        .map_err(store_error)?;

    while store.delete_spool_entries(spool_id, page).map_err(store_error)? > 0 {}

    store.delete_spool_state(spool_id).map_err(store_error)
}

pub fn remove_object_listing_for_track<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    info: &CompressedTrack,
) -> Result<(), NodeError> {
    let Some(metadata) = store.get_object_metadata(track).map_err(store_error)? else {
        return Ok(());
    };

    let entry = store
        .get_object_entry(info.tape, &metadata.name)
        .map_err(store_error)?;

    let Some(entry) = entry else {
        return Ok(());
    };

    // The name may already list a newer write, which on a tape reserved again
    // at this address can sit at the same track number.
    if entry.track == track {
        store
            .delete_object_entry(info.tape, &metadata.name)
            .map_err(store_error)?;
    }

    Ok(())
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tape_store_api::{BatchOp, Direction, StoreIter, Value, WriteBatch};
    use tape_store_memory::MemoryStore;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::tape::TapeFlags;
    use tape_core::track::types::{TrackFlags, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};
    use tape_crypto::Hash;
    use tape_store::types::{Entry, TapeInfo};

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    const TARGET: TapeNumber = TapeNumber(1);
    const OTHER: TapeNumber = TapeNumber(2);
    /// The seeded tapes end here, and the sweep under test runs in this epoch
    const EXPIRY: EpochNumber = EpochNumber(10);
    /// The seeded tapes' track counter, past every track the tests write
    const WRITTEN: TrackNumber = TrackNumber(8);

    fn seed_tape(store: &TapeStore<MemoryStore>, tape_number: TapeNumber) -> Address {
        let tape = Address::new_unique();
        let info = TapeInfo::new(tape_number, TapeFlags::NONE, EXPIRY, WRITTEN);
        store.put_tape(tape, info).unwrap();
        tape
    }

    // delete_tape_tracks derives each address from the stored leaf, so every
    // track is seeded under its own address.
    fn seed_track(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        tape_number: TapeNumber,
        track_number: TrackNumber,
        group: VirtualGroup,
    ) -> Address {
        let info = CompressedTrack {
            tape,
            tape_number,
            key: Hash::new_unique(),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group,
            value_hash: Hash::new_unique(),
        };
        let track = info.address();

        store.put_track(track, info).unwrap();

        for position in 0..usize::from(TAPENET_PROFILE.n()) {
            store
                .put_slice(group.spool_at(position), track, vec![7u8; 8])
                .unwrap();
        }

        track
    }

    // a tape's tracks go and its row stays, and another tape is left alone
    #[test]
    fn clears_only_its_own_tape() {
        let store = test_store();
        let target = seed_tape(&store, TARGET);
        let other = seed_tape(&store, OTHER);
        let group = VirtualGroup::from(3);

        let mine: Vec<Address> = (0..5)
            .map(|n| seed_track(&store, target, TARGET, TrackNumber(n), group))
            .collect();
        let theirs: Vec<Address> = (0..3)
            .map(|n| seed_track(&store, other, OTHER, TrackNumber(n), group))
            .collect();

        // Batch under the track count, so the tape cursor has to advance.
        let stats =
            delete_tape_tracks(&store, target, EXPIRY, 2, TAPENET_PROFILE).unwrap();

        assert_eq!(stats.tapes_cleared, 1);
        assert_eq!(stats.tracks_deleted, mine.len());

        for track in mine {
            assert!(!store.has_track(track).unwrap());
            assert!(store.has_slice(group.spool_at(0), track).unwrap());
        }

        for track in theirs {
            assert!(store.has_track(track).unwrap());
            assert!(store.has_slice(group.spool_at(0), track).unwrap());
        }

        assert!(store.get_tape(target).unwrap().is_some());
        assert!(store.get_tape(other).unwrap().is_some());
        assert!(store
            .iter_tracks_by_tape_from(target, TARGET, None, 16)
            .unwrap()
            .is_empty());
        assert_eq!(
            store.iter_tracks_by_tape_from(other, OTHER, None, 16).unwrap().len(),
            3
        );
    }

    // the program gives every tape its own number, and a delete all the same
    // leaves alone a tape that shares the deleted tape's number
    #[test]
    fn shared_number() {
        let store = test_store();
        let target = seed_tape(&store, TARGET);
        let other = seed_tape(&store, TARGET);
        let group = VirtualGroup::from(3);
        let mine = seed_track(&store, target, TARGET, TrackNumber(0), group);
        let theirs = seed_track(&store, other, TARGET, TrackNumber(0), group);

        let stats =
            delete_tape_tracks(&store, target, EXPIRY, 2, TAPENET_PROFILE).unwrap();

        assert_eq!(stats.tracks_deleted, 1);
        assert!(!store.has_track(mine).unwrap());
        assert!(store.has_track(theirs).unwrap());
        assert!(store.get_tape(other).unwrap().is_some());
    }

    // a tape renewed or reserved again after the sweep listed it keeps its tracks
    #[test]
    fn live_again() {
        let store = test_store();
        let group = VirtualGroup::from(3);
        let later = EpochNumber(EXPIRY.0 + 5);

        // Renewed: the row is live again and its tree starts past the old track.
        let renewed = seed_tape(&store, TARGET);
        let row = TapeInfo {
            end_epoch: later,
            tree_start: TrackNumber(1),
            ..TapeInfo::new(TARGET, TapeFlags::NONE, EXPIRY, WRITTEN)
        };
        store.put_tape(renewed, row).unwrap();
        let old = seed_track(&store, renewed, TARGET, TrackNumber(0), group);
        let new = seed_track(&store, renewed, TARGET, TrackNumber(1), group);

        // Reserved again: the address holds another tape number.
        let reserved = seed_tape(&store, TARGET);
        store
            .put_tape(reserved, TapeInfo::new(OTHER, TapeFlags::NONE, later, TrackNumber(1)))
            .unwrap();
        let fresh = seed_track(&store, reserved, OTHER, TrackNumber(0), group);

        for tape in [renewed, reserved] {
            let stats =
                delete_tape_tracks(&store, tape, EXPIRY, 2, TAPENET_PROFILE).unwrap();
            assert_eq!(stats.tracks_deleted, 0);
        }

        // The old track has left its tape, and the sweep of such tracks takes it.
        for track in [old, new, fresh] {
            assert!(store.has_track(track).unwrap());
        }
    }

    // the walk stops at the expired row's track counter, short of a later write
    #[test]
    fn renewed_mid_walk() {
        let store = test_store();
        let group = VirtualGroup::from(3);
        let tape = Address::new_unique();
        store
            .put_tape(tape, TapeInfo::new(TARGET, TapeFlags::NONE, EXPIRY, TrackNumber(2)))
            .unwrap();
        let old: Vec<Address> = (0..2)
            .map(|n| seed_track(&store, tape, TARGET, TrackNumber(n), group))
            .collect();
        let written_after = seed_track(&store, tape, TARGET, TrackNumber(2), group);

        let stats =
            delete_tape_tracks(&store, tape, EXPIRY, 1, TAPENET_PROFILE).unwrap();

        assert_eq!(stats.tracks_deleted, old.len());
        assert!(store.has_track(written_after).unwrap());
    }

    /// A memory store that counts the deletes sent to it, alone or in a batch
    struct CountingStore {
        inner: MemoryStore,
        deletes: AtomicUsize,
    }

    impl Store for CountingStore {
        fn store_engine(&self) -> tape_store_api::StoreEngine {
            self.inner.store_engine()
        }
        fn get(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<Option<Value>> {
            self.inner.get(cf, key)
        }
        fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> tape_store_api::Result<()> {
            self.inner.put(cf, key, value)
        }
        fn delete(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<()> {
            self.deletes.fetch_add(1, Ordering::Relaxed);
            self.inner.delete(cf, key)
        }
        fn contains(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<bool> {
            self.inner.contains(cf, key)
        }
        fn write_batch(&self, batch: WriteBatch) -> tape_store_api::Result<()> {
            for op in batch.iter() {
                if matches!(op, BatchOp::Delete { .. }) {
                    self.deletes.fetch_add(1, Ordering::Relaxed);
                }
            }
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
            self.inner.iter_from(cf, start, direction)
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

    // the gc sweep clears a dead track's entries every epoch, and on the reel
    // a delete of a missing row leaves a grave, so only spools that hold an
    // entry for the track get deletes, once
    #[test]
    fn repeated_clears_delete_only_rows_that_exist() {
        let store = TapeStore::new(CountingStore {
            inner: MemoryStore::new(),
            deletes: AtomicUsize::new(0),
        });
        let group = VirtualGroup::from(3);
        let track = Address::new_unique();
        let handover = EntryKey { spool: group.spool_at(2), is_handover: true, track };
        let fresh = EntryKey { spool: group.spool_at(5), is_handover: false, track };
        store.put_entry(handover, Entry::never_tried()).unwrap();
        store.put_entry(fresh, Entry::never_tried()).unwrap();

        for _ in 0..EXPIRY.0 {
            remove_track_entries(&store, track, group, TAPENET_PROFILE).unwrap();
        }

        assert!(!store.has_entry(handover.spool, track).unwrap());
        assert!(!store.has_entry(fresh.spool, track).unwrap());
        assert_eq!(store.inner().inner().deletes.load(Ordering::Relaxed), 4);
    }
}

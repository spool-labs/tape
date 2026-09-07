use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::challenge::schedule::{
    MAINNET_CADENCE_SLOTS, SAMPLE_LOOKBACK_SLOTS, round_width_slots,
};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{SlotNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store::ops::{
    ObjectInfoOps, ObjectListOps, ObjectMetadataOps, SampleOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::TapeStore;

use crate::core::error::NodeError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupStats {
    pub tapes_deleted: usize,
    pub tracks_deleted: usize,
    pub slices_deleted: usize,
}

pub fn delete_track_local<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    deleted_at: SlotNumber,
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();

    if let Some(info) = store.get_track(track).map_err(store_error)? {
        remove_object_listing_for_track(store, track, &info)?;
        stats.slices_deleted += cleanup_track_slices(store, track, info.group, deleted_at)?;
        stats.tracks_deleted += 1;
    }

    // The registration slot stays: the tombstones below reference it until no
    // round can, and a leftover row is a few bytes.
    store.delete_track(track).map_err(store_error)?;
    store.delete_object_info(track).map_err(store_error)?;
    store.delete_object_metadata(track).map_err(store_error)?;

    Ok(stats)
}

pub fn delete_tape_local<Db: Store>(
    store: &TapeStore<Db>,
    tape: Address,
    track_batch: usize,
    deleted_at: SlotNumber,
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();
    if store.get_tape(tape).map_err(store_error)?.is_some() {
        stats.tapes_deleted = 1;
    }
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_by_tape_from(tape, cursor, track_batch)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        cursor = tracks.last().map(|info| info.track_number);

        for info in &tracks {
            let track = track_pda(tape, info.track_number).0;
            remove_object_listing_for_track(store, track, info)?;
            stats.slices_deleted += cleanup_track_slices(store, track, info.group, deleted_at)?;
            store.delete_track(track).map_err(store_error)?;
            store.delete_object_info(track).map_err(store_error)?;
            store.delete_object_metadata(track).map_err(store_error)?;
            stats.tracks_deleted += 1;
        }
    }

    store.delete_tape(tape).map_err(store_error)?;
    Ok(stats)
}

pub fn cleanup_track_slices<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: GroupIndex,
    deleted_at: SlotNumber,
) -> Result<usize, NodeError> {
    // A round whose window opened before the deletion still asks about this
    // track, so the row stays until no round can. The slices stay with it: the
    // sweep drops both together when the row goes. Deleting them here left the
    // set asking for bytes every owner had already thrown away, and a whole
    // group answered nothing for that round.
    store
        .mark_track_sample_deleted(group, track, deleted_at)
        .map_err(store_error)?;

    for slice_index in 0..GROUP_SIZE {
        let spool_id = group.spool_at(slice_index);
        store
            .remove_pending_repair(spool_id, track)
            .map_err(store_error)?;
        store
            .remove_pending_recovery(spool_id, track)
            .map_err(store_error)?;
    }

    Ok(0)
}

/// Retains deleted slices until every round that sampled them has settled.
pub const DELETED_SLICE_HORIZON_SLOTS: u64 =
    SAMPLE_LOOKBACK_SLOTS + MAINNET_CADENCE_SLOTS + round_width_slots();

/// Drops deleted sample rows, slices, and encodings after the round horizon.
pub fn sweep_deleted_slices<Db: Store>(
    store: &TapeStore<Db>,
    now: SlotNumber,
) -> Result<DeletedSweep, NodeError> {
    let cutoff = SlotNumber(now.as_u64().saturating_sub(DELETED_SLICE_HORIZON_SLOTS));
    let mut swept = DeletedSweep::default();

    for (group, track) in store
        .track_samples_deleted_before(cutoff)
        .map_err(store_error)?
    {
        swept.slices += drop_track_slices(store, track, group)?;
        store
            .delete_track_sample(group, track)
            .map_err(store_error)?;
        swept.tracks += 1;
    }
    Ok(swept)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeletedSweep {
    pub tracks: usize,
    pub slices: usize,
}

pub fn drop_track_slices<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: GroupIndex,
) -> Result<usize, NodeError> {
    let mut deleted = 0usize;
    for slice_index in 0..GROUP_SIZE {
        let spool_id = group.spool_at(slice_index);
        if store.has_slice(spool_id, track).map_err(store_error)? {
            deleted += 1;
        }
        store.delete_slice(spool_id, track).map_err(store_error)?;
    }
    store.delete_track_data(track).map_err(store_error)?;
    Ok(deleted)
}

pub fn purge_spool_local<Db: Store>(
    store: &TapeStore<Db>,
    spool_id: SpoolIndex,
) -> Result<(), NodeError> {
    store
        .delete_all_slices_for_spool(spool_id)
        .map_err(store_error)?;

    store
        .clear_all_pending_repairs(spool_id)
        .map_err(store_error)?;

    store
        .clear_all_pending_recoveries(spool_id)
        .map_err(store_error)?;

    store
        .remove_spool_sync_cursor(spool_id)
        .map_err(store_error)?;

    store.remove_spool_state(spool_id).map_err(store_error)
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

    if entry.data_tape == info.tape && entry.track_number == info.track_number {
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
    use store_memory::MemoryStore;
    use tape_core::track::types::{TrackKind, TrackState};
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    // Track addresses are PDAs of (tape, track_number), which is the derivation
    // delete_tape_local relies on to walk the tape index.
    fn seed_track(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        track_number: TrackNumber,
        group: GroupIndex,
    ) -> Address {
        let track = track_pda(tape, track_number).0;

        store
            .put_track(
                track,
                CompressedTrack {
                    tape,
                    key: Hash::new_unique(),
                    track_number,
                    kind: TrackKind::Coded as u64,
                    state: TrackState::Certified as u64,
                    size: StorageUnits::from_bytes(1024),
                    group,
                    value_hash: Hash::new_unique(),
                },
            )
            .unwrap();

        for position in 0..GROUP_SIZE {
            store
                .put_slice(group.spool_at(position), track, vec![7u8; 8])
                .unwrap();
        }

        track
    }

    #[test]
    fn delete_tape_local_clears_only_its_own_tape() {
        let store = test_store();
        let target = Address::new_unique();
        let other = Address::new_unique();
        let group = GroupIndex::from(3);

        let mine: Vec<Address> = (0..5)
            .map(|n| seed_track(&store, target, TrackNumber(n), group))
            .collect();
        let theirs: Vec<Address> = (0..3)
            .map(|n| seed_track(&store, other, TrackNumber(n), group))
            .collect();

        // Batch under the track count, so the tape cursor has to advance.
        let stats = delete_tape_local(&store, target, 2, SlotNumber(10)).unwrap();

        assert_eq!(stats.tracks_deleted, mine.len());
        assert_eq!(stats.slices_deleted, 0);

        for track in mine {
            assert!(!store.has_track(track).unwrap());
            assert!(store.has_slice(group.spool_at(0), track).unwrap());
        }

        for track in theirs {
            assert!(store.has_track(track).unwrap());
            assert!(store.has_slice(group.spool_at(0), track).unwrap());
        }

        assert!(store
            .iter_tracks_by_tape_from(target, None, 16)
            .unwrap()
            .is_empty());
        assert_eq!(
            store.iter_tracks_by_tape_from(other, None, 16).unwrap().len(),
            3
        );
    }
}

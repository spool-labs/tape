use store::Store;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::{
    ObjectInfoOps, ObjectListOps, ObjectMetadataOps, SliceOps, SpoolOps, TapeOps, TrackDataOps,
    TrackOps,
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
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();

    if let Some(info) = store.get_track(track).map_err(store_error)? {
        remove_object_listing_for_track(store, track, &info)?;
        stats.slices_deleted += cleanup_track_slices(store, track, info.group)?;
        stats.tracks_deleted += 1;
    }

    store.delete_track(track).map_err(store_error)?;
    store.delete_track_data(track).map_err(store_error)?;
    store.delete_object_info(track).map_err(store_error)?;
    store.delete_object_metadata(track).map_err(store_error)?;

    Ok(stats)
}

pub fn delete_tape_local<Db: Store>(
    store: &TapeStore<Db>,
    tape: Address,
    track_batch: usize,
) -> Result<CleanupStats, NodeError> {
    let mut stats = CleanupStats::default();
    if store.get_tape(tape).map_err(store_error)?.is_some() {
        stats.tapes_deleted = 1;
    }
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, track_batch)
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, info) in &tracks {
            if info.tape == tape.into() {
                remove_object_listing_for_track(store, *track, info)?;
                stats.slices_deleted += cleanup_track_slices(store, *track, info.group)?;
                store.delete_track(*track).map_err(store_error)?;
                store.delete_track_data(*track).map_err(store_error)?;
                store.delete_object_info(*track).map_err(store_error)?;
                store.delete_object_metadata(*track).map_err(store_error)?;
                stats.tracks_deleted += 1;
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
    }

    store.delete_tape(tape).map_err(store_error)?;
    Ok(stats)
}

pub fn cleanup_track_slices<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: GroupIndex,
) -> Result<usize, NodeError> {
    let mut deleted_slices = 0usize;

    for slice_index in 0..GROUP_SIZE {
        let spool_id = group.spool_at(slice_index);

        if store.has_slice(spool_id, track).map_err(store_error)? {
            deleted_slices += 1;
        }

        store
            .delete_slice(spool_id, track)
            .map_err(store_error)?;

        store
            .remove_pending_repair(spool_id, track)
            .map_err(store_error)?;

        store
            .remove_pending_recovery(spool_id, track)
            .map_err(store_error)?;
    }

    Ok(deleted_slices)
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

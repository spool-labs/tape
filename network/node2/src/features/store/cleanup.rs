use store::Store;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::types::Pubkey;
use tape_store::TapeStore;

use crate::core::error::NodeError;

pub fn delete_track_local<Db: Store>(
    store: &TapeStore<Db>,
    track: Pubkey,
) -> Result<(), NodeError> {
    if let Some(info) = store.get_track(track).map_err(store_error)? {
        cleanup_track_slices(store, track, info.spool_group)?;
    }

    store.delete_track(track).map_err(store_error)?;
    store.delete_track_data(track).map_err(store_error)?;
    store.delete_object_info(track).map_err(store_error)
}

pub fn delete_tape_local<Db: Store>(
    store: &TapeStore<Db>,
    tape: Pubkey,
    track_batch: usize,
) -> Result<(), NodeError> {
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
                cleanup_track_slices(store, *track, info.spool_group)?;
                store.delete_track(*track).map_err(store_error)?;
                store.delete_track_data(*track).map_err(store_error)?;
                store.delete_object_info(*track).map_err(store_error)?;
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
    }

    store.delete_tape(tape).map_err(store_error)
}

pub fn cleanup_track_slices<Db: Store>(
    store: &TapeStore<Db>,
    track: Pubkey,
    spool_group: SpoolGroup,
) -> Result<(), NodeError> {
    for slice_index in 0..SPOOL_GROUP_SIZE {
        let spool_id = spool_group.spool_at(slice_index);

        store.delete_slice(spool_id, track).map_err(store_error)?;
        store
            .remove_pending_repair(spool_id, track)
            .map_err(store_error)?;
        store
            .remove_pending_recovery(spool_id, track)
            .map_err(store_error)?;
    }

    Ok(())
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

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

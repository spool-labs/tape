use store::Store;
use tape_core::types::EpochNumber;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey};
use tape_store::TapeStore;
use tokio::task::yield_now;

use crate::core::config::GcConfig;
use crate::core::error::NodeError;
use crate::features::state::cleanup::{
    cleanup_track_slices, delete_tape_local, delete_track_local,
};

pub async fn sweep_epoch<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
) -> Result<(), NodeError> {
    sweep_expired_tapes(store, config, current_epoch).await?;
    sweep_orphan_tracks(store, config).await?;
    sweep_orphan_slices(store, config).await?;
    sweep_stale_recoveries(store).await?;

    Ok(())
}

async fn sweep_expired_tapes<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
) -> Result<(), NodeError> {
    let tapes = store.iter_all_tapes().map_err(store_error)?;
    for (index, (tape, info)) in tapes.into_iter().enumerate() {
        if info.end_epoch <= current_epoch {
            delete_tape_local(store, tape, track_batch_size(config))?;
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(())
}

async fn sweep_orphan_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
) -> Result<(), NodeError> {
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, track_batch_size(config))
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, info) in &tracks {
            if store.get_tape(info.tape_address).map_err(store_error)?.is_none() {
                delete_track_local(store, *track)?;
                continue;
            }

            match store.get_object_info(*track).map_err(store_error)? {
                Some(ObjectInfo::Valid { .. }) => {}
                Some(ObjectInfo::Invalid { .. }) | Some(ObjectInfo::Blacklisted) | None => {
                    cleanup_track_slices(store, *track, info.spool_group)?;
                }
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
        yield_now().await;
    }

    Ok(())
}

async fn sweep_orphan_slices<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
) -> Result<(), NodeError> {
    let spools = store.iter_all_spools().map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let mut cursor = None;

        loop {
            let slices = store
                .iter_slices_by_spool_from(spool_id, cursor, slice_batch_size(config))
                .map_err(store_error)?;

            if slices.is_empty() {
                break;
            }

            for (track, _) in &slices {
                if should_delete_slice(store, spool_id, *track)? {
                    store.delete_slice(spool_id, *track).map_err(store_error)?;
                }
            }

            cursor = slices.last().map(|(track, _)| *track);
            yield_now().await;
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(())
}

async fn sweep_stale_recoveries<Db: Store>(store: &TapeStore<Db>) -> Result<(), NodeError> {
    let spools = store.iter_all_spools().map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let pending = store
            .iter_pending_recoveries(spool_id, usize::MAX)
            .map_err(store_error)?;

        for track in pending {
            if recovery_is_stale(store, spool_id, track)? {
                store
                    .remove_pending_recovery(spool_id, track)
                    .map_err(store_error)?;
            }
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(())
}

fn should_delete_slice<Db: Store>(
    store: &TapeStore<Db>,
    spool_id: u16,
    track: Pubkey,
) -> Result<bool, NodeError> {
    let Some(track_info) = store.get_track(track).map_err(store_error)? else {
        return Ok(true);
    };

    if !track_info.spool_group.contains(spool_id) {
        return Ok(true);
    }

    let object = store.get_object_info(track).map_err(store_error)?;
    Ok(!matches!(object, Some(ObjectInfo::Valid { .. })))
}

fn recovery_is_stale<Db: Store>(
    store: &TapeStore<Db>,
    spool_id: u16,
    track: Pubkey,
) -> Result<bool, NodeError> {
    let Some(track_info) = store.get_track(track).map_err(store_error)? else {
        return Ok(true);
    };

    if !track_info.spool_group.contains(spool_id) {
        return Ok(true);
    }

    let object = store.get_object_info(track).map_err(store_error)?;
    Ok(!matches!(object, Some(ObjectInfo::Valid { .. })))
}

fn track_batch_size(config: &GcConfig) -> usize {
    config.track_batch_size.max(1)
}

fn slice_batch_size(config: &GcConfig) -> usize {
    config.slice_batch_size.max(1)
}

fn should_yield(index: usize) -> bool {
    index > 0 && index % 32 == 0
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use store_memory::MemoryStore;
    use tape_core::spooler::SpoolGroup;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store::ops::{
        ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps,
    };
    use tape_store::types::{ObjectInfo, Pubkey, TapeInfo, TrackInfo};
    use tape_store::TapeStore;

    use super::sweep_epoch;
    use crate::core::config::GcConfig;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn test_config() -> GcConfig {
        GcConfig {
            enabled: true,
            scan_interval: Duration::from_secs(60),
            track_batch_size: 2,
            slice_batch_size: 2,
        }
    }

    fn valid_object(track: Pubkey, epoch: EpochNumber, slot: SlotNumber) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch: epoch,
            certified_epoch: None,
            slot,
        }
    }

    fn track_info(tape: Pubkey, spool_group: SpoolGroup) -> TrackInfo {
        TrackInfo {
            tape_address: tape,
            spool_group,
            original_size: 1024,
            stripe_size: 64,
            stripe_count: 2,
            encoding_type: 0,
            encoding_params: 0,
            commitment: Vec::new(),
        }
    }

    #[tokio::test]
    async fn expired_tape_sweep_cascades() {
        let store = test_store();
        let config = test_config();
        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let spool_id = 20;
        let slot = SlotNumber(10);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(2),
                },
            )
            .unwrap();
        store.put_track(track, track_info(tape, SpoolGroup(1))).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(1), slot))
            .unwrap();
        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(3)).await.unwrap();

        assert!(store.get_tape(tape).unwrap().is_none());
        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn orphan_track_without_tape_is_removed() {
        let store = test_store();
        let config = test_config();
        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store.put_track(track, track_info(tape, SpoolGroup(1))).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(2), SlotNumber(20)))
            .unwrap();
        store.put_slice(spool_id, track, vec![5, 6, 7]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6)).await.unwrap();

        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn orphan_slice_without_valid_object_is_removed() {
        let store = test_store();
        let config = test_config();
        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(10),
                },
            )
            .unwrap();
        store.put_track(track, track_info(tape, SpoolGroup(1))).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();
        store.put_slice(spool_id, track, vec![8, 8, 8]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6)).await.unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert!(matches!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::Invalid { .. })
        ));
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_recovery_without_valid_track_is_removed() {
        let store = test_store();
        let config = test_config();
        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(10),
                },
            )
            .unwrap();
        store.put_track(track, track_info(tape, SpoolGroup(1))).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();
        store.add_pending_recovery(spool_id, track).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6)).await.unwrap();

        assert!(!store.has_pending_recovery(spool_id, track).unwrap());
    }

    #[tokio::test]
    async fn rerunning_same_epoch_sweep_is_a_no_op() {
        let store = test_store();
        let config = test_config();
        let tape = Pubkey::new_unique();
        let track = Pubkey::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(1),
                },
            )
            .unwrap();
        store.put_track(track, track_info(tape, SpoolGroup(1))).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(1), SlotNumber(10)))
            .unwrap();
        store.put_slice(spool_id, track, vec![1]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(5)).await.unwrap();
        sweep_epoch(&store, &config, EpochNumber(5)).await.unwrap();

        assert!(store.get_tape(tape).unwrap().is_none());
        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }
}

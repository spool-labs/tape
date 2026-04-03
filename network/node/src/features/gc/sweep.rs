use tokio::task::yield_now;

use store::Store;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_store::{
    TapeStore,
    ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps},
    types::ObjectInfo,
};

use crate::config::store::GcConfig;
use crate::core::error::NodeError;
use crate::features::store::cleanup::{
    cleanup_track_slices, delete_tape_local, delete_track_local, CleanupStats,
};

const UNCERTIFIED_RETENTION_EPOCHS: u64 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcSweepStats {
    pub tapes_deleted: usize,
    pub tracks_deleted: usize,
    pub slices_deleted: usize,
}

pub async fn sweep_epoch<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    stats += sweep_expired_tapes(store, config, current_epoch).await?;
    stats += sweep_uncertified_tracks(store, config, current_epoch).await?;
    stats += sweep_orphan_tracks(store, config).await?;
    stats += sweep_orphan_slices(store, config).await?;
    sweep_stale_recoveries(store).await?;

    Ok(stats)
}

async fn sweep_expired_tapes<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let tapes = store.iter_all_tapes().map_err(store_error)?;
    for (index, (tape, info)) in tapes.into_iter().enumerate() {
        if info.end_epoch <= current_epoch {
            stats += delete_tape_local(store, tape, track_batch(config))?.into();
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(stats)
}

async fn sweep_uncertified_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let mut cursor = None;
    let retention = UNCERTIFIED_RETENTION_EPOCHS;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, track_batch(config))
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, info) in &tracks {
            let object = store.get_object_info(*track).map_err(store_error)?;

            if let Some(ObjectInfo::Valid {
                certified_epoch: None,
                registered_epoch,
                ..
            }) = object
            {
                if current_epoch.saturating_sub(registered_epoch).as_u64() >= retention {
                    stats.slices_deleted += cleanup_track_slices(store, *track, info.spool_group)?;
                    stats += delete_track_local(store, *track)?.into();
                }
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
        yield_now().await;
    }

    Ok(stats)
}

async fn sweep_orphan_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let mut cursor = None;

    loop {
        let tracks = store
            .iter_tracks_from(cursor, track_batch(config))
            .map_err(store_error)?;

        if tracks.is_empty() {
            break;
        }

        for (track, info) in &tracks {
            if store.get_tape(info.tape.into()).map_err(store_error)?.is_none() {
                stats += delete_track_local(store, *track)?.into();
                continue;
            }

            match store.get_object_info(*track).map_err(store_error)? {
                Some(ObjectInfo::Valid { .. }) => {}
                Some(ObjectInfo::Invalid { .. }) | Some(ObjectInfo::Blacklisted) | None => {
                    stats.slices_deleted += cleanup_track_slices(store, *track, info.spool_group)?;
                }
            }
        }

        cursor = tracks.last().map(|(track, _)| *track);
        yield_now().await;
    }

    Ok(stats)
}

async fn sweep_orphan_slices<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let spools = store.iter_all_spools().map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let mut cursor = None;

        loop {
            let slices = store
                .iter_slices_by_spool_from(spool_id, cursor, slice_batch(config))
                .map_err(store_error)?;

            if slices.is_empty() {
                break;
            }

            for (track, _) in &slices {
                if should_delete_slice(store, spool_id, *track)? {
                    store.delete_slice(spool_id, *track).map_err(store_error)?;
                    stats.slices_deleted += 1;
                }
            }

            cursor = slices.last().map(|(track, _)| *track);
            yield_now().await;
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(stats)
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
    track: Address,
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
    track: Address,
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

fn track_batch(config: &GcConfig) -> usize {
    config.track_batch.max(1)
}

fn slice_batch(config: &GcConfig) -> usize {
    config.slice_batch.max(1)
}

fn should_yield(index: usize) -> bool {
    index > 0 && index % 32 == 0
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

impl core::ops::AddAssign for GcSweepStats {
    fn add_assign(&mut self, rhs: Self) {
        self.tapes_deleted += rhs.tapes_deleted;
        self.tracks_deleted += rhs.tracks_deleted;
        self.slices_deleted += rhs.slices_deleted;
    }
}

impl From<CleanupStats> for GcSweepStats {
    fn from(value: CleanupStats) -> Self {
        Self {
            tapes_deleted: value.tapes_deleted,
            tracks_deleted: value.tracks_deleted,
            slices_deleted: value.slices_deleted,
        }
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use store_memory::MemoryStore;
    use tape_core::spooler::SpoolGroup;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, TrackNumber};
    use tape_crypto::Hash;
    use tape_store::{
        TapeStore,
        ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps},
        types::{ObjectInfo, TapeInfo},
    };

    use super::sweep_epoch;
    use crate::config::store::GcConfig;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn test_config() -> GcConfig {
        GcConfig {
            enabled: true,
            interval_secs: 60,
            track_batch: 2,
            slice_batch: 2,
            reclaim_min_deleted_slices: 20,
        }
    }

    fn valid_object(track: Address, epoch: EpochNumber, slot: SlotNumber) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch: epoch,
            certified_epoch: None,
            slot,
        }
    }

    fn track_info(tape: Address, spool_group: SpoolGroup) -> CompressedTrack {
        CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(1024),
            spool_group,
            value_hash: Hash::new_unique(),
        }
    }

    #[tokio::test]
    async fn expires_tape() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let track = Address::new_unique();
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
                    next_track_number: TrackNumber(0),
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
    async fn removes_orphan_track() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let track = Address::new_unique();
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
    async fn removes_orphan_slice() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
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
    async fn removes_stale_recovery() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
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
    async fn repeat_noop() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let track = Address::new_unique();
        let spool_id = 20;

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(1),
                    next_track_number: TrackNumber(0),
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

    #[tokio::test]
    async fn sweeps_stale_uncertified() {
        let store = test_store();
        let config = test_config();

        let tape = Address::new_unique();
        let track_stale = Address::new_unique();
        let track_recent = Address::new_unique();
        let spool_group = SpoolGroup(1);
        let spool_id = spool_group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(5)))
            .unwrap();
        store
            .put_tape(tape, TapeInfo { end_epoch: EpochNumber(20), next_track_number: TrackNumber(0) })
            .unwrap();

        // Stale uncertified: registered epoch 2, current epoch 5 -> age 3 >= threshold 2
        store.put_track(track_stale, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track_stale,
                ObjectInfo::Valid {
                    track_address: track_stale,
                    registered_epoch: EpochNumber(2),
                    certified_epoch: None,
                    slot: SlotNumber(10),
                },
            )
            .unwrap();
        store.put_slice(spool_id, track_stale, vec![1, 2, 3]).unwrap();

        // Recent uncertified: registered epoch 4, current epoch 5 -> age 1 < threshold 2
        store.put_track(track_recent, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track_recent,
                ObjectInfo::Valid {
                    track_address: track_recent,
                    registered_epoch: EpochNumber(4),
                    certified_epoch: None,
                    slot: SlotNumber(40),
                },
            )
            .unwrap();
        store.put_slice(spool_id, track_recent, vec![4, 5, 6]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(5)).await.unwrap();

        // Stale track should be gone
        assert!(store.get_track(track_stale).unwrap().is_none());
        assert!(store.get_object_info(track_stale).unwrap().is_none());
        assert!(store.get_slice(spool_id, track_stale).unwrap().is_none());

        // Recent track should remain
        assert!(store.get_track(track_recent).unwrap().is_some());
        assert!(store.get_object_info(track_recent).unwrap().is_some());
        assert!(store.get_slice(spool_id, track_recent).unwrap().is_some());
    }
}

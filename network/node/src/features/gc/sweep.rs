use std::collections::HashSet;
use tokio::task::yield_now;

use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_core::types::{EpochNumber, SlotNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_store::{
    TapeStore,
    ops::{ChallengeOps, ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps},
    types::ObjectInfo,
};
use tracing::debug;

use crate::config::store::GcConfig;
use crate::core::error::NodeError;
use crate::features::block::pending_tracks::PendingTracks;
use crate::features::store::cleanup::{
    cleanup_track_slices, delete_tape_local, delete_track_local, CleanupStats,
};

const UNCERTIFIED_RETENTION_EPOCHS: u64 = 2;

/// Closed epochs of per-round challenge outcomes kept behind the current one.
///
/// A record's recency is rebuilt from the rounds as stored and a run of misses
/// can cross a boundary, so the epoch that just closed has to stay readable.
/// Anything older is history: nothing folds into it and no eviction reads it.
const ROUND_RETENTION_EPOCHS: u64 = 1;

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
    epoch_start_slot: SlotNumber,
    owned_spools: &HashSet<SpoolIndex>,
    pending: &PendingTracks,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    stats += sweep_expired_tapes(store, config, current_epoch, epoch_start_slot, at_tip, profile)
        .await?;
    stats +=
        sweep_uncertified_tracks(store, config, current_epoch, owned_spools, at_tip, profile)
            .await?;
    stats += sweep_orphan_tracks(store, config, epoch_start_slot, at_tip, profile).await?;
    stats += sweep_orphan_slices(store, config, pending, at_tip).await?;

    sweep_stale_recoveries(store, pending, at_tip).await?;

    let keep_from = EpochNumber(
        current_epoch
            .as_u64()
            .saturating_sub(ROUND_RETENTION_EPOCHS),
    );
    store.prune_rounds_before(keep_from).map_err(store_error)?;

    Ok(stats)
}

/// Delete every tape expired as of `current_epoch`, with its tracks; a no-op behind the tip
pub async fn sweep_expired_tapes<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
    epoch_start_slot: SlotNumber,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    // Local expiry is only trustworthy when our catalog is current. A node
    // that is behind may not have applied an ExtendTape event yet.
    if !at_tip {
        return Ok(stats);
    }

    let tapes = store.iter_all_tapes().map_err(store_error)?;
    for (index, (tape, info)) in tapes.into_iter().enumerate() {
        if info.end_epoch <= current_epoch {
            stats +=
                delete_tape_local(store, tape, track_batch(config), epoch_start_slot, profile)?
                    .into();
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
    owned_spools: &HashSet<SpoolIndex>,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    // Local certify status is only trustworthy when our catalog is current. A
    // node that is behind may simply not have applied the CertifyTrack event yet.
    if !at_tip {
        return Ok(stats);
    }

    let mut cursor: Option<Vec<u8>> = None;
    let retention = UNCERTIFIED_RETENTION_EPOCHS;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), track_batch(config))
            .map_err(store_error)?;

        let mut keys: Vec<(Address, TrackNumber)> = Vec::with_capacity(tracks.len());
        for (track, info) in &tracks {
            keys.push((*track, info.track_number));
        }
        // One call for the batch's certify status, since the decision is per row
        // but the read need not be.
        let objects = store.get_object_infos(&keys).map_err(store_error)?;

        for ((track, info), object) in tracks.iter().zip(objects) {
            if let Some(ObjectInfo::Valid {
                certified_epoch: None,
                registered_epoch,
                ..
            }) = object
            {
                if current_epoch.saturating_sub(registered_epoch).as_u64() >= retention {
                    stats.slices_deleted += cleanup_unowned_track_slices(
                        store,
                        *track,
                        info.group,
                        owned_spools,
                        profile,
                    )?;
                }
            }
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        yield_now().await;
    }

    Ok(stats)
}

fn cleanup_unowned_track_slices<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: GroupIndex,
    owned_spools: &HashSet<SpoolIndex>,
    profile: ErasureCode,
) -> Result<usize, NodeError> {
    let mut deleted_slices = 0usize;

    for slice_index in 0..usize::from(profile.n()) {
        let spool_id = group.spool_at(slice_index);

        if owned_spools.contains(&spool_id) {
            continue;
        }

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

async fn sweep_orphan_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    epoch_start_slot: SlotNumber,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), track_batch(config))
            .map_err(store_error)?;

        let mut keys: Vec<(Address, TrackNumber)> = Vec::with_capacity(tracks.len());
        let mut tapes: Vec<Address> = Vec::with_capacity(tracks.len());
        for (track, info) in &tracks {
            keys.push((*track, info.track_number));
            tapes.push(info.tape);
        }
        // Both of the batch's reads once each. Tracks of one tape ask for it
        // repeatedly, which a batch answers as cheaply as it answers the rest.
        let parents = store.get_tapes(&tapes).map_err(store_error)?;
        let objects = store.get_object_infos(&keys).map_err(store_error)?;

        for (((track, info), parent), object) in
            tracks.iter().zip(parents).zip(objects)
        {
            if parent.is_none() {
                if !at_tip {
                    continue;
                }

                debug!(
                    track = %track,
                    tape = %info.tape,
                    track_number = info.track_number.0,
                    "gc deleting orphan track with missing parent tape"
                );
                stats += delete_track_local(
                    store,
                    *track,
                    info.track_number,
                    epoch_start_slot,
                    profile,
                )?
                .into();
                continue;
            }

            let reclaim = match object {
                Some(object) if object.is_live() => false,
                Some(_) => true,
                None => at_tip,
            };

            if reclaim {
                stats.slices_deleted +=
                    cleanup_track_slices(store, *track, info, epoch_start_slot, profile)?;
            }
        }

        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        yield_now().await;
    }

    Ok(stats)
}

async fn sweep_orphan_slices<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    pending: &PendingTracks,
    at_tip: bool,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let spools = store.iter_all_spools().map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let mut cursor: Option<Vec<u8>> = None;

        loop {
            // Keys alone: the decision is made from the track and object rows,
            // and a page of slice payloads read here would be read to be dropped.
            let (addresses, next) = store
                .sweep_slice_keys_by_spool(spool_id, cursor.as_deref(), slice_batch(config))
                .map_err(store_error)?;

            for track in &addresses {
                if should_delete_slice(store, pending, at_tip, spool_id, *track)? {
                    // An orphan slice has no track record, so it was never in
                    // the sample set and leaves nothing behind.
                    store.delete_slice(spool_id, *track).map_err(store_error)?;
                    stats.slices_deleted += 1;
                }
            }

            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
            yield_now().await;
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(stats)
}

async fn sweep_stale_recoveries<Db: Store>(
    store: &TapeStore<Db>,
    pending: &PendingTracks,
    at_tip: bool,
) -> Result<(), NodeError> {
    let spools = store.iter_all_spools().map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let queued = store
            .iter_pending_recoveries(spool_id, usize::MAX)
            .map_err(store_error)?;

        for track in queued {
            if should_delete_slice(store, pending, at_tip, spool_id, track)? {
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

/// Whether one slice has outlived every track it belongs to
///
/// A spool holds one copy of a slice per address, so the slice stays while any
/// track under the address in this spool's group still needs it.
fn should_delete_slice<Db: Store>(
    store: &TapeStore<Db>,
    pending: &PendingTracks,
    at_tip: bool,
    spool_id: SpoolIndex,
    track: Address,
) -> Result<bool, NodeError> {
    let stored = store.tracks_by_address(track).map_err(store_error)?;
    let known = pending.apply_to_tracks(track, stored.clone());
    if known.is_empty() {
        return Ok(at_tip);
    }

    for info in known.iter().filter(|info| info.group.contains(spool_id)) {
        let is_stored = stored
            .iter()
            .any(|held| held.track_number == info.track_number);
        if !is_stored {
            return Ok(false);
        }

        let object = store
            .get_object_info(track, info.track_number)
            .map_err(store_error)?;
        let is_needed = match object {
            Some(object) => object.is_live(),
            None => !at_tip,
        };
        if is_needed {
            return Ok(false);
        }
    }

    Ok(true)
}

fn track_batch(config: &GcConfig) -> usize {
    config.track_batch.max(1)
}

fn slice_batch(config: &GcConfig) -> usize {
    config.slice_batch.max(1)
}

fn should_yield(index: usize) -> bool {
    index > 0 && index.is_multiple_of(32)
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
    use std::collections::HashSet;

    use tape_store_memory::MemoryStore;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, RoundNumber, SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_core::challenge::sample::EntryKind;
    use tape_store::{
        ops::{
            ChallengeOps, ObjectInfoOps, SampleOps, SliceOps, SpoolOps, TapeOps, TrackDataOps,
            TrackOps,
        },
        types::{ObjectInfo, SystemObjectKind, TapeInfo, TrackSample},
        TapeStore,
    };

    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;

    use super::{should_delete_slice, sweep_epoch, PendingTracks};
    use crate::features::store::cleanup::{
        cleanup_track_slices, sweep_deleted_slices, DELETED_SLICE_HORIZON_SLOTS,
    };
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

    fn owned_spools(spools: &[SpoolIndex]) -> HashSet<SpoolIndex> {
        spools.iter().copied().collect()
    }

    const NUMBER: TrackNumber = TrackNumber(0);

    fn inline_sample() -> TrackSample {
        TrackSample {
            kind: EntryKind::Inline,
            value_hash: Hash::default(),
            registered_slot: SlotNumber(1),
            deleted_slot: None,
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

    fn track_info(tape: Address, track_number: TrackNumber, group: GroupIndex) -> CompressedTrack {
        CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number,
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(1024),
            group,
            value_hash: Hash::new_unique(),
        }
    }

    #[tokio::test]
    async fn expires_tape() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);
        let slot = SlotNumber(10);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(2),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, TrackNumber(0), valid_object(track, EpochNumber(1), slot))
            .unwrap();
        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(3), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_tape(tape).unwrap().is_none());
        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_object_info(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    // expired tape survives a sweep behind the tip and dies once caught up
    #[tokio::test]
    async fn expiry_needs_tip() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(2),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .expect("put tape");

        sweep_epoch(&store, &config, EpochNumber(3), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), false, TAPENET_PROFILE)
            .await
            .expect("sweep behind tip");

        assert!(store.get_tape(tape).expect("get tape").is_some());

        sweep_epoch(&store, &config, EpochNumber(3), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .expect("sweep at tip");

        assert!(store.get_tape(tape).expect("get tape").is_none());
    }

    #[tokio::test]
    async fn removes_orphan_track() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, TrackNumber(0), valid_object(track, EpochNumber(2), SlotNumber(20)))
            .unwrap();
        store.put_slice(spool_id, track, vec![5, 6, 7]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_object_info(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn removes_orphan_slice() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();
        store.put_slice(spool_id, track, vec![8, 8, 8]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_some());
        assert!(matches!(
            store.get_object_info(track, TrackNumber(0)).unwrap(),
            Some(ObjectInfo::Invalid { .. })
        ));
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn keeps_snapshot_slices() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot {
                        epoch: EpochNumber(3),
                    },
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: Some(EpochNumber(3)),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();
        store.put_slice(spool_id, track, vec![8, 8, 8]).unwrap();
        store.add_pending_recovery(spool_id, track).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_some());
        assert!(matches!(
            store.get_object_info(track, TrackNumber(0)).unwrap(),
            Some(ObjectInfo::System {
                kind: SystemObjectKind::Snapshot { .. },
                ..
            })
        ));
        assert!(store.get_slice(spool_id, track).unwrap().is_some());
        assert!(store.has_pending_recovery(spool_id, track).unwrap());
    }

    #[tokio::test]
    async fn removes_stale_recovery() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(2)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                },
            )
            .unwrap();
        store.add_pending_recovery(spool_id, track).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(!store.has_pending_recovery(spool_id, track).unwrap());
    }

    #[tokio::test]
    async fn repeat_noop() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), GroupIndex(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(1),
                    next_track_number: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    share_nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, TrackNumber(0), valid_object(track, EpochNumber(1), SlotNumber(10)))
            .unwrap();
        store.put_slice(spool_id, track, vec![1]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(5), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();
        sweep_epoch(&store, &config, EpochNumber(5), SlotNumber(0), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_tape(tape).unwrap().is_none());
        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_object_info(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn sweeps_stale_uncertified() {
        let store = test_store();
        let config = test_config();

        let tape = Address::new_unique();
        let group = GroupIndex(1);
        let info_stale = track_info(tape, TrackNumber(0), group);
        let info_recent = track_info(tape, TrackNumber(1), group);
        let track_stale = info_stale.address();
        let track_recent = info_recent.address();
        let owned_spool = group.spool_at(0);
        let unowned_spool = group.spool_at(1);

        store
            .set_spool_state(owned_spool, SpoolState::new(SpoolStatus::Active, EpochNumber(5)))
            .unwrap();
        store
            .set_spool_state(unowned_spool, SpoolState::new(SpoolStatus::Active, EpochNumber(5)))
            .unwrap();
        store
            .put_tape(tape, TapeInfo::new(TapeNumber(1), 0, EpochNumber(20), TrackNumber(0)))
            .unwrap();

        // Stale uncertified: registered epoch 2, current epoch 5 -> age 3 >= threshold 2
        store.put_track(track_stale, info_stale).unwrap();
        store
            .put_object_info(
                track_stale,
                TrackNumber(0),
                ObjectInfo::Valid {
                    track_address: track_stale,
                    registered_epoch: EpochNumber(2),
                    certified_epoch: None,
                    slot: SlotNumber(10),
                },
            )
            .unwrap();
        store.put_slice(owned_spool, track_stale, vec![1, 2, 3]).unwrap();
        store.put_slice(unowned_spool, track_stale, vec![3, 2, 1]).unwrap();
        store.add_pending_repair(unowned_spool, track_stale).unwrap();
        store.add_pending_recovery(unowned_spool, track_stale).unwrap();

        // Recent uncertified: registered epoch 4, current epoch 5 -> age 1 < threshold 2
        store.put_track(track_recent, info_recent).unwrap();
        store
            .put_object_info(
                track_recent,
                TrackNumber(1),
                ObjectInfo::Valid {
                    track_address: track_recent,
                    registered_epoch: EpochNumber(4),
                    certified_epoch: None,
                    slot: SlotNumber(40),
                },
            )
            .unwrap();
        store.put_slice(unowned_spool, track_recent, vec![4, 5, 6]).unwrap();

        sweep_epoch(
            &store,
            &config,
            EpochNumber(5),
            SlotNumber(0),
            &owned_spools(&[owned_spool]),
            &PendingTracks::new(),
            true,
            TAPENET_PROFILE,
        )
        .await
        .unwrap();

        // Stale uncertified metadata is proof substrate and must remain.
        assert!(store.get_track(track_stale, TrackNumber(0)).unwrap().is_some());
        assert!(store.get_object_info(track_stale, TrackNumber(0)).unwrap().is_some());
        assert!(store.get_slice(owned_spool, track_stale).unwrap().is_some());
        assert!(store.get_slice(unowned_spool, track_stale).unwrap().is_none());
        assert!(!store.has_pending_repair(unowned_spool, track_stale).unwrap());
        assert!(!store.has_pending_recovery(unowned_spool, track_stale).unwrap());

        // Recent track should remain
        assert!(store.get_track(track_recent, TrackNumber(1)).unwrap().is_some());
        assert!(store.get_object_info(track_recent, TrackNumber(1)).unwrap().is_some());
        assert!(store.get_slice(unowned_spool, track_recent).unwrap().is_some());
    }

    #[tokio::test]
    async fn orphan_slice_deferred_when_not_at_tip() {
        let store = test_store();
        let pending = PendingTracks::new();
        let spool_id = SpoolIndex(20);
        let track = Address::new_unique();

        // Slice on disk with no track record anywhere.
        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();

        // Behind the durable tip: "absent locally" is ambiguous, so defer.
        assert!(!should_delete_slice(&store, &pending, false, spool_id, track).unwrap());
        // Caught up: the track is genuinely absent on-chain, so reclaim.
        assert!(should_delete_slice(&store, &pending, true, spool_id, track).unwrap());
    }

    #[tokio::test]
    async fn slice_kept_when_register_only_pending() {
        let store = test_store();
        let pending = PendingTracks::new();
        let group = GroupIndex(1);
        let spool_id = group.spool_at(0);
        let track = Address::new_unique();

        // Registration is only in the confirmed overlay; nothing durable yet —
        // exactly the just-uploaded window that must not be reclaimed even at tip.
        pending.apply_register(
            SlotNumber(10),
            track,
            track_info(Address::new_unique(), TrackNumber(0), group),
            tape_core::track::data::BlobData::Inline(vec![]),
        );
        store.put_slice(spool_id, track, vec![9, 9, 9]).unwrap();

        assert!(!should_delete_slice(&store, &pending, true, spool_id, track).unwrap());
    }

    // a slice stays while any track under its address in the group still needs it
    #[tokio::test]
    async fn shared_slice() {
        let store = test_store();
        let pending = PendingTracks::new();
        let tape = Address::new_unique();
        let group = GroupIndex(1);
        let spool_id = group.spool_at(0);
        let track = Address::new_unique();

        for number in [0, 3] {
            store.put_track(track, track_info(tape, TrackNumber(number), group)).unwrap();
        }
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Invalid { epoch: EpochNumber(3), slot: SlotNumber(30) },
            )
            .unwrap();
        store
            .put_object_info(track, TrackNumber(3), valid_object(track, EpochNumber(3), SlotNumber(31)))
            .unwrap();
        store.put_slice(spool_id, track, vec![9, 9, 9]).unwrap();

        assert!(!should_delete_slice(&store, &pending, true, spool_id, track).unwrap());

        // The other group's spools never held this address.
        assert!(should_delete_slice(&store, &pending, true, GroupIndex(2).spool_at(0), track).unwrap());

        store
            .put_object_info(
                track,
                TrackNumber(3),
                ObjectInfo::Invalid { epoch: EpochNumber(4), slot: SlotNumber(40) },
            )
            .unwrap();
        assert!(should_delete_slice(&store, &pending, true, spool_id, track).unwrap());
    }

    // a deleted track keeps its slices while a round can still draw it, and only
    // that long: tying them to the epoch sweep would hold them for a week on
    // mainnet
    #[tokio::test]
    async fn slices_outlive_the_delete_by_a_round_horizon() {
        let store = test_store();
        let track = Address::new_unique();
        let group = GroupIndex(0);
        let spool = group.spool_at(0);
        let info = track_info(Address::new_unique(), NUMBER, group);

        store.put_slice(spool, track, vec![7u8; 32]).unwrap();
        store
            .put_track_data(track, NUMBER, tape_core::track::data::BlobData::Inline(vec![1, 2, 3]))
            .unwrap();
        store
            .put_track_sample(group, track, NUMBER, inline_sample())
            .unwrap();

        cleanup_track_slices(&store, track, &info, SlotNumber(10), TAPENET_PROFILE).unwrap();
        assert!(
            store.get_slice(spool, track).unwrap().is_some(),
            "the delete took bytes a live round still asks for"
        );
        assert!(
            store.get_track_data(track, NUMBER).unwrap().is_some(),
            "the delete took the encoding an observer verifies against"
        );

        // Inside the horizon a round can still draw it, so the bytes stay.
        let inside = SlotNumber(10 + DELETED_SLICE_HORIZON_SLOTS);
        assert_eq!(sweep_deleted_slices(&store, inside, TAPENET_PROFILE).unwrap().tracks, 0);
        assert!(store.get_slice(spool, track).unwrap().is_some());

        // Past it, nothing can, and the row goes with its slices and encoding.
        // One horizon owns all three, or whichever went first strands the rest.
        let past = SlotNumber(11 + DELETED_SLICE_HORIZON_SLOTS);
        let swept = sweep_deleted_slices(&store, past, TAPENET_PROFILE).unwrap();
        assert_eq!((swept.tracks, swept.slices), (1, 1));
        assert!(store.get_slice(spool, track).unwrap().is_none());
        assert!(store.get_track_data(track, NUMBER).unwrap().is_none());
        assert!(store.track_sample(group, track, NUMBER).unwrap().is_none());
    }

    // two tracks with one address in a group share its slices, which go only
    // when the second one's row is swept
    #[tokio::test]
    async fn shared_slices_outlive_first_delete() {
        let store = test_store();
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex(0);
        let spool = group.spool_at(0);
        let first = track_info(tape, TrackNumber(2), group);
        let second = track_info(tape, TrackNumber(5), group);

        store.put_slice(spool, track, vec![7u8; 32]).unwrap();
        for info in [first, second] {
            store.put_track(track, info).unwrap();
            store
                .put_track_data(track, info.track_number, tape_core::track::data::BlobData::Inline(vec![1]))
                .unwrap();
            store
                .put_track_sample(group, track, info.track_number, inline_sample())
                .unwrap();
        }
        store.add_pending_repair(spool, track).unwrap();

        cleanup_track_slices(&store, track, &first, SlotNumber(10), TAPENET_PROFILE).unwrap();
        store.delete_track(track, first.track_number).unwrap();
        assert!(store.has_pending_repair(spool, track).unwrap());

        let past = SlotNumber(11 + DELETED_SLICE_HORIZON_SLOTS);
        let swept = sweep_deleted_slices(&store, past, TAPENET_PROFILE).unwrap();
        assert_eq!((swept.tracks, swept.slices), (1, 0));
        assert!(store.get_slice(spool, track).unwrap().is_some());
        assert!(store.get_track_data(track, TrackNumber(2)).unwrap().is_none());
        assert!(store.get_track_data(track, TrackNumber(5)).unwrap().is_some());

        cleanup_track_slices(&store, track, &second, past, TAPENET_PROFILE).unwrap();
        store.delete_track(track, second.track_number).unwrap();
        assert!(!store.has_pending_repair(spool, track).unwrap());

        let later = SlotNumber(past.as_u64() + 1 + DELETED_SLICE_HORIZON_SLOTS);
        let swept = sweep_deleted_slices(&store, later, TAPENET_PROFILE).unwrap();
        assert_eq!((swept.tracks, swept.slices), (1, 1));
        assert!(store.get_slice(spool, track).unwrap().is_none());
        assert!(store.get_track_data(track, TrackNumber(5)).unwrap().is_none());
    }

    // the sweep drops per-round outcomes older than the epoch that just closed,
    // and keeps that one, since a run of misses can cross the boundary
    #[tokio::test]
    async fn prunes_old_rounds() {
        let store = test_store();
        let peer = Address::new_unique();
        let spool = SpoolIndex(7);

        for epoch in 1..=3u64 {
            store
                .put_round_outcome(peer, spool, EpochNumber(epoch), RoundNumber(0), false)
                .unwrap();
        }

        sweep_epoch(
            &store,
            &test_config(),
            EpochNumber(3),
            SlotNumber(100),
            &owned_spools(&[]),
            &PendingTracks::new(),
            true,
            TAPENET_PROFILE,
        )
        .await
        .unwrap();

        let kept: Vec<EpochNumber> = store
            .peer_rounds(peer, spool)
            .unwrap()
            .into_iter()
            .map(|(epoch, _, _)| epoch)
            .collect();
        assert_eq!(kept, vec![EpochNumber(2), EpochNumber(3)]);
    }
}

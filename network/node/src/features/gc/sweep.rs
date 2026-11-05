use std::collections::HashSet;
use tokio::task::yield_now;

use tape_store_api::Store;
use tape_core::encoding::ErasureCode;
use tape_core::spooler::VirtualGroup;
use tape_core::types::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_core::track::types::CompressedTrack;
use tape_store::{
    TapeStore,
    ops::{
        ObjectInfoOps, RemovalOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps,
        track_has_left, STORED_SPOOL_COUNT,
    },
    types::ObjectInfo,
};
use tracing::debug;

use crate::config::store::GcConfig;
use crate::core::error::NodeError;
use crate::features::block::pending_tracks::PendingTracks;
use crate::features::store::cleanup::{
    delete_tape_tracks, delete_track_local, remove_track_entries, CleanupStats,
};

const UNCERTIFIED_RETENTION_EPOCHS: u64 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcSweepStats {
    pub tapes_cleared: usize,
    pub tracks_deleted: usize,
    pub slices_deleted: usize,
}

pub async fn sweep_epoch<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
    owned_spools: &HashSet<SpoolIndex>,
    pending: &PendingTracks,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    stats += sweep_expired_tapes(store, config, current_epoch, profile).await?;
    stats +=
        sweep_uncertified_tracks(store, config, current_epoch, owned_spools, at_tip, profile)
            .await?;
    stats += sweep_orphan_tracks(store, config, current_epoch, at_tip, profile).await?;
    stats += sweep_orphan_slices(store, config, current_epoch, pending, at_tip).await?;

    // A removal entry is read by its own epoch's assignment round and by this
    // sweep, and the store only reaches an epoch once the round before it is over.
    store.prune_removals_before(current_epoch).map_err(store_error)?;

    Ok(stats)
}

/// Delete the tracks of every tape expired as of `current_epoch`. The store
/// has to have reached that epoch, or an extension may still be on its way.
/// The tape's row stays, since a renew brings the tape back under it.
pub async fn sweep_expired_tapes<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();

    // The list is a moment old by the time a tape is reached, so the delete
    // reads the tape's row again before it removes anything.
    let tapes = store.iter_all_tapes().map_err(store_error)?;
    for (index, (tape, info)) in tapes.into_iter().enumerate() {
        if info.end_epoch <= current_epoch {
            stats +=
                delete_tape_tracks(store, tape, current_epoch, track_batch(config), profile)?
                    .into();
        }

        if should_yield(index) {
            yield_now().await;
        }
    }

    Ok(stats)
}

/// Delete every track that has left its tape. A gateway runs this pass: it
/// builds no assignment, so no round still counts such a track.
pub async fn sweep_left_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), track_batch(config))
            .map_err(store_error)?;

        let tapes: Vec<Address> = tracks.iter().map(|(_, info)| info.tape).collect();
        let parents = store.get_tapes(&tapes).map_err(store_error)?;

        for ((track, info), parent) in tracks.iter().zip(parents) {
            if track_has_left(parent.as_ref(), info) {
                stats += delete_track_local(store, *track, profile)?.into();
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

        let mut addresses: Vec<Address> = Vec::with_capacity(tracks.len());
        for (track, _) in &tracks {
            addresses.push(*track);
        }
        // One call for the batch's certify status, since the decision is per row
        // but the read need not be.
        let objects = store.get_object_infos(&addresses).map_err(store_error)?;

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
    group: VirtualGroup,
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
    }

    Ok(deleted_slices)
}

async fn sweep_orphan_tracks<Db: Store>(
    store: &TapeStore<Db>,
    config: &GcConfig,
    current_epoch: EpochNumber,
    at_tip: bool,
    profile: ErasureCode,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let mut cursor: Option<Vec<u8>> = None;

    loop {
        let (tracks, next) = store
            .sweep_tracks(cursor.as_deref(), track_batch(config))
            .map_err(store_error)?;

        let mut addresses: Vec<Address> = Vec::with_capacity(tracks.len());
        let mut tapes: Vec<Address> = Vec::with_capacity(tracks.len());
        for (track, info) in &tracks {
            addresses.push(*track);
            tapes.push(info.tape);
        }
        // Both of the batch's reads once each. Tracks of one tape ask for it
        // repeatedly, which a batch answers as cheaply as it answers the rest.
        let parents = store.get_tapes(&tapes).map_err(store_error)?;
        let objects = store.get_object_infos(&addresses).map_err(store_error)?;

        for (((track, info), parent), object) in
            tracks.iter().zip(parents).zip(objects)
        {
            // A track leaves when its tape is closed, rewound or renewed. It
            // stays through the epoch it left in, whose assignment round
            // counts it, and goes once that epoch is over.
            if track_has_left(parent.as_ref(), info) {
                if !at_tip {
                    continue;
                }

                let is_counted = store
                    .removals_from(info.tape, info.tape_number, current_epoch)
                    .map_err(store_error)?
                    .iter()
                    .any(|tree_start| info.track_number >= *tree_start);
                if is_counted {
                    continue;
                }

                debug!(
                    track = %track,
                    tape = %info.tape,
                    track_number = info.track_number.0,
                    "gc deleting a track that has left its tape"
                );
                stats += delete_track_local(store, *track, profile)?.into();
                continue;
            }

            // A deleted track stays through its own epoch, whose assignment
            // round counts it, and goes once that epoch is over.
            if let Some(ObjectInfo::Deleted { deleted_epoch, .. }) = object {
                if deleted_epoch < current_epoch {
                    stats += delete_track_local(store, *track, profile)?.into();
                }
                continue;
            }

            // An invalidated track a round counted keeps its data through
            // its own epoch too. Once that epoch is over the data goes and
            // the record forgets what it was counted by. The row stays.
            if let Some(ObjectInfo::Invalid {
                epoch,
                slot,
                counted: Some(_),
            }) = object
            {
                if epoch < current_epoch {
                    store.delete_track_data(*track).map_err(store_error)?;
                    let settled = ObjectInfo::Invalid {
                        epoch,
                        slot,
                        counted: None,
                    };
                    store.put_object_info(*track, settled).map_err(store_error)?;
                }
                continue;
            }

            // The slice sweep after this pass deletes the slices of these tracks.
            let is_dead = match object {
                Some(object) if object.is_live() => false,
                Some(_) => true,
                None => at_tip,
            };

            if is_dead {
                remove_track_entries(store, *track, info.group, profile)?;
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
    current_epoch: EpochNumber,
    pending: &PendingTracks,
    at_tip: bool,
) -> Result<GcSweepStats, NodeError> {
    let mut stats = GcSweepStats::default();
    let spools = store
        .get_spool_states(None, STORED_SPOOL_COUNT)
        .map_err(store_error)?;
    for (index, (spool_id, _)) in spools.into_iter().enumerate() {
        let mut cursor: Option<Vec<u8>> = None;

        loop {
            // Keys alone: the decision is made from the track and object rows,
            // and a page of slice payloads read here would be read to be dropped.
            let (addresses, next) = store
                .sweep_slice_keys_by_spool(spool_id, cursor.as_deref(), slice_batch(config))
                .map_err(store_error)?;
            // Both reads the decision needs, once each for the page. The object
            // read is unconditional per batch rather than conditional per row,
            // which trades a few small values for a round trip a row.
            let in_store = store.get_tracks(&addresses).map_err(store_error)?;
            let objects = store.get_object_infos(&addresses).map_err(store_error)?;

            for ((track, held), object) in addresses.iter().zip(in_store).zip(objects) {
                if should_delete_slice(pending, current_epoch, at_tip, spool_id, *track, held, object) {
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

/// Whether one slice has outlived the track it belongs to
///
/// A track that was deleted or invalidated keeps its slices through the
/// epoch a round still counts it in, so its holders can still prove them.
///
/// Reads nothing. The page it belongs to read the track and its object info in
/// one call apiece, so the decision is arithmetic on what came back.
fn should_delete_slice(
    pending: &PendingTracks,
    current_epoch: EpochNumber,
    at_tip: bool,
    spool_id: SpoolIndex,
    track: Address,
    in_store: Option<CompressedTrack>,
    object: Option<ObjectInfo>,
) -> bool {
    let Some(track_info) = pending.apply_to_track(track, in_store) else {
        return at_tip;
    };

    if !track_info.group.contains(spool_id) {
        return true;
    }

    if in_store.is_none() {
        return false;
    }

    match object {
        Some(object) if object.is_live() => false,
        Some(object) => object
            .counted_through()
            .is_none_or(|epoch| epoch < current_epoch),
        None => at_tip,
    }
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
        self.tapes_cleared += rhs.tapes_cleared;
        self.tracks_deleted += rhs.tracks_deleted;
        self.slices_deleted += rhs.slices_deleted;
    }
}

impl From<CleanupStats> for GcSweepStats {
    fn from(value: CleanupStats) -> Self {
        Self {
            tapes_cleared: value.tapes_cleared,
            tracks_deleted: value.tracks_deleted,
            slices_deleted: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tape_store_memory::MemoryStore;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_recover::state::SpoolState;
    use tape_recover::track::MissingTrack;
    use tape_recover::time::WallTime;
    use tape_store::{
        ops::{
            ObjectInfoOps, RemovalOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps,
        },
        types::{CountedTrack, ObjectInfo, SystemObjectKind, TapeInfo},
        TapeStore,
    };

    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;

    use super::{should_delete_slice, sweep_epoch, PendingTracks};
    use crate::features::store::cleanup::delete_track_local;
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

    fn uploaded(spool: SpoolIndex, track: Address) -> MissingTrack {
        MissingTrack::new_upload(spool, track, WallTime(1))
    }

    fn valid_object(track: Address, epoch: EpochNumber, slot: SlotNumber) -> ObjectInfo {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch: epoch,
            certified_epoch: None,
            slot,
        }
    }

    fn track_info(tape: Address, track_number: TrackNumber, group: VirtualGroup) -> CompressedTrack {
        CompressedTrack {
            tape,
            tape_number: TapeNumber(1),
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
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);
        let slot = SlotNumber(10);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(2),
                    next_track_number: TrackNumber(1),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(1), slot))
            .unwrap();
        store.put_slice(spool_id, track, vec![1, 2, 3]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(3), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        // The row stays for a renew, and goes when the tape is closed.
        assert!(store.get_tape(tape).unwrap().is_some());
        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    // a track that left its tape stays through the sweep of the epoch it left
    // in, whose round counts it, and goes with the next sweep. A track that
    // left in an earlier epoch goes at once.
    #[tokio::test]
    async fn left_track_by_epoch() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let left_epoch = EpochNumber(6);
        let earlier = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let counted = track_info(tape, TrackNumber(1), VirtualGroup(1));

        for info in [earlier, counted] {
            let track = info.address();
            store.put_track(track, info).unwrap();
            store
                .put_object_info(track, valid_object(track, EpochNumber(2), SlotNumber(20)))
                .unwrap();
        }
        // The tape was rewound before this epoch and closed in it, which left
        // no row and an entry from the tree start it had by then.
        store
            .record_removal(tape, counted.tape_number, left_epoch, counted.track_number)
            .unwrap();

        sweep_epoch(&store, &config, left_epoch, &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(earlier.address()).unwrap().is_none());
        assert!(store.get_track(counted.address()).unwrap().is_some());
        assert!(store.get_removal(tape, counted.tape_number, left_epoch).unwrap().is_some());

        sweep_epoch(&store, &config, left_epoch.next(), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(counted.address()).unwrap().is_none());
        assert!(store.get_removal(tape, counted.tape_number, left_epoch).unwrap().is_none());
    }

    // a track that has left is kept while the node is behind the tip, where a
    // missing tape row may only be a row the replay has yet to write
    #[tokio::test]
    async fn left_track_behind_tip() {
        let store = test_store();
        let config = test_config();
        let info = track_info(Address::new_unique(), TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(2), SlotNumber(20)))
            .unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), false, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
    }

    #[tokio::test]
    async fn removes_orphan_track() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(2), SlotNumber(20)))
            .unwrap();
        store.put_slice(spool_id, track, vec![5, 6, 7]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn removes_orphan_slice() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                    counted: None,
                },
            )
            .unwrap();
        store.put_slice(spool_id, track, vec![8, 8, 8]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert!(matches!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::Invalid { .. })
        ));
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn keeps_snapshot_slices() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(u64::MAX),
                    next_track_number: TrackNumber(0),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
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
        store.put_missing_track(&uploaded(spool_id, track)).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert!(matches!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::System {
                kind: SystemObjectKind::Snapshot { .. },
                ..
            })
        ));
        assert!(store.get_slice(spool_id, track).unwrap().is_some());
        assert!(store.has_missing_track(spool_id, track).unwrap());
    }

    // a dead track's missing_list entries go with it
    #[tokio::test]
    async fn removes_dead_entry() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Invalid {
                    epoch: EpochNumber(3),
                    slot: SlotNumber(30),
                    counted: None,
                },
            )
            .unwrap();
        store.put_missing_track(&uploaded(spool_id, track)).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(!store.has_missing_track(spool_id, track).unwrap());
    }

    #[tokio::test]
    async fn repeat_noop() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();
        let spool_id = SpoolIndex(20);

        store
            .put_spool_states(&[(spool_id, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(1),
                    next_track_number: TrackNumber(1),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, valid_object(track, EpochNumber(1), SlotNumber(10)))
            .unwrap();
        store.put_slice(spool_id, track, vec![1]).unwrap();

        sweep_epoch(&store, &config, EpochNumber(5), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();
        sweep_epoch(&store, &config, EpochNumber(5), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_tape(tape).unwrap().is_some());
        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        assert!(store.get_slice(spool_id, track).unwrap().is_none());
    }

    #[tokio::test]
    async fn sweeps_stale_uncertified() {
        let store = test_store();
        let config = test_config();

        let tape = Address::new_unique();
        let group = VirtualGroup(1);
        let info_stale = track_info(tape, TrackNumber(0), group);
        let info_recent = track_info(tape, TrackNumber(1), group);
        let track_stale = info_stale.address();
        let track_recent = info_recent.address();
        let owned_spool = group.spool_at(0);
        let unowned_spool = group.spool_at(1);

        store
            .put_spool_states(&[(owned_spool, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_spool_states(&[(unowned_spool, SpoolState::new_scanning())])
            .unwrap();
        store
            .put_tape(tape, TapeInfo::new(TapeNumber(1), 0, EpochNumber(20), TrackNumber(0)))
            .unwrap();

        // Stale uncertified: registered epoch 2, current epoch 5 -> age 3 >= threshold 2
        store.put_track(track_stale, info_stale).unwrap();
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
        store.put_slice(owned_spool, track_stale, vec![1, 2, 3]).unwrap();
        store.put_slice(unowned_spool, track_stale, vec![3, 2, 1]).unwrap();

        // Recent uncertified: registered epoch 4, current epoch 5 -> age 1 < threshold 2
        store.put_track(track_recent, info_recent).unwrap();
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
        store.put_slice(unowned_spool, track_recent, vec![4, 5, 6]).unwrap();

        sweep_epoch(
            &store,
            &config,
            EpochNumber(5),
            &owned_spools(&[owned_spool]),
            &PendingTracks::new(),
            true,
            TAPENET_PROFILE,
        )
        .await
        .unwrap();

        // Stale uncertified metadata is proof substrate and must remain.
        assert!(store.get_track(track_stale).unwrap().is_some());
        assert!(store.get_object_info(track_stale).unwrap().is_some());
        assert!(store.get_slice(owned_spool, track_stale).unwrap().is_some());
        assert!(store.get_slice(unowned_spool, track_stale).unwrap().is_none());

        // Recent track should remain
        assert!(store.get_track(track_recent).unwrap().is_some());
        assert!(store.get_object_info(track_recent).unwrap().is_some());
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
        assert!(!should_delete_slice(
            &pending,
            EpochNumber(1),
            false,
            spool_id,
            track,
            store.get_track(track).unwrap(),
            store.get_object_info(track).unwrap(),
        ));
        // Caught up: the track is genuinely absent on-chain, so reclaim.
        assert!(should_delete_slice(
            &pending,
            EpochNumber(1),
            true,
            spool_id,
            track,
            store.get_track(track).unwrap(),
            store.get_object_info(track).unwrap(),
        ));
    }

    #[tokio::test]
    async fn slice_kept_when_register_only_pending() {
        let store = test_store();
        let pending = PendingTracks::new();
        let group = VirtualGroup(1);
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

        assert!(!should_delete_slice(
            &pending,
            EpochNumber(1),
            true,
            spool_id,
            track,
            store.get_track(track).unwrap(),
            store.get_object_info(track).unwrap(),
        ));
    }

    /// This tape outlives every sweep in these tests
    fn live_tape() -> TapeInfo {
        TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch: EpochNumber(10),
            next_track_number: TrackNumber(0),
            tree_start: TrackNumber(0),
            authority: Address::default(),
            delegate: Address::default(),
            nonce: Hash::default(),
        }
    }

    // a deleted track keeps the encoding the assignment round reads, and the
    // slices a challenge asks for, through its own epoch; the sweep after that
    // epoch deletes them
    #[tokio::test]
    async fn deleted_track_bytes_go_after_its_epoch() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let group = VirtualGroup(1);
        let info = track_info(tape, TrackNumber(0), group);
        let track = info.address();
        let spool = group.spool_at(0);

        store
            .put_spool_states(&[(spool, SpoolState::new_scanning())])
            .unwrap();
        store.put_tape(tape, live_tape()).unwrap();
        store.put_track(track, info).unwrap();
        store.put_slice(spool, track, vec![7u8; 32]).unwrap();
        store
            .put_track_data(track, tape_core::track::data::BlobData::Inline(vec![1, 2, 3]))
            .unwrap();
        store
            .put_object_info(track, deleted_object(track, EpochNumber(6)))
            .unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track_data(track).unwrap().is_some());
        assert!(store.get_slice(spool, track).unwrap().is_some());

        sweep_epoch(&store, &config, EpochNumber(7), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_track_data(track).unwrap().is_none());
        assert!(store.get_slice(spool, track).unwrap().is_none());
    }

    // an invalidated track a round counts keeps its data and slices through
    // its own epoch too; after it they go, and the row and the record stay
    #[tokio::test]
    async fn invalid_track_bytes_go_after_its_epoch() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let group = VirtualGroup(1);
        let info = track_info(tape, TrackNumber(0), group);
        let track = info.address();
        let spool = group.spool_at(0);
        let counted = CountedTrack {
            track_address: track,
            registered_epoch: EpochNumber(2),
            certified_epoch: EpochNumber(2),
        };
        let invalid = |counted| ObjectInfo::Invalid { epoch: EpochNumber(6), slot: SlotNumber(60), counted };

        store
            .put_spool_states(&[(spool, SpoolState::new_scanning())])
            .unwrap();
        store.put_tape(tape, live_tape()).unwrap();
        store.put_track(track, info).unwrap();
        store.put_slice(spool, track, vec![7u8; 32]).unwrap();
        store
            .put_track_data(track, tape_core::track::data::BlobData::Inline(vec![1, 2, 3]))
            .unwrap();
        store.put_object_info(track, invalid(Some(counted.clone()))).unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track_data(track).unwrap().is_some());
        assert!(store.get_slice(spool, track).unwrap().is_some());
        assert_eq!(store.get_object_info(track).unwrap(), Some(invalid(Some(counted))));

        sweep_epoch(&store, &config, EpochNumber(7), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert!(store.get_track_data(track).unwrap().is_none());
        assert!(store.get_slice(spool, track).unwrap().is_none());
        assert_eq!(store.get_object_info(track).unwrap(), Some(invalid(None)));
    }

    fn deleted_object(track: Address, deleted_epoch: EpochNumber) -> ObjectInfo {
        ObjectInfo::Deleted {
            kind: None,
            track_address: track,
            registered_epoch: EpochNumber(2),
            certified_epoch: EpochNumber(2),
            deleted_epoch,
        }
    }

    // a deleted track stays through its own epoch and goes in the next one
    #[tokio::test]
    async fn purges_deleted_track() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let info = track_info(tape, TrackNumber(0), VirtualGroup(1));
        let track = info.address();

        store
            .put_tape(
                tape,
                TapeInfo {
                    id: TapeNumber(1),
                    flags: 0,
                    end_epoch: EpochNumber(10),
                    next_track_number: TrackNumber(0),
                    tree_start: TrackNumber(0),
                    authority: Address::default(),
                    delegate: Address::default(),
                    nonce: Hash::default(),
                },
            )
            .unwrap();
        store.put_track(track, info).unwrap();
        store
            .put_object_info(track, deleted_object(track, EpochNumber(6)))
            .unwrap();

        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert!(store.get_object_info(track).unwrap().is_some());

        sweep_epoch(&store, &config, EpochNumber(7), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
    }

    // the same content written twice into one group keeps two sets of slices,
    // so deleting one track leaves the other's alone
    #[tokio::test]
    async fn own_slices() {
        let store = test_store();
        let config = test_config();
        let tape = Address::new_unique();
        let group = VirtualGroup(0);
        let spool = group.spool_at(0);
        let written = track_info(tape, TrackNumber(2), group);
        let again = CompressedTrack { track_number: TrackNumber(5), ..written };
        let (first, second) = (written.address(), again.address());
        assert_ne!(first, second);

        store
            .put_spool_states(&[(spool, SpoolState::new_scanning())])
            .unwrap();
        store.put_tape(tape, live_tape()).unwrap();
        for (track, info) in [(first, written), (second, again)] {
            let certified = ObjectInfo::Valid {
                track_address: track,
                registered_epoch: EpochNumber(2),
                certified_epoch: Some(EpochNumber(2)),
                slot: SlotNumber(20),
            };
            store.put_track(track, info).unwrap();
            store.put_object_info(track, certified).unwrap();
            store.put_slice(spool, track, vec![7u8; 32]).unwrap();
        }

        delete_track_local(&store, first, TAPENET_PROFILE).unwrap();
        sweep_epoch(&store, &config, EpochNumber(6), &owned_spools(&[]), &PendingTracks::new(), true, TAPENET_PROFILE)
            .await
            .unwrap();

        assert!(store.get_slice(spool, first).unwrap().is_none());
        assert!(store.get_slice(spool, second).unwrap().is_some());
    }
}

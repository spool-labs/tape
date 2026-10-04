use tape_store_api::Store;
use tape_api::program::tapedrive::{blacklist_pda, history_pda, TrackAddress, SYSTEM_ADDRESS};
use tape_core::encoding::ErasureCode;
use tape_core::object::object_etag;
use tape_core::snapshot::replay::{ReplayTrack, ReplayableEvent};
use tape_core::spooler::GroupIndex;
use tape_core::tape::{
    blacklist_tape_number, history_tape_number, snapshot_tape_number, tape_index, tape_namespace,
    TapeFlags, TapeNamespace,
};
use tape_core::track::data::BlobData;
use tape_core::track::types::TrackState;
use tape_core::types::{EpochNumber, SlotNumber, TapeNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::ops::{
    ObjectInfoOps, ObjectListOps, ObjectMetadataOps, SliceOps, SpoolOps, TapeOps, TrackDataOps,
    TrackOps,
};
use tape_store::types::{ObjectInfo, ObjectListEntry, ObjectMetadata, SystemObjectKind, TapeInfo};
use tape_store::TapeStore;
use tracing::warn;

use crate::core::error::NodeError;
use crate::features::store::sample::put_sample;
use crate::features::store::cleanup::{
    cleanup_track_slices, delete_tape_local, delete_track_local, remove_object_listing_for_track,
};

const DELETE_TAPE_BATCH_SIZE: usize = 100;

/// A slice missing at certification waits this many slots before the repair path takes it
const REPAIR_GRACE_SLOTS: SlotNumber = SlotNumber(12);

/// Apply a slot's events. `profile` is the network's, which sizes a track's
/// slices and says how many spools its group holds.
pub fn apply_slot<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    block_time: Option<i64>,
    events: &[ReplayableEvent],
    profile: ErasureCode,
) -> Result<(), NodeError> {
    for event in events {
        apply_event(store, slot, block_time, event, profile)?;
    }

    Ok(())
}

pub fn apply_event<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    block_time: Option<i64>,
    event: &ReplayableEvent,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    match event {
        ReplayableEvent::Track(replay) => {
            put_track_object(store, replay, slot, block_time, profile)?;
        }
        ReplayableEvent::CertifyTrack { track, track_number, epoch } => {
            set_certified(store, *track, *track_number, *epoch, slot, profile)?;
        }
        ReplayableEvent::DeleteTrack { track, track_number, .. } => {
            delete_track_local(store, *track, *track_number, slot, profile)?;
        }
        ReplayableEvent::InvalidateTrack { track, track_number, epoch } => {
            invalidate_track(store, *track, *track_number, *epoch, slot, profile)?;
        }
        ReplayableEvent::ReserveTape {
            tape,
            id,
            flags,
            authority,
            expiry_epoch,
            share_nonce,
            ..
        } => {
            store
                .put_tape(
                    *tape,
                    TapeInfo {
                        id: *id,
                        flags: *flags,
                        end_epoch: *expiry_epoch,
                        next_track_number: TrackNumber(0),
                        authority: *authority,
                        delegate: Address::default(),
                        share_nonce: *share_nonce,
                    },
                )
                .map_err(store_error)?;
        }
        // The deployed program can still close an expired tape
        ReplayableEvent::DestroyTape { tape, .. } => {
            delete_tape_local(store, *tape, DELETE_TAPE_BATCH_SIZE, slot, profile)?;
        }
        ReplayableEvent::ExtendTape {
            tape,
            expiry_epoch,
            ..
        } => {
            // The event carries the post-extend expiry; a capacity-only
            // extend leaves it unchanged and needs no write.
            let existing = store.get_tape(*tape).map_err(store_error)?;
            match existing {
                Some(mut info) if info.end_epoch != *expiry_epoch => {
                    info.end_epoch = *expiry_epoch;
                    store.put_tape(*tape, info).map_err(store_error)?;
                }
                Some(_) => {}
                // A lagging node may have swept the tape before applying the
                // extend; there is nothing left to keep alive.
                None => {
                    warn!(tape = %tape, "extend for unknown tape, skipping");
                }
            }
        }
        ReplayableEvent::SetTapeDelegate { tape, delegate } => {
            update_tape(store, *tape, "set delegate", |info| info.delegate = *delegate)?;
        }
        ReplayableEvent::AdvanceShareNonce { tape, nonce } => {
            update_tape(store, *tape, "advance share nonce", |info| info.share_nonce = *nonce)?;
        }
        ReplayableEvent::RegisterNode { node, id, .. } => {
            let (history, _) = history_pda(*node);
            store
                .put_tape(
                    history,
                    TapeInfo {
                        id: history_tape_number(*id),
                        flags: TapeFlags::SYSTEM,
                        end_epoch: EpochNumber(u64::MAX),
                        next_track_number: TrackNumber(0),
                        authority: SYSTEM_ADDRESS,
                        delegate: Address::default(),
                        share_nonce: Hash::default(),
                    },
                )
                .map_err(store_error)?;

            let (blacklist, _) = blacklist_pda(*node);
            store
                .put_tape(
                    blacklist,
                    TapeInfo {
                        id: blacklist_tape_number(*id),
                        flags: TapeFlags::SYSTEM,
                        end_epoch: EpochNumber(u64::MAX),
                        next_track_number: TrackNumber(0),
                        authority: SYSTEM_ADDRESS,
                        delegate: Address::default(),
                        share_nonce: Hash::default(),
                    },
                )
                .map_err(store_error)?;
        }
        ReplayableEvent::SnapshotFinalized {
            epoch,
            snapshot_tape,
            ..
        } => {
            // During bootstrap the snapshot reader may materialize this tape's
            // chunk tracks before this finalize event replays; preserve the
            // existing track cursor so we don't reset it back to zero.
            let next_track_number = store
                .get_tape(*snapshot_tape)
                .map_err(store_error)?
                .map(|tape| tape.next_track_number)
                .unwrap_or(TrackNumber(0));

            store
                .put_tape(
                    *snapshot_tape,
                    TapeInfo {
                        id: snapshot_tape_number(*epoch),
                        flags: TapeFlags::SYSTEM,
                        end_epoch: EpochNumber(u64::MAX),
                        next_track_number,
                        authority: SYSTEM_ADDRESS,
                        delegate: Address::default(),
                        share_nonce: Hash::default(),
                    },
                )
                .map_err(store_error)?;
        }
        ReplayableEvent::AdvanceEpoch { .. }
        | ReplayableEvent::SyncSpool { .. }
        | ReplayableEvent::JoinCommittee { .. }
        | ReplayableEvent::NodeEvicted { .. }
        | ReplayableEvent::AssignmentFinalized { .. }
        | ReplayableEvent::StakeDeposited { .. }
        | ReplayableEvent::StakeUnlockRequested { .. }
        | ReplayableEvent::StakeWithdrawn { .. }
        | ReplayableEvent::VoteProposed { .. }
        | ReplayableEvent::VoteRecorded { .. } => {}
    }

    Ok(())
}

fn put_track_object<Db: Store>(
    store: &TapeStore<Db>,
    replay: &ReplayTrack,
    slot: SlotNumber,
    block_time: Option<i64>,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    validate_replay_track(replay)?;

    let track = replay.state.address();
    let track_number = replay.state.track_number;

    store.put_track(track, replay.state)
        .map_err(store_error)?;

    // The challenge sample set, from the registration rather than from what this
    // node stored. Both kinds are sampled: a coded track by its slice, an inline
    // track as one bounded entry whose payload every owner keeps.
    let data = match replay.blob {
        Some(blob) => BlobData::Coded(blob),
        None => BlobData::Inline(Vec::new()),
    };
    put_sample(
        store,
        replay.state.group,
        track,
        track_number,
        &data,
        replay.state.value_hash,
        slot,
        profile,
    )?;

    // We need to advance the track cursor so that merkle proofs for this tape don't break due to
    // using the wrong index when tracks are deleted.
    advance_track_cursor(
        store,
        replay.state.tape,
        track_number
    )?;

    if let Some(blob) = replay.blob {
        store
            .put_track_data(track, track_number, BlobData::Coded(blob))
            .map_err(store_error)?;
    }

    let certified_epoch = replay.state
        .is_certified()
        .then_some(replay.epoch);

    let tape_info = store
        .get_tape(replay.state.tape)
        .map_err(store_error)?;

    // Replay is ordered: user writes should arrive after ReserveTape, while a
    // missing tape is treated as non-system so the normal validity path can
    // still record the track and surface the inconsistency elsewhere.
    let is_system = tape_info
        .as_ref()
        .is_some_and(|tape| TapeFlags::is_system(tape.flags));

    let object_info = if let Some(tape) = tape_info.filter(|tape| TapeFlags::is_system(tape.flags)) {
        let kind = system_object_kind(tape.id)?;
        let (registered_epoch, certified_epoch) = match &kind {
            SystemObjectKind::Snapshot { epoch } => (*epoch, Some(*epoch)),
            _ => (replay.epoch, certified_epoch),
        };

        ObjectInfo::System {
            kind,
            track_address: track,
            registered_epoch,
            certified_epoch,
            slot,
        }
    } else {
        ObjectInfo::Valid {
            track_address: track,
            registered_epoch: replay.epoch,
            certified_epoch,
            slot,
        }
    };

    store
        .put_object_info(
            track,
            track_number,
            object_info,
        )
        .map_err(store_error)?;

    if !is_system {
        put_named_object_entry(store, replay, track, block_time, slot)?;
    }

    Ok(())
}

fn put_named_object_entry<Db: Store>(
    store: &TapeStore<Db>,
    replay: &ReplayTrack,
    track: Address,
    block_time: Option<i64>,
    slot: SlotNumber,
) -> Result<(), NodeError> {
    let Some(object) = replay.object.as_ref() else {
        return Ok(());
    };

    store
        .put_object_metadata(
            track,
            replay.state.track_number,
            ObjectMetadata {
                name: object.name.clone(),
                content_type: object.content_type,
            },
        )
        .map_err(store_error)?;

    let entry = ObjectListEntry {
        size: object.logical_size,
        etag: object_etag(&replay.state, replay.blob.as_ref()),
        block_time,
        slot,
        data_tape: replay.state.tape,
        track_number: replay.state.track_number,
        track,
        kind: replay.state.kind,
        content_type: object.content_type,
    };

    store
        .put_object_entry(replay.state.tape, &object.name, entry)
        .map_err(store_error)
}

fn system_object_kind(tape_id: TapeNumber) -> Result<SystemObjectKind, NodeError> {
    match tape_namespace(tape_id) {
        Some(TapeNamespace::Snapshot) => Ok(SystemObjectKind::Snapshot {
            epoch: EpochNumber(tape_index(tape_id)),
        }),
        Some(TapeNamespace::History) => Ok(SystemObjectKind::History),
        Some(TapeNamespace::Blacklist) => Ok(SystemObjectKind::Blacklist),
        _ => Err(NodeError::Store(format!(
            "unknown system tape namespace for tape id {}",
            tape_id.0
        ))),
    }
}

fn advance_track_cursor<Db: Store>(
    store: &TapeStore<Db>,
    tape: Address,
    track_number: TrackNumber,
) -> Result<(), NodeError> {
    let info = store.get_tape(tape).map_err(store_error)?;
    let Some(mut tape_info) = info else {
        return Ok(());
    };

    let next_track_number = TrackNumber(
        track_number
            .0
            .checked_add(1)
            .ok_or_else(|| NodeError::Store("track number overflow".into()))?,
    );

    if tape_info.next_track_number < next_track_number {
        tape_info.next_track_number = next_track_number;
        store
            .put_tape(tape, tape_info)
            .map_err(store_error)?;
    }

    Ok(())
}

fn validate_replay_track(replay: &ReplayTrack) -> Result<(), NodeError> {
    match (replay.state.is_coded(), replay.blob) {
        (true, Some(blob)) if replay.state.value_hash == blob.get_hash() => Ok(()),
        (true, Some(_)) => Err(NodeError::Store(
            "replay blob track value_hash does not match blob metadata".into(),
        )),
        (true, None) => Err(NodeError::Store(
            "replay blob track missing blob metadata".into(),
        )),
        (false, Some(_)) => Err(NodeError::Store(
            "replay raw track carried unexpected blob metadata".into(),
        )),
        (false, None) => Ok(()),
    }
}

fn update_tape<Db: Store, Edit: FnOnce(&mut TapeInfo)>(
    store: &TapeStore<Db>,
    tape: Address,
    what: &str,
    edit: Edit,
) -> Result<(), NodeError> {
    let Some(mut info) = store.get_tape(tape).map_err(store_error)? else {
        // A lagging node may have swept the tape already; there is nothing to update.
        warn!(tape = %tape, what, "tape update for unknown tape, skipping");
        return Ok(());
    };

    edit(&mut info);
    store.put_tape(tape, info).map_err(store_error)?;

    Ok(())
}

fn set_certified<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    track_number: TrackNumber,
    epoch: EpochNumber,
    certified_slot: SlotNumber,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    // Both records or neither: object info stamped against a track never stored
    // leaves the pair disagreeing with nothing to repair it
    let Some(mut track_info) = store.get_track(track, track_number).map_err(store_error)? else {
        return Ok(());
    };
    track_info.state = TrackState::Certified as u64;
    store.put_track(track, track_info).map_err(store_error)?;

    let Some(info) = store.get_object_info(track, track_number).map_err(store_error)? else {
        return Ok(());
    };

    if let ObjectInfo::Valid {
        track_address,
        registered_epoch,
        slot,
        ..
    } = info {
        store
            .put_object_info(
                track,
                track_number,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )
            .map_err(store_error)?;

        defer_certified_repairs(store, track, track_info.group, certified_slot, profile)?;
    } else if let ObjectInfo::System {
        kind,
        track_address,
        registered_epoch,
        slot,
        ..
    } = info {
        store
            .put_object_info(
                track,
                track_number,
                ObjectInfo::System {
                    kind,
                    track_address,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )
            .map_err(store_error)?;
    }

    Ok(())
}

/// Queue a repair for every slice of a freshly certified track this node misses, due once the grace is out
fn defer_certified_repairs<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    group: GroupIndex,
    certified_slot: SlotNumber,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    let ready_at = certified_slot.saturating_add(REPAIR_GRACE_SLOTS);

    for slice in 0..usize::from(profile.n()) {
        let spool = group.spool_at(slice);

        let Some(state) = store.get_spool_state(spool).map_err(store_error)? else {
            continue;
        };

        if state.is_locked() {
            continue;
        }

        if store.has_slice(spool, track).map_err(store_error)? {
            continue;
        }

        store
            .defer_pending_repair(spool, track, ready_at)
            .map_err(store_error)?;
    }

    Ok(())
}

fn invalidate_track<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    track_number: TrackNumber,
    epoch: EpochNumber,
    slot: SlotNumber,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    if let Some(mut info) = store.get_track(track, track_number).map_err(store_error)? {
        // Keep object_metadata across invalidation. Delete/GC still use it as
        // the track -> name reverse lookup when the track is eventually removed.
        remove_object_listing_for_track(store, track, &info)?;
        let _ = cleanup_track_slices(store, track, &info, slot, profile)?;
        info.state = TrackState::Invalidated as u64;
        store.put_track(track, info).map_err(store_error)?;
    }

    store.delete_track_data(track, track_number).map_err(store_error)?;

    store
        .put_object_info(track, track_number, ObjectInfo::Invalid { epoch, slot })
        .map_err(store_error)
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::snapshot::replay::{ReplayTrack, ReplayTrackObject, ReplayableEvent};
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        ContentType, EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::hash::hash;
    use tape_crypto::Hash;
    use tape_store::columns::ALL_COLUMN_FAMILIES;
    use tape_store::ops::{
        MetaOps, ObjectInfoOps, ObjectListOps, ObjectMetadataOps, SliceOps, SpoolOps, TapeOps,
        TrackDataOps, TrackOps,
    };
    use tape_store::types::{ObjectInfo, TapeInfo};
    use tape_store::TapeStore;
    use tape_store_api::Store as _;

    use crate::core::error::NodeError;
    use crate::features::spool::manager::has_pending_work;

    use super::{apply_slot, REPAIR_GRACE_SLOTS};

    /// Spools in a tapenet group.
    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    /// Apply a slot's events on a tapenet node.
    fn apply(
        store: &TapeStore<MemoryStore>,
        slot: SlotNumber,
        block_time: Option<i64>,
        events: &[ReplayableEvent],
    ) -> Result<(), NodeError> {
        apply_slot(store, slot, block_time, events, TAPENET_PROFILE)
    }

    /// Copy every row into a fresh store, the way a restart finds what it left behind
    fn reopen(store: &TapeStore<MemoryStore>) -> TapeStore<MemoryStore> {
        let fresh = test_store();

        for column in ALL_COLUMN_FAMILIES {
            let rows = store.inner().inner().iter(column).expect("iter column");
            for (key, value) in rows {
                fresh
                    .inner()
                    .inner()
                    .put(column, &key, &value)
                    .expect("put row");
            }
        }

        fresh
    }

    fn make_blob_track(tape: Address, track_number: TrackNumber, epoch: EpochNumber) -> ReplayableEvent {
        let blob = BlobEncoding {
            size: StorageUnits::mb(2),
            commitment: Hash::new_unique(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(3),
            leaves: [Hash::default(); MAX_GROUP_SIZE],
        };

        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number,
                kind: TrackKind::Coded as u64,
                state: TrackState::Registered as u64,
                flags: TrackFlags::NONE,
                size: blob.size,
                group: GroupIndex::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
            object: None,
        })
    }

    fn make_raw_track(tape: Address, track_number: TrackNumber, epoch: EpochNumber) -> ReplayableEvent {
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number,
                kind: TrackKind::Inline as u64,
                state: TrackState::Certified as u64,
                flags: TrackFlags::NONE,
                size: StorageUnits::from_bytes(4 * 1024),
                group: GroupIndex::from(5),
                value_hash: Hash::new_unique(),
            },
            epoch,
            blob: None,
            object: None,
        })
    }

    fn track_address(event: &ReplayableEvent) -> Address {
        match event {
            ReplayableEvent::Track(replay) => replay.state.address(),
            _ => panic!("expected track event"),
        }
    }

    /// The same content written again at another track number
    fn written_again(event: &ReplayableEvent, track_number: TrackNumber) -> ReplayableEvent {
        match event {
            ReplayableEvent::Track(replay) => {
                let mut copy = replay.clone();
                copy.state.track_number = track_number;
                ReplayableEvent::Track(copy)
            }
            _ => panic!("expected track event"),
        }
    }

    fn delete(track: Address, track_number: TrackNumber) -> ReplayableEvent {
        ReplayableEvent::DeleteTrack {
            track,
            track_number,
            epoch: EpochNumber(8),
        }
    }

    fn certify(track: Address) -> ReplayableEvent {
        ReplayableEvent::CertifyTrack {
            track,
            track_number: TrackNumber(0),
            epoch: EpochNumber(4),
        }
    }

    fn stage_certify(
        store: &TapeStore<MemoryStore>,
        track: Address,
        tape: Address,
        group: GroupIndex,
        slot: SlotNumber,
    ) {
        store
            .put_track(track, track_info(tape, TrackNumber(0), group))
            .expect("put track");
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .expect("put object info");
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

    #[test]
    fn writes_state() {
        let store = test_store();
        let slot = SlotNumber(55);
        let tape = Address::new_unique();
        let authority = Address::new_unique();
        let share_nonce = Hash::new_unique();
        let track_number = TrackNumber(9);
        let track_event = make_blob_track(tape, track_number, EpochNumber(6));
        let track = track_address(&track_event);

        let events = vec![
            ReplayableEvent::ReserveTape {
                tape,
                id: TapeNumber(1),
                flags: 0,
                authority,
                capacity: StorageUnits::mb(10),
                active_epoch: EpochNumber(6),
                expiry_epoch: EpochNumber(12),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
                share_nonce,
            },
            track_event,
            ReplayableEvent::CertifyTrack {
                track,
                track_number,
                epoch: EpochNumber(8),
            },
        ];

        apply(&store, slot, None, &events).unwrap();

        assert_eq!(
            store.get_tape(tape).unwrap(),
            Some(TapeInfo {
                id: TapeNumber(1),
                flags: 0,
                end_epoch: EpochNumber(12),
                next_track_number: TrackNumber(10),
                authority,
                delegate: Address::default(),
                share_nonce,
            })
        );

        let track_info = store.get_track(track, track_number).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.kind, TrackKind::Coded as u64);
        assert_eq!(track_info.state, TrackState::Certified as u64);
        assert_eq!(track_info.size, StorageUnits::mb(2));
        assert_eq!(track_info.group, GroupIndex::from(4));
        match &events[1] {
            ReplayableEvent::Track(replay) => {
                assert_eq!(
                    store.get_track_data(track, track_number).unwrap(),
                    Some(BlobData::Coded(replay.blob.unwrap())),
                );
            }
            _ => panic!("expected track event"),
        }

        assert_eq!(
            store.get_object_info(track, track_number).unwrap(),
            Some(ObjectInfo::Valid {
                track_address: track,
                registered_epoch: EpochNumber(6),
                certified_epoch: Some(EpochNumber(8)),
                slot,
            })
        );
    }

    // named writes materialize into the object listing index
    #[test]
    fn materializes_listing() {
        let store = test_store();
        let slot = SlotNumber(57);
        let block_time = Some(1_700_000_057);
        let tape = Address::new_unique();
        let track_number = TrackNumber(11);
        let name = b"photos/cat.jpg".to_vec();

        let mut track_event = make_blob_track(tape, track_number, EpochNumber(7));
        let blob = match &mut track_event {
            ReplayableEvent::Track(replay) => {
                // Listing hides what a GET would 404 on, so a named object only
                // appears once its track is certified.
                replay.state.state = TrackState::Certified as u64;
                replay.state.key = hash(&name);
                replay.object = Some(ReplayTrackObject {
                    name: name.clone(),
                    content_type: ContentType::ImageJpeg,
                    logical_size: StorageUnits::mb(2),
                });
                replay.blob.expect("blob metadata")
            }
            _ => panic!("expected track event"),
        };
        let track = track_address(&track_event);

        let events = vec![
            ReplayableEvent::ReserveTape {
                tape,
                id: TapeNumber(1),
                flags: 0,
                authority: Address::new_unique(),
                capacity: StorageUnits::mb(10),
                active_epoch: EpochNumber(7),
                expiry_epoch: EpochNumber(12),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
                share_nonce: Hash::default(),
            },
            track_event,
        ];

        apply(&store, slot, block_time, &events).unwrap();

        let page = store
            .list_objects(tape, b"photos/", None, None, 100)
            .unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].0, name);
        assert_eq!(page.objects[0].1.size, blob.size);
        assert_eq!(page.objects[0].1.etag, blob.commitment);
        assert_eq!(page.objects[0].1.block_time, block_time);
        assert_eq!(page.objects[0].1.slot, slot);
        assert_eq!(page.objects[0].1.data_tape, tape);
        assert_eq!(page.objects[0].1.track_number, track_number);
        assert_eq!(page.objects[0].1.track, track);
        assert_eq!(page.objects[0].1.kind, TrackKind::Coded as u64);
        assert_eq!(page.objects[0].1.content_type, ContentType::ImageJpeg);

        let metadata = store.get_object_metadata(track, track_number).unwrap().unwrap();
        assert_eq!(metadata.name, b"photos/cat.jpg".to_vec());
        assert_eq!(metadata.content_type, ContentType::ImageJpeg);

        apply(
            &store,
            SlotNumber(58),
            None,
            &[ReplayableEvent::DeleteTrack {
                track,
                track_number,
                epoch: EpochNumber(8),
            }],
        )
        .unwrap();

        assert!(store.get_object_entry(tape, b"photos/cat.jpg").unwrap().is_none());
        assert!(store.get_object_metadata(track, track_number).unwrap().is_none());
    }

    #[test]
    fn writes_raw_state() {
        let store = test_store();
        let slot = SlotNumber(56);
        let tape = Address::new_unique();
        let track_number = TrackNumber(10);
        let track_event = make_raw_track(tape, track_number, EpochNumber(7));
        let track = track_address(&track_event);

        apply(&store, slot, None, &[track_event]).unwrap();

        let track_info = store.get_track(track, track_number).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.kind, TrackKind::Inline as u64);
        assert_eq!(track_info.state, TrackState::Certified as u64);
        assert_eq!(track_info.size, StorageUnits::from_bytes(4 * 1024));

        assert_eq!(
            store.get_object_info(track, track_number).unwrap(),
            Some(ObjectInfo::Valid {
                track_address: track,
                registered_epoch: EpochNumber(7),
                certified_epoch: Some(EpochNumber(7)),
                slot,
            })
        );
    }

    // two tracks with one address are stored, read and deleted one at a time
    #[test]
    fn shared_address() {
        let store = test_store();
        let tape = Address::new_unique();
        let (first, second) = (TrackNumber(3), TrackNumber(7));
        let written = make_raw_track(tape, first, EpochNumber(7));
        let track = track_address(&written);
        let again = written_again(&written, second);
        assert_eq!(track_address(&again), track);

        apply(&store, SlotNumber(50), None, &[written.clone(), again]).unwrap();

        assert_eq!(store.tracks_by_address(track).unwrap().len(), 2);
        assert_eq!(store.latest_track(track).unwrap().unwrap().track_number, second);
        assert!(store.get_object_info(track, first).unwrap().is_some());
        assert!(store.get_object_info(track, second).unwrap().is_some());

        apply(&store, SlotNumber(51), None, &[delete(track, second)]).unwrap();

        assert_eq!(store.latest_track(track).unwrap().unwrap().track_number, first);
        assert!(store.get_object_info(track, first).unwrap().is_some());
        assert!(store.get_object_info(track, second).unwrap().is_none());

        apply(&store, SlotNumber(52), None, &[delete(track, first)]).unwrap();

        assert!(store.tracks_by_address(track).unwrap().is_empty());

        // The same content written after the deletes is a track of its own.
        let third = TrackNumber(9);
        apply(&store, SlotNumber(53), None, &[written_again(&written, third)]).unwrap();

        assert_eq!(store.latest_track(track).unwrap().unwrap().track_number, third);
        assert!(store.get_object_info(track, third).unwrap().is_some());
        assert!(store.get_track(track, first).unwrap().is_none());
    }

    #[test]
    fn deletes_track() {
        let store = test_store();
        let slot = SlotNumber(21);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(11);

        store.put_track(track, track_info(tape, TrackNumber(0), group)).unwrap();
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();
        for slice_index in 0..WIDTH {
            store
                .put_slice(group.spool_at(slice_index), track, vec![slice_index as u8])
                .unwrap();
        }

        apply(
            &store,
            slot,
            None,
            &[ReplayableEvent::DeleteTrack {
                track,
                track_number: TrackNumber(0),
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();

        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_none());
        assert!(store.get_object_info(track, TrackNumber(0)).unwrap().is_none());
        // The slices outlive the deletion: a round whose window opened before it
        // still asks about the track, and the sweep drops both together.
        for slice_index in 0..WIDTH {
            assert!(
                store
                    .get_slice(group.spool_at(slice_index), track)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn invalidates_track() {
        let store = test_store();
        let slot = SlotNumber(34);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(23);

        store.put_track(track, track_info(tape, TrackNumber(0), group)).unwrap();
        store
            .put_object_info(
                track,
                TrackNumber(0),
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();
        for slice_index in 0..WIDTH {
            store
                .put_slice(group.spool_at(slice_index), track, vec![0xAB; 8])
                .unwrap();
        }

        apply(
            &store,
            SlotNumber(55),
            None,
            &[ReplayableEvent::InvalidateTrack {
                track,
                track_number: TrackNumber(0),
                epoch: EpochNumber(8),
            }],
        )
        .unwrap();

        assert!(store.get_track(track, TrackNumber(0)).unwrap().is_some());
        assert_eq!(
            store.get_object_info(track, TrackNumber(0)).unwrap(),
            Some(ObjectInfo::Invalid {
                epoch: EpochNumber(8),
                slot: SlotNumber(55),
            })
        );
        // The slices outlive the deletion: a round whose window opened before it
        // still asks about the track, and the sweep drops both together.
        for slice_index in 0..WIDTH {
            assert!(
                store
                    .get_slice(group.spool_at(slice_index), track)
                    .unwrap()
                    .is_some()
            );
        }
    }

    // extend updates the expiry while keeping id, flags and track cursor
    #[test]
    fn extends_tape() {
        let store = test_store();
        let tape = Address::new_unique();
        let authority = Address::new_unique();

        let events = vec![
            ReplayableEvent::ReserveTape {
                tape,
                id: TapeNumber(3),
                flags: 0,
                authority,
                capacity: StorageUnits::mb(10),
                active_epoch: EpochNumber(6),
                expiry_epoch: EpochNumber(12),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
                share_nonce: Hash::default(),
            },
            make_blob_track(tape, TrackNumber(4), EpochNumber(6)),
            ReplayableEvent::ExtendTape {
                tape,
                capacity: StorageUnits::mb(20),
                expiry_epoch: EpochNumber(30),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
            },
        ];

        apply(&store, SlotNumber(60), None, &events).expect("apply events");

        assert_eq!(
            store.get_tape(tape).expect("get tape"),
            Some(TapeInfo {
                id: TapeNumber(3),
                flags: 0,
                end_epoch: EpochNumber(30),
                next_track_number: TrackNumber(5),
                authority,
                delegate: Address::default(),
                share_nonce: Hash::default(),
            })
        );
    }

    // extend for a tape this node never stored is skipped without error
    #[test]
    fn extend_unknown_tape() {
        let store = test_store();
        let tape = Address::new_unique();

        let result = apply(
            &store,
            SlotNumber(61),
            None,
            &[ReplayableEvent::ExtendTape {
                tape,
                capacity: StorageUnits::mb(10),
                expiry_epoch: EpochNumber(30),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
            }],
        );

        assert!(result.is_ok());
        assert!(store.get_tape(tape).expect("get tape").is_none());
    }

    // a certify with the slice missing queues a repair the grace holds back
    #[test]
    fn certify_defers_repair() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);
        let spool_id = group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .expect("set spool state");
        stage_certify(&store, track, tape, group, slot);

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");

        assert!(store.has_pending_repair(spool_id, track).expect("has pending repair"));
        assert!(store
            .iter_pending_repairs(spool_id, 10)
            .expect("iter pending repairs")
            .is_empty());
        assert_eq!(
            store.get_spool_state(spool_id).expect("get state").expect("state").status,
            SpoolStatus::Active
        );
    }

    // a slice that lands inside the grace leaves the spool nothing to repair
    #[test]
    fn arrival_clears_work() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);
        let spool_id = group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .expect("set spool state");
        stage_certify(&store, track, tape, group, slot);

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");
        store
            .put_slice(spool_id, track, vec![0xAB; 64])
            .expect("put slice");
        store
            .set_sync_cursor(slot.saturating_add(REPAIR_GRACE_SLOTS), None)
            .expect("set sync cursor");

        assert_eq!(
            has_pending_work(&store, spool_id).expect("pending work"),
            (false, false)
        );
        // The read leaves the row alone; the repair worker drops it.
        assert!(store.has_pending_repair(spool_id, track).expect("has pending repair"));
    }

    // a slice still missing when the grace ends becomes a repair the spool can run
    #[test]
    fn certify_enqueues_repair() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);
        let spool_id = group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .expect("set spool state");
        stage_certify(&store, track, tape, group, slot);

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");
        store
            .set_sync_cursor(slot.saturating_add(REPAIR_GRACE_SLOTS), None)
            .expect("set sync cursor");

        assert_eq!(
            store.iter_pending_repairs(spool_id, 10).expect("iter pending repairs"),
            vec![track]
        );
        assert_eq!(
            has_pending_work(&store, spool_id).expect("pending work"),
            (true, false)
        );
    }

    // a repair deferred at certify outlives the process that queued it
    #[test]
    fn deferred_repair_survives_restart() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);
        let spool_id = group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .expect("set spool state");
        stage_certify(&store, track, tape, group, slot);

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");

        let restarted = reopen(&store);
        restarted
            .set_sync_cursor(slot.saturating_add(REPAIR_GRACE_SLOTS), None)
            .expect("set sync cursor");

        assert_eq!(
            restarted.iter_pending_repairs(spool_id, 10).expect("iter pending repairs"),
            vec![track]
        );
    }

    // a certify for a slice this node already holds queues nothing
    #[test]
    fn certify_noop_when_slice_present() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);
        let spool_id = group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .expect("set spool state");
        stage_certify(&store, track, tape, group, slot);
        store
            .put_slice(spool_id, track, vec![0xAB; 64])
            .expect("put slice");

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");

        assert!(!store.has_pending_repair(spool_id, track).expect("has pending repair"));
        assert_eq!(
            store.get_spool_state(spool_id).expect("get state").expect("state").status,
            SpoolStatus::Active
        );
    }

    // a certify queues nothing when this node holds no spool in the group
    #[test]
    fn certify_noop_when_not_owner() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let group = GroupIndex::from(7);

        stage_certify(&store, track, tape, group, slot);

        apply(&store, slot, None, &[certify(track)]).expect("apply slot");

        for slice in 0..WIDTH {
            let spool = group.spool_at(slice);
            assert!(!store.has_pending_repair(spool, track).expect("has pending repair"));
        }
    }
}

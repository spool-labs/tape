use store::Store;
use tape_api::program::tapedrive::track_pda;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::snapshot::types::{ReplayTrack, ReplayableEvent};
use tape_core::track::types::TrackState;
use tape_core::types::{EpochNumber, SlotNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps};
use tape_store::types::{ObjectInfo, SpoolStatus, TapeInfo, TrackData};
use tape_store::TapeStore;

use crate::core::error::NodeError;
use crate::features::store::cleanup::{cleanup_track_slices, delete_tape_local, delete_track_local};
use crate::features::store::ownership::stores_track_data;

const DELETE_TAPE_BATCH_SIZE: usize = 100;

pub fn apply_slot<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    events: &[ReplayableEvent],
) -> Result<(), NodeError> {
    for event in events {
        apply_event(store, slot, event)?;
    }

    Ok(())
}

pub fn apply_event<Db: Store>(
    store: &TapeStore<Db>,
    slot: SlotNumber,
    event: &ReplayableEvent,
) -> Result<(), NodeError> {
    match event {
        ReplayableEvent::Track(replay) => {
            put_track_object(store, replay, slot)?;
        }
        ReplayableEvent::CertifyTrack { track, epoch } => {
            set_certified(store, *track, *epoch)?;
        }
        ReplayableEvent::DeleteTrack { track, .. } => {
            let _ = delete_track_local(store, *track)?;
        }
        ReplayableEvent::InvalidateTrack { track, epoch } => {
            invalidate_track(store, *track, *epoch, slot)?;
        }
        ReplayableEvent::ReserveTape {
            tape, expiry_epoch, ..
        } => {
            store
                .put_tape(
                    *tape,
                    TapeInfo {
                        end_epoch: *expiry_epoch,
                        next_track_number: TrackNumber(0),
                    },
                )
                .map_err(store_error)?;
        }
        ReplayableEvent::DestroyTape { tape, .. } => {
            let _ = delete_tape_local(store, *tape, DELETE_TAPE_BATCH_SIZE)?;
        }
        ReplayableEvent::AdvanceEpoch { .. }
        | ReplayableEvent::SyncEpoch { .. }
        | ReplayableEvent::RegisterNode { .. }
        | ReplayableEvent::JoinNetwork { .. } => {}
    }

    Ok(())
}

fn put_track_object<Db: Store>(
    store: &TapeStore<Db>,
    replay: &ReplayTrack,
    slot: SlotNumber,
) -> Result<(), NodeError> {
    validate_replay_track(replay)?;

    let (track, _) = track_pda(replay.state.tape, replay.state.track_number);
    let track = Address::from(track);

    store.put_track(track, replay.state).map_err(store_error)?;

    if let Some(mut tape_info) = store
        .get_tape(Address::from(replay.state.tape))
        .map_err(store_error)?
    {
        let next_track_number = TrackNumber(replay.state.track_number.0 + 1);
        if tape_info.next_track_number.0 < next_track_number.0 {
            tape_info.next_track_number = next_track_number;
            store
                .put_tape(Address::from(replay.state.tape), tape_info)
                .map_err(store_error)?;
        }
    }

    if stores_track_data(store, replay.state.spool_group)? {
        if let Some(blob) = replay.blob {
            store
                .put_track_data(track, TrackData::Blob(blob))
                .map_err(store_error)?;
        }
    }

    store
        .put_object_info(
            track,
            ObjectInfo::Valid {
                track_address: track,
                registered_epoch: replay.epoch,
                certified_epoch: None,
                slot,
            },
        )
        .map_err(store_error)
}

fn validate_replay_track(replay: &ReplayTrack) -> Result<(), NodeError> {
    match (replay.state.is_blob(), replay.blob) {
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

fn set_certified<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
    if let Some(mut track_info) = store.get_track(track).map_err(store_error)? {
        track_info.state = TrackState::Certified as u64;
        store.put_track(track, track_info).map_err(store_error)?;
    }

    let Some(info) = store.get_object_info(track).map_err(store_error)? else {
        return Ok(());
    };

    if let ObjectInfo::Valid {
        track_address,
        registered_epoch,
        slot,
        ..
    } = info
    {
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )
            .map_err(store_error)?;

        enqueue_certified_repairs(store, track)?;
    }

    Ok(())
}

fn enqueue_certified_repairs<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
) -> Result<(), NodeError> {
    let Some(track_info) = store.get_track(track).map_err(store_error)? else {
        return Ok(());
    };

    let group = track_info.spool_group;

    for slice in 0..SPOOL_GROUP_SIZE {
        let spool = group.spool_at(slice);

        let Some(mut state) = store.get_spool_state(spool).map_err(store_error)? else {
            continue;
        };

        if state.is_locked() {
            continue;
        }

        if store.has_slice(spool, track).map_err(store_error)? {
            continue;
        }

        store.add_pending_repair(spool, track).map_err(store_error)?;

        // Only transition Active → Repair. Active means no worker is running,
        // so this is safe. For other states, the in-flight worker will see the
        // pending entry via reconcile_terminal when it completes.
        if state.status == SpoolStatus::Active {
            state.set_status(SpoolStatus::Repair);
            store.set_spool_state(spool, state).map_err(store_error)?;
        }
    }

    Ok(())
}

fn invalidate_track<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    epoch: EpochNumber,
    slot: SlotNumber,
) -> Result<(), NodeError> {
    if let Some(mut info) = store.get_track(track).map_err(store_error)? {
        let _ = cleanup_track_slices(store, track, info.spool_group)?;
        info.state = TrackState::Invalidated as u64;
        store.put_track(track, info).map_err(store_error)?;
    }

    store.delete_track_data(track).map_err(store_error)?;

    store
        .put_object_info(track, ObjectInfo::Invalid { epoch, slot })
        .map_err(store_error)
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::snapshot::{ReplayTrack, ReplayableEvent};
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
    use tape_store::types::{ObjectInfo, SpoolState, SpoolStatus, TapeInfo};
    use tape_store::TapeStore;

    use super::apply_slot;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_blob_track(tape: Address, track_number: TrackNumber, epoch: EpochNumber) -> ReplayableEvent {
        let blob = BlobInfo {
            size: StorageUnits::mb(2),
            root: Hash::new_unique(),
            commitment: Hash::new_unique(),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(3),
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
        };

        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number,
                kind: TrackKind::Blob as u64,
                state: TrackState::Registered as u64,
                size: blob.size,
                spool_group: SpoolGroup::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
        })
    }

    fn make_raw_track(tape: Address, track_number: TrackNumber, epoch: EpochNumber) -> ReplayableEvent {
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number,
                kind: TrackKind::Raw as u64,
                state: TrackState::Certified as u64,
                size: StorageUnits::from_bytes(4 * 1024),
                spool_group: SpoolGroup::from(5),
                value_hash: Hash::new_unique(),
            },
            epoch,
            blob: None,
        })
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

    #[test]
    fn writes_state() {
        let store = test_store();
        let slot = SlotNumber(55);
        let tape = Address::new_unique();
        let track_number = TrackNumber(9);
        let (track, _) = track_pda(tape, track_number);

        let events = vec![
            ReplayableEvent::ReserveTape {
                tape,
                authority: Address::new_unique(),
                active_epoch: EpochNumber(6),
                expiry_epoch: EpochNumber(12),
            },
            make_blob_track(tape, track_number, EpochNumber(6)),
            ReplayableEvent::CertifyTrack {
                track,
                epoch: EpochNumber(8),
            },
        ];

        apply_slot(&store, slot, &events).unwrap();

        assert_eq!(
            store.get_tape(tape).unwrap(),
            Some(TapeInfo {
                end_epoch: EpochNumber(12),
                next_track_number: TrackNumber(10),
            })
        );

        let track_info = store.get_track(track).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.kind, TrackKind::Blob as u64);
        assert_eq!(track_info.state, TrackState::Certified as u64);
        assert_eq!(track_info.size, StorageUnits::mb(2));
        assert_eq!(track_info.spool_group, SpoolGroup::from(4));

        assert_eq!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::Valid {
                track_address: track,
                registered_epoch: EpochNumber(6),
                certified_epoch: Some(EpochNumber(8)),
                slot,
            })
        );
    }

    #[test]
    fn writes_raw_state() {
        let store = test_store();
        let slot = SlotNumber(56);
        let tape = Address::new_unique();
        let track_number = TrackNumber(10);
        let (track, _) = track_pda(tape, track_number);

        apply_slot(
            &store,
            slot,
            &[make_raw_track(tape, track_number, EpochNumber(7))],
        )
        .unwrap();

        let track_info = store.get_track(track).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.kind, TrackKind::Raw as u64);
        assert_eq!(track_info.state, TrackState::Certified as u64);
        assert_eq!(track_info.size, StorageUnits::from_bytes(4 * 1024));
    }

    #[test]
    fn deletes_track() {
        let store = test_store();
        let slot = SlotNumber(21);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let spool_group = SpoolGroup::from(11);

        store.put_track(track, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();
        for slice_index in 0..SPOOL_GROUP_SIZE {
            store
                .put_slice(spool_group.spool_at(slice_index), track, vec![slice_index as u8])
                .unwrap();
        }

        apply_slot(
            &store,
            slot,
            &[ReplayableEvent::DeleteTrack {
                track,
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();

        assert!(store.get_track(track).unwrap().is_none());
        assert!(store.get_object_info(track).unwrap().is_none());
        for slice_index in 0..SPOOL_GROUP_SIZE {
            assert!(
                store
                    .get_slice(spool_group.spool_at(slice_index), track)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn invalidates_track() {
        let store = test_store();
        let slot = SlotNumber(34);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let spool_group = SpoolGroup::from(23);

        store.put_track(track, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();
        for slice_index in 0..SPOOL_GROUP_SIZE {
            store
                .put_slice(spool_group.spool_at(slice_index), track, vec![0xAB; 8])
                .unwrap();
        }

        apply_slot(
            &store,
            SlotNumber(55),
            &[ReplayableEvent::InvalidateTrack {
                track,
                epoch: EpochNumber(8),
            }],
        )
        .unwrap();

        assert!(store.get_track(track).unwrap().is_some());
        assert_eq!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::Invalid {
                epoch: EpochNumber(8),
                slot: SlotNumber(55),
            })
        );
        for slice_index in 0..SPOOL_GROUP_SIZE {
            assert!(
                store
                    .get_slice(spool_group.spool_at(slice_index), track)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn destroys_tape() {
        let store = test_store();
        let slot = SlotNumber(13);
        let tape = Address::new_unique();
        let other_tape = Address::new_unique();
        let track_a = Address::new_unique();
        let track_b = Address::new_unique();
        let track_other = Address::new_unique();
        let spool_group = SpoolGroup::from(31);

        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(6),
                    next_track_number: TrackNumber(0),
                },
            )
            .unwrap();
        store
            .put_tape(
                other_tape,
                TapeInfo {
                    end_epoch: EpochNumber(7),
                    next_track_number: TrackNumber(0),
                },
            )
            .unwrap();

        for track in [track_a, track_b] {
            store.put_track(track, track_info(tape, spool_group)).unwrap();
            store
                .put_object_info(
                    track,
                    ObjectInfo::Valid {
                        track_address: track,
                        registered_epoch: EpochNumber(3),
                        certified_epoch: None,
                        slot,
                    },
                )
                .unwrap();
            for slice_index in 0..SPOOL_GROUP_SIZE {
                store
                    .put_slice(spool_group.spool_at(slice_index), track, vec![0xCD; 8])
                    .unwrap();
            }
        }

        store
            .put_track(track_other, track_info(other_tape, spool_group))
            .unwrap();
        store
            .put_object_info(
                track_other,
                ObjectInfo::Valid {
                    track_address: track_other,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();

        apply_slot(
            &store,
            SlotNumber(99),
            &[ReplayableEvent::DestroyTape {
                tape,
                epoch: EpochNumber(9),
            }],
        )
        .unwrap();

        assert!(store.get_tape(tape).unwrap().is_none());
        for track in [track_a, track_b] {
            assert!(store.get_track(track).unwrap().is_none());
            assert!(store.get_object_info(track).unwrap().is_none());
            for slice_index in 0..SPOOL_GROUP_SIZE {
                assert!(
                    store
                        .get_slice(spool_group.spool_at(slice_index), track)
                        .unwrap()
                        .is_none()
                );
            }
        }

        assert!(store.get_tape(other_tape).unwrap().is_some());
        assert!(store.get_track(track_other).unwrap().is_some());
        assert!(store.get_object_info(track_other).unwrap().is_some());
    }

    #[test]
    fn certify_enqueues_repair() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let spool_group = SpoolGroup::from(7);
        let spool_id = spool_group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .unwrap();
        store.put_track(track, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();

        apply_slot(
            &store,
            slot,
            &[ReplayableEvent::CertifyTrack {
                track,
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();

        assert!(store.has_pending_repair(spool_id, track).unwrap());
        let state = store.get_spool_state(spool_id).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Repair);
    }

    #[test]
    fn certify_noop_when_slice_present() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let spool_group = SpoolGroup::from(7);
        let spool_id = spool_group.spool_at(0);

        store
            .set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(3)))
            .unwrap();
        store.put_track(track, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();
        store.put_slice(spool_id, track, vec![0xAB; 64]).unwrap();

        apply_slot(
            &store,
            slot,
            &[ReplayableEvent::CertifyTrack {
                track,
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();

        assert!(!store.has_pending_repair(spool_id, track).unwrap());
        assert_eq!(
            store.get_spool_state(spool_id).unwrap().unwrap().status,
            SpoolStatus::Active
        );
    }

    #[test]
    fn certify_noop_when_not_owner() {
        let store = test_store();
        let slot = SlotNumber(10);
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let spool_group = SpoolGroup::from(7);

        store.put_track(track, track_info(tape, spool_group)).unwrap();
        store
            .put_object_info(
                track,
                ObjectInfo::Valid {
                    track_address: track,
                    registered_epoch: EpochNumber(3),
                    certified_epoch: None,
                    slot,
                },
            )
            .unwrap();

        apply_slot(
            &store,
            slot,
            &[ReplayableEvent::CertifyTrack {
                track,
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();
    }
}

use store::Store;
use tape_api::event::TrackRegistered;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::snapshot::ReplayableEvent;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey, SpoolStatus, TapeInfo, TrackInfo};
use tape_store::TapeStore;

use crate::core::error::NodeError;
use crate::features::store::cleanup::{cleanup_track_slices, delete_tape_local, delete_track_local};

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
        ReplayableEvent::RegisterTrack { track, event_data } => {
            let track_key = Pubkey(*track);
            let event = bytemuck::try_from_bytes::<TrackRegistered>(event_data.as_slice())
                .map_err(|error| NodeError::Store(format!("decode TrackRegistered: {error}")))?;
            put_track_object(store, track_key, event, slot)?;
        }
        ReplayableEvent::CertifyTrack { track, epoch } => {
            set_certified(store, Pubkey(*track), *epoch)?;
        }
        ReplayableEvent::DeleteTrack { track, .. } => {
            delete_track_local(store, Pubkey(*track))?;
        }
        ReplayableEvent::InvalidateTrack { track, epoch } => {
            invalidate_track(store, Pubkey(*track), *epoch, slot)?;
        }
        ReplayableEvent::ReserveTape {
            tape, expiry_epoch, ..
        } => {
            store
                .put_tape(
                    Pubkey(*tape),
                    TapeInfo {
                        end_epoch: *expiry_epoch,
                    },
                )
                .map_err(store_error)?;
        }
        ReplayableEvent::DestroyTape { tape, .. } => {
            delete_tape_local(store, Pubkey(*tape), DELETE_TAPE_BATCH_SIZE)?;
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
    track: Pubkey,
    event: &TrackRegistered,
    slot: SlotNumber,
) -> Result<(), NodeError> {
    let mut info = TrackInfo {
        tape_address: event.tape.into(),
        spool_group: SpoolGroup::unpack(event.spool_group),
        original_size: event.size.0,
        stripe_size: u64::from_le_bytes(event.stripe_size),
        stripe_count: u64::from_le_bytes(event.stripe_count),
        encoding_type: 0,
        encoding_params: 0,
        commitment: event.leaves.to_vec(),
    };

    info.set_profile(event.profile);

    store.put_track(track, info).map_err(store_error)?;
    store
        .put_object_info(
            track,
            ObjectInfo::Valid {
                track_address: track,
                registered_epoch: event.epoch,
                certified_epoch: None,
                slot,
            },
        )
        .map_err(store_error)
}

fn set_certified<Db: Store>(
    store: &TapeStore<Db>,
    track: Pubkey,
    epoch: EpochNumber,
) -> Result<(), NodeError> {
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
    track: Pubkey,
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
    track: Pubkey,
    epoch: EpochNumber,
    slot: SlotNumber,
) -> Result<(), NodeError> {
    if let Some(info) = store.get_track(track).map_err(store_error)? {
        cleanup_track_slices(store, track, info.spool_group)?;
    }

    store
        .put_object_info(track, ObjectInfo::Invalid { epoch, slot })
        .map_err(store_error)
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use bytemuck::bytes_of;
    use solana_sdk::pubkey::Pubkey as SolanaPubkey;
    use store_memory::MemoryStore;
    use tape_api::event::TrackRegistered;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::spooler::SpoolGroup;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, SliceOps, SpoolOps, TapeOps, TrackOps};
    use tape_store::types::{ObjectInfo, Pubkey, SpoolState, SpoolStatus, TapeInfo, TrackInfo};
    use tape_store::TapeStore;

    use super::apply_slot;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn make_register_track(track: SolanaPubkey, tape: SolanaPubkey, epoch: EpochNumber) -> ReplayableEvent {
        let event = TrackRegistered {
            track,
            tape,
            key: Hash::new_unique(),
            size: StorageUnits::mb(2),
            commitment: Hash::new_unique(),
            epoch,
            profile: EncodingProfile::default(),
            spool_group: 4u64.to_le_bytes(),
            stripe_size: 128u64.to_le_bytes(),
            stripe_count: 3u64.to_le_bytes(),
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
        };

        ReplayableEvent::RegisterTrack {
            track: track.to_bytes(),
            event_data: bytes_of(&event).to_vec(),
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

    #[test]
    fn writes_state() {
        let store = test_store();
        let slot = SlotNumber(55);
        let track = SolanaPubkey::new_unique();
        let tape = SolanaPubkey::new_unique();

        let events = vec![
            ReplayableEvent::ReserveTape {
                tape: tape.to_bytes(),
                authority: SolanaPubkey::new_unique().to_bytes(),
                active_epoch: EpochNumber(6),
                expiry_epoch: EpochNumber(12),
            },
            make_register_track(track, tape, EpochNumber(6)),
            ReplayableEvent::CertifyTrack {
                track: track.to_bytes(),
                epoch: EpochNumber(8),
            },
        ];

        apply_slot(&store, slot, &events).unwrap();

        assert_eq!(
            store.get_tape(Pubkey::from(tape)).unwrap(),
            Some(TapeInfo {
                end_epoch: EpochNumber(12),
            })
        );

        let track_info = store.get_track(Pubkey::from(track)).unwrap().unwrap();
        assert_eq!(track_info.tape_address, Pubkey::from(tape));
        assert_eq!(track_info.original_size, StorageUnits::mb(2).0);
        assert_eq!(track_info.stripe_size, 128);
        assert_eq!(track_info.stripe_count, 3);

        assert_eq!(
            store.get_object_info(Pubkey::from(track)).unwrap(),
            Some(ObjectInfo::Valid {
                track_address: Pubkey::from(track),
                registered_epoch: EpochNumber(6),
                certified_epoch: Some(EpochNumber(8)),
                slot,
            })
        );
    }

    #[test]
    fn deletes_track() {
        let store = test_store();
        let slot = SlotNumber(21);
        let track = Pubkey::from(SolanaPubkey::new_unique());
        let tape = Pubkey::from(SolanaPubkey::new_unique());
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
                track: track.0,
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
        let track = Pubkey::from(SolanaPubkey::new_unique());
        let tape = Pubkey::from(SolanaPubkey::new_unique());
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
                track: track.0,
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
        let tape = Pubkey::from(SolanaPubkey::new_unique());
        let other_tape = Pubkey::from(SolanaPubkey::new_unique());
        let track_a = Pubkey::from(SolanaPubkey::new_unique());
        let track_b = Pubkey::from(SolanaPubkey::new_unique());
        let track_other = Pubkey::from(SolanaPubkey::new_unique());
        let spool_group = SpoolGroup::from(31);

        store
            .put_tape(
                tape,
                TapeInfo {
                    end_epoch: EpochNumber(6),
                },
            )
            .unwrap();
        store
            .put_tape(
                other_tape,
                TapeInfo {
                    end_epoch: EpochNumber(7),
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
                tape: tape.0,
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
        let track = Pubkey::from(SolanaPubkey::new_unique());
        let tape = Pubkey::from(SolanaPubkey::new_unique());
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
                track: track.0,
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
        let track = Pubkey::from(SolanaPubkey::new_unique());
        let tape = Pubkey::from(SolanaPubkey::new_unique());
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
                track: track.0,
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
        let track = Pubkey::from(SolanaPubkey::new_unique());
        let tape = Pubkey::from(SolanaPubkey::new_unique());
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
                track: track.0,
                epoch: EpochNumber(4),
            }],
        )
        .unwrap();
    }
}

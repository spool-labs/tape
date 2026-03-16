use store::Store;
use tape_api::event::TrackRegistered;
use tape_core::snapshot::ReplayableEvent;
use tape_core::spooler::SpoolGroup;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey, TapeInfo, TrackInfo};
use tape_store::TapeStore;

use crate::core::error::NodeError;
use crate::features::state::cleanup::{cleanup_track_slices, delete_tape_local, delete_track_local};

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
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
    use tape_store::types::{ObjectInfo, Pubkey, TapeInfo};
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

    #[test]
    fn apply_slot_writes_track_tape_and_object_state() {
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
}

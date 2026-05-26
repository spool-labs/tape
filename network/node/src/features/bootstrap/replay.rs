//! Apply a decoded `SnapshotLog` through the same `apply_event` path the
//! live block ingestor uses.
//!
//! Bootstrap runs pre-supervisor with no other writer touching the store, so
//! this is a plain synchronous iteration, no channels, no locks, no task
//! spawning. Events within a slot apply in their recorded order; slots apply
//! in the order the log was built (which mirrors block processing order).

use tape_core::snapshot::replay::SnapshotLog;
use store::Store;
use tape_store::TapeStore;

use crate::core::error::NodeError;
use crate::features::replay::engine::ReplayEngine;

/// Replay every event in the snapshot log in (slot-then-position) order.
pub fn apply_snapshot_log<Db: Store>(
    store: &TapeStore<Db>,
    log: &SnapshotLog,
) -> Result<(), NodeError> {
    ReplayEngine::new(store, log.epoch)
        .apply_snapshot_log(log)
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::snapshot::replay::{
        ReplayTrack, ReplayableEvent, SnapshotEntry, SnapshotLog,
    };
    use tape_core::spooler::GroupIndex;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_store::ops::{ObjectInfoOps, TapeOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};
    use tape_store::TapeStore;

    use super::apply_snapshot_log;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn blob() -> BlobInfo {
        BlobInfo {
            size: StorageUnits::mb(1),
            commitment: Hash::new_unique(),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(3),
            leaves: [Hash::default(); GROUP_SIZE],
        }
    }

    fn track_event(
        tape: Address,
        track_number: TrackNumber,
        epoch: EpochNumber,
    ) -> ReplayableEvent {
        let blob = blob();
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: Hash::new_unique(),
                track_number,
                kind: TrackKind::Blob as u64,
                state: TrackState::Registered as u64,
                size: blob.size,
                group: GroupIndex::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
        })
    }

    fn log_with_entries(
        epoch: EpochNumber,
        start: SlotNumber,
        end: SlotNumber,
        entries: Vec<SnapshotEntry>,
    ) -> SnapshotLog {
        SnapshotLog {
            epoch,
            start_slot: start,
            end_slot: end,
            entries,
        }
    }

    #[test]
    fn applies_reserve_tape_then_track() {
        let store = test_store();
        let tape = Address::new_unique();
        let track_number = TrackNumber(3);
        let (track, _) = track_pda(tape, track_number);

        let log = log_with_entries(
            EpochNumber(6),
            SlotNumber(100),
            SlotNumber(100),
            vec![SnapshotEntry {
                slot: SlotNumber(100),
                events: vec![
                    ReplayableEvent::ReserveTape {
                        tape,
                        id: TapeNumber(1),
                        flags: 0,
                        authority: Address::new_unique(),
                        active_epoch: EpochNumber(6),
                        expiry_epoch: EpochNumber(12),
                    },
                    track_event(tape, track_number, EpochNumber(6)),
                ],
            }],
        );

        apply_snapshot_log(&store, &log).unwrap();

        assert_eq!(
            store.get_tape(tape).unwrap(),
            Some(TapeInfo {
                id: TapeNumber(1),
                flags: 0,
                end_epoch: EpochNumber(12),
                next_track_number: TrackNumber(4),
            })
        );

        let track_info = store.get_track(track).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.group, GroupIndex::from(4));

        assert!(matches!(
            store.get_object_info(track).unwrap(),
            Some(ObjectInfo::Valid { .. })
        ));
    }

    #[test]
    fn applies_events_across_multiple_slots_in_order() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track_a, _) = track_pda(tape, TrackNumber(0));
        let (track_b, _) = track_pda(tape, TrackNumber(1));

        let log = log_with_entries(
            EpochNumber(7),
            SlotNumber(100),
            SlotNumber(101),
            vec![
                SnapshotEntry {
                    slot: SlotNumber(100),
                    events: vec![
                        ReplayableEvent::ReserveTape {
                            tape,
                            id: TapeNumber(2),
                            flags: 0,
                            authority: Address::new_unique(),
                            active_epoch: EpochNumber(7),
                            expiry_epoch: EpochNumber(13),
                        },
                        track_event(tape, TrackNumber(0), EpochNumber(7)),
                    ],
                },
                SnapshotEntry {
                    slot: SlotNumber(101),
                    events: vec![
                        track_event(tape, TrackNumber(1), EpochNumber(7)),
                        ReplayableEvent::CertifyTrack {
                            track: track_a,
                            epoch: EpochNumber(8),
                        },
                    ],
                },
            ],
        );

        apply_snapshot_log(&store, &log).unwrap();

        // Both tracks present; tape counter advanced past the highest.
        assert!(store.get_track(track_a).unwrap().is_some());
        assert!(store.get_track(track_b).unwrap().is_some());
        assert_eq!(
            store.get_tape(tape).unwrap().unwrap().next_track_number,
            TrackNumber(2)
        );

        // CertifyTrack was applied at slot 101 — track_a state flipped.
        let info_a = store.get_track(track_a).unwrap().unwrap();
        assert_eq!(info_a.state, TrackState::Certified as u64);
    }

    #[test]
    fn replay_is_idempotent() {
        let store = test_store();
        let tape = Address::new_unique();
        let (track, _) = track_pda(tape, TrackNumber(5));

        let log = log_with_entries(
            EpochNumber(4),
            SlotNumber(50),
            SlotNumber(50),
            vec![SnapshotEntry {
                slot: SlotNumber(50),
                events: vec![
                    ReplayableEvent::ReserveTape {
                        tape,
                        id: TapeNumber(3),
                        flags: 0,
                        authority: Address::new_unique(),
                        active_epoch: EpochNumber(4),
                        expiry_epoch: EpochNumber(9),
                    },
                    track_event(tape, TrackNumber(5), EpochNumber(4)),
                    ReplayableEvent::CertifyTrack {
                        track,
                        epoch: EpochNumber(5),
                    },
                ],
            }],
        );

        apply_snapshot_log(&store, &log).unwrap();
        let snapshot_a = (
            store.get_tape(tape).unwrap(),
            store.get_track(track).unwrap(),
            store.get_object_info(track).unwrap(),
        );

        // Apply the same log again. Non-destructive events must converge on
        // identical state.
        apply_snapshot_log(&store, &log).unwrap();
        let snapshot_b = (
            store.get_tape(tape).unwrap(),
            store.get_track(track).unwrap(),
            store.get_object_info(track).unwrap(),
        );

        assert_eq!(snapshot_a, snapshot_b);
    }

    #[test]
    fn empty_log_is_noop() {
        let store = test_store();
        let log = log_with_entries(
            EpochNumber(2),
            SlotNumber(0),
            SlotNumber(0),
            Vec::new(),
        );
        apply_snapshot_log(&store, &log).unwrap();
        // Nothing to assert — just confirm no error.
    }
}

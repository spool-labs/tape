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
    use std::collections::BTreeMap;

    use store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::snapshot::replay::{
        ReplayRecord, ReplayTrack, ReplayableEvent, SnapshotEntry, SnapshotLog,
    };
    use tape_core::spooler::GroupIndex;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::{track_key, BlobDataSlice};
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        ChunkNumber, ContentType, EpochNumber, SlotNumber, StorageUnits, StripeCount,
        TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::tx::Txid;
    use tape_crypto::Hash;
    use tape_snapshot::{assemble_snapshot_log, decode_chunk_payload, encode_snapshot, K_INNER};
    use tape_store::ops::{
        ObjectInfoOps, ObjectListOps, ObjectMetadataOps, TapeOps, TrackOps,
    };
    use tape_store::types::{ObjectInfo, TapeInfo};
    use tape_store::TapeStore;

    use super::apply_snapshot_log;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn blob() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::mb(1),
            commitment: Hash::new_unique(),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(3),
            leaves: [Hash::default(); GROUP_SIZE],
        }
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
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
                key: track_key(b"", &BlobDataSlice::Coded(blob)),
                track_number,
                kind: TrackKind::Coded as u64,
                state: TrackState::Registered as u64,
                size: blob.size,
                group: GroupIndex::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
            name: None,
            content_type: ContentType::Unknown,
        })
    }

    fn named_track_event(
        tape: Address,
        track_number: TrackNumber,
        epoch: EpochNumber,
        blob: BlobEncoding,
        name: Vec<u8>,
        content_type: ContentType,
    ) -> ReplayableEvent {
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: track_key(&name, &BlobDataSlice::Coded(blob)),
                track_number,
                kind: TrackKind::Coded as u64,
                state: TrackState::Registered as u64,
                size: blob.size,
                group: GroupIndex::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
            name: Some(name),
            content_type,
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

    fn decode_encoded_log(snapshot_tape: Address, log: &SnapshotLog) -> SnapshotLog {
        const TOTAL_GROUPS: usize = 3;

        let chunks = encode_snapshot(snapshot_tape, log.epoch, log, TOTAL_GROUPS).unwrap();
        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> =
            BTreeMap::new();
        for chunk in &chunks {
            let inner: Vec<(usize, &[u8])> = chunk
                .slices
                .iter()
                .enumerate()
                .take(K_INNER)
                .map(|(index, slice)| (index, slice.as_slice()))
                .collect();
            let (chunk_number, symbol) = decode_chunk_payload(&inner).unwrap();
            symbols_by_segment
                .entry(chunk_number)
                .or_default()
                .push((chunk.group.0 as usize, symbol));
        }

        assemble_snapshot_log(&symbols_by_segment, log.epoch, TOTAL_GROUPS).unwrap()
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
                block_time: Some(1_700_000_000),
                records: vec![
                    record(ReplayableEvent::ReserveTape {
                        tape,
                        id: TapeNumber(1),
                        flags: 0,
                        authority: Address::new_unique(),
                        capacity: StorageUnits::mb(10),
                        active_epoch: EpochNumber(6),
                        expiry_epoch: EpochNumber(12),
                        cost: TAPE(0),
                        burned: TAPE(0),
                        scheduled: TAPE(0),
                    }),
                    record(track_event(tape, track_number, EpochNumber(6))),
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
                    block_time: Some(1_700_000_000),
                    records: vec![
                        record(ReplayableEvent::ReserveTape {
                            tape,
                            id: TapeNumber(2),
                            flags: 0,
                            authority: Address::new_unique(),
                            capacity: StorageUnits::mb(10),
                            active_epoch: EpochNumber(7),
                            expiry_epoch: EpochNumber(13),
                            cost: TAPE(0),
                            burned: TAPE(0),
                            scheduled: TAPE(0),
                        }),
                        record(track_event(tape, TrackNumber(0), EpochNumber(7))),
                    ],
                },
                SnapshotEntry {
                    slot: SlotNumber(101),
                    block_time: Some(1_700_000_050),
                    records: vec![
                        record(track_event(tape, TrackNumber(1), EpochNumber(7))),
                        record(ReplayableEvent::CertifyTrack {
                            track: track_a,
                            epoch: EpochNumber(8),
                        }),
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
                block_time: Some(1_700_000_000),
                records: vec![
                    record(ReplayableEvent::ReserveTape {
                        tape,
                        id: TapeNumber(3),
                        flags: 0,
                        authority: Address::new_unique(),
                        capacity: StorageUnits::mb(10),
                        active_epoch: EpochNumber(4),
                        expiry_epoch: EpochNumber(9),
                        cost: TAPE(0),
                        burned: TAPE(0),
                        scheduled: TAPE(0),
                    }),
                    record(track_event(tape, TrackNumber(5), EpochNumber(4))),
                    record(ReplayableEvent::CertifyTrack {
                        track,
                        epoch: EpochNumber(5),
                    }),
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
    fn named_object_roundtrip() {
        let epoch = EpochNumber(9);
        let slot = SlotNumber(120);
        let block_time = Some(1_700_000_120);
        let tape = Address::new_unique();
        let track_number = TrackNumber(7);
        let track = track_pda(tape, track_number).0;
        let blob = blob();
        let name = b"photos/cat.jpg".to_vec();
        let content_type = ContentType::ImageJpeg;

        let log = log_with_entries(
            epoch,
            slot,
            slot,
            vec![SnapshotEntry {
                slot,
                block_time,
                records: vec![
                    record(ReplayableEvent::ReserveTape {
                        tape,
                        id: TapeNumber(4),
                        flags: 0,
                        authority: Address::new_unique(),
                        capacity: StorageUnits::mb(10),
                        active_epoch: epoch,
                        expiry_epoch: EpochNumber(20),
                        cost: TAPE(0),
                        burned: TAPE(0),
                        scheduled: TAPE(0),
                    }),
                    record(named_track_event(
                        tape,
                        track_number,
                        epoch,
                        blob,
                        name.clone(),
                        content_type,
                    )),
                ],
            }],
        );

        let live_store = test_store();
        apply_snapshot_log(&live_store, &log).unwrap();

        let decoded = decode_encoded_log(Address::new_unique(), &log);
        let snapshot_store = test_store();
        apply_snapshot_log(&snapshot_store, &decoded).unwrap();

        assert_eq!(
            snapshot_store.get_object_metadata(track).unwrap(),
            live_store.get_object_metadata(track).unwrap()
        );

        let live_entry = live_store.get_object_entry(tape, &name).unwrap().unwrap();
        let snapshot_entry = snapshot_store.get_object_entry(tape, &name).unwrap().unwrap();
        assert_eq!(snapshot_entry, live_entry);
        assert_eq!(snapshot_entry.etag, blob.commitment);
        assert_eq!(snapshot_entry.content_type, content_type);
        assert_eq!(snapshot_entry.block_time, block_time);
        assert_eq!(snapshot_entry.slot, slot);

        let page = snapshot_store
            .list_objects(tape, b"photos/", None, None, 10)
            .unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].0, name);
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

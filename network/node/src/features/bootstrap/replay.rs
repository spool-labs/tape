//! Apply a decoded `SnapshotLog` through the same `apply_event` path the
//! live block ingestor uses.
//!
//! Bootstrap runs pre-supervisor with no other writer touching the store, so
//! this is a plain synchronous iteration, no channels, no locks, no task
//! spawning. Events within a slot apply in their recorded order; slots apply
//! in the order the log was built (which mirrors block processing order).

use tape_core::encoding::ErasureCode;
use tape_core::snapshot::replay::SnapshotLog;
use tape_store_api::Store;
use tape_store::TapeStore;

use crate::core::error::NodeError;
use crate::features::replay::engine::ReplayEngine;

/// Replay every event in the snapshot log in (slot-then-position) order.
pub fn apply_snapshot_log<Db: Store>(
    store: &TapeStore<Db>,
    log: &SnapshotLog,
    profile: ErasureCode,
) -> Result<(), NodeError> {
    ReplayEngine::new(store, log.epoch, profile)
        .apply_snapshot_log(log)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::snapshot::replay::{
        ReplayRecord, ReplayTrack, ReplayTrackObject, ReplayableEvent, SnapshotEntry, SnapshotLog,
    };
    use tape_core::spooler::GroupIndex;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::{track_key, BlobData, BlobDataSlice, TrackMeta};
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        ChunkNumber, ContentType, EpochNumber, SlotNumber, StorageUnits, StripeCount,
        TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::tx::Txid;
    use tape_crypto::Hash;
    use tape_snapshot::{assemble_snapshot_log, decode_chunk_payload, encode_snapshot};
    use tape_store::ops::{
        ObjectInfoOps, ObjectListOps, ObjectMetadataOps, TapeOps, TrackDataOps, TrackOps,
    };
    use tape_store::types::{ObjectInfo, TapeInfo};
    use tape_store::TapeStore;

    use crate::core::error::NodeError;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    /// Replay a log on a tapenet node.
    fn apply_snapshot_log(
        store: &TapeStore<MemoryStore>,
        log: &SnapshotLog,
    ) -> Result<(), NodeError> {
        super::apply_snapshot_log(store, log, TAPENET_PROFILE)
    }

    fn blob() -> BlobEncoding {
        BlobEncoding {
            size: StorageUnits::mb(1),
            commitment: Hash::new_unique(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(3),
            leaves: [Hash::default(); MAX_GROUP_SIZE],
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
                // Certified: the listing scan hides objects a GET would 404 on.
                state: TrackState::Certified as u64,
                flags: TrackFlags::NONE,
                size: blob.size,
                group: GroupIndex::from(4),
                value_hash: blob.get_hash(),
            },
            epoch,
            blob: Some(blob),
            object: Some(ReplayTrackObject {
                name,
                content_type,
                logical_size: blob.size,
            }),
        })
    }

    fn inline_track_event(
        tape: Address,
        track_number: TrackNumber,
        epoch: EpochNumber,
        name: Vec<u8>,
        content_type: ContentType,
        bytes: &[u8],
    ) -> ReplayableEvent {
        let data = BlobDataSlice::Inline(bytes);
        let meta = TrackMeta::inline(bytes);
        ReplayableEvent::Track(ReplayTrack {
            state: CompressedTrack {
                tape,
                key: track_key(&name, &data),
                track_number,
                kind: meta.kind as u64,
                state: meta.state as u64,
                flags: TrackFlags::NONE,
                size: meta.size,
                group: GroupIndex::from(4),
                value_hash: meta.value_hash,
            },
            epoch,
            blob: None,
            object: Some(ReplayTrackObject {
                name,
                content_type,
                logical_size: meta.size,
            }),
        })
    }

    fn track_address(event: &ReplayableEvent) -> Address {
        match event {
            ReplayableEvent::Track(replay) => replay.state.address(),
            _ => panic!("expected track event"),
        }
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

        let chunks =
            encode_snapshot(snapshot_tape, log.epoch, log, TOTAL_GROUPS, TAPENET_PROFILE).unwrap();
        let mut symbols_by_segment: BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>> =
            BTreeMap::new();
        for chunk in &chunks {
            let inner: Vec<(usize, &[u8])> = chunk
                .slices
                .iter()
                .enumerate()
                .take(usize::from(TAPENET_PROFILE.k()))
                .map(|(index, slice)| (index, slice.as_slice()))
                .collect();
            let (chunk_number, symbol) = decode_chunk_payload(&inner, TAPENET_PROFILE).unwrap();
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
        let authority = Address::new_unique();
        let track_number = TrackNumber(3);
        let written = track_event(tape, track_number, EpochNumber(6));
        let track = track_address(&written);

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
                        authority,
                        capacity: StorageUnits::mb(10),
                        active_epoch: EpochNumber(6),
                        expiry_epoch: EpochNumber(12),
                        cost: TAPE(0),
                        burned: TAPE(0),
                        scheduled: TAPE(0),
                        share_nonce: Hash::default(),
                    }),
                    record(written),
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
                authority,
                delegate: Address::default(),
                share_nonce: Hash::default(),
            })
        );

        let track_info = store.get_track(track, track_number).unwrap().unwrap();
        assert_eq!(track_info.tape, tape);
        assert_eq!(track_info.track_number, track_number);
        assert_eq!(track_info.group, GroupIndex::from(4));

        assert!(matches!(
            store.get_object_info(track, track_number).unwrap(),
            Some(ObjectInfo::Valid { .. })
        ));
    }

    #[test]
    fn applies_events_across_multiple_slots_in_order() {
        let store = test_store();
        let tape = Address::new_unique();
        let written_a = track_event(tape, TrackNumber(0), EpochNumber(7));
        let written_b = track_event(tape, TrackNumber(1), EpochNumber(7));
        let track_a = track_address(&written_a);
        let track_b = track_address(&written_b);

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
                            share_nonce: Hash::default(),
                        }),
                        record(written_a),
                    ],
                },
                SnapshotEntry {
                    slot: SlotNumber(101),
                    block_time: Some(1_700_000_050),
                    records: vec![
                        record(written_b),
                        record(ReplayableEvent::CertifyTrack {
                            track: track_a,
                            track_number: TrackNumber(0),
                            epoch: EpochNumber(8),
                        }),
                    ],
                },
            ],
        );

        apply_snapshot_log(&store, &log).unwrap();

        // Both tracks present; tape counter advanced past the highest.
        assert!(store.get_track(track_a, TrackNumber(0)).unwrap().is_some());
        assert!(store.get_track(track_b, TrackNumber(1)).unwrap().is_some());
        assert_eq!(
            store.get_tape(tape).unwrap().unwrap().next_track_number,
            TrackNumber(2)
        );

        // CertifyTrack was applied at slot 101 — track_a state flipped.
        let info_a = store.get_track(track_a, TrackNumber(0)).unwrap().unwrap();
        assert_eq!(info_a.state, TrackState::Certified as u64);
    }

    #[test]
    fn replay_is_idempotent() {
        let store = test_store();
        let tape = Address::new_unique();
        let written = track_event(tape, TrackNumber(5), EpochNumber(4));
        let track = track_address(&written);

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
                        share_nonce: Hash::default(),
                    }),
                    record(written),
                    record(ReplayableEvent::CertifyTrack {
                        track,
                        track_number: TrackNumber(5),
                        epoch: EpochNumber(5),
                    }),
                ],
            }],
        );

        apply_snapshot_log(&store, &log).unwrap();
        let snapshot_a = (
            store.get_tape(tape).unwrap(),
            store.get_track(track, TrackNumber(5)).unwrap(),
            store.get_object_info(track, TrackNumber(5)).unwrap(),
        );

        // Apply the same log again. Non-destructive events must converge on
        // identical state.
        apply_snapshot_log(&store, &log).unwrap();
        let snapshot_b = (
            store.get_tape(tape).unwrap(),
            store.get_track(track, TrackNumber(5)).unwrap(),
            store.get_object_info(track, TrackNumber(5)).unwrap(),
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
        let blob = blob();
        let name = b"photos/cat.jpg".to_vec();
        let content_type = ContentType::ImageJpeg;
        let written =
            named_track_event(tape, track_number, epoch, blob, name.clone(), content_type);
        let track = track_address(&written);

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
                        share_nonce: Hash::default(),
                    }),
                    record(written),
                ],
            }],
        );

        let live_store = test_store();
        apply_snapshot_log(&live_store, &log).unwrap();

        let decoded = decode_encoded_log(Address::new_unique(), &log);
        let snapshot_store = test_store();
        apply_snapshot_log(&snapshot_store, &decoded).unwrap();

        assert_eq!(
            snapshot_store.get_object_metadata(track, track_number).unwrap(),
            live_store.get_object_metadata(track, track_number).unwrap()
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

    // a snapshot round-trip keeps an inline track's record and object entry
    // while its bytes stay out of the snapshot for the sync flow to backfill
    #[test]
    fn inline_record_roundtrip() {
        let epoch = EpochNumber(9);
        let slot = SlotNumber(120);
        let tape = Address::new_unique();

        let inline_number = TrackNumber(7);
        let inline_name = b"loadgen/obj-42".to_vec();
        let inline_bytes = vec![0xAB; 51];
        let inline_event = inline_track_event(
            tape,
            inline_number,
            epoch,
            inline_name.clone(),
            ContentType::TextPlain,
            &inline_bytes,
        );
        let inline_track = track_address(&inline_event);

        let sliced_number = TrackNumber(8);
        let sliced_name = b"loadgen/obj-41".to_vec();
        let sliced_blob = blob();
        let sliced_event = named_track_event(
            tape,
            sliced_number,
            epoch,
            sliced_blob,
            sliced_name.clone(),
            ContentType::Unknown,
        );
        let sliced_track = track_address(&sliced_event);

        let log = log_with_entries(
            epoch,
            slot,
            slot,
            vec![SnapshotEntry {
                slot,
                block_time: Some(1_700_000_120),
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
                        share_nonce: Hash::default(),
                    }),
                    record(inline_event),
                    record(sliced_event),
                ],
            }],
        );

        let decoded = decode_encoded_log(Address::new_unique(), &log);
        let store = test_store();
        apply_snapshot_log(&store, &decoded).unwrap();

        let inline_info = store.get_track(inline_track, inline_number).unwrap().unwrap();
        assert_eq!(inline_info.kind, TrackKind::Inline as u64);
        assert!(store.get_object_entry(tape, &inline_name).unwrap().is_some());

        // content is deliberately absent, only the record rides the snapshot
        assert_eq!(store.get_track_data(inline_track, inline_number).unwrap(), None);

        // the sliced neighbor keeps its record and blob metadata, as a control
        assert!(store.get_track(sliced_track, sliced_number).unwrap().is_some());
        assert!(matches!(
            store.get_track_data(sliced_track, sliced_number).unwrap(),
            Some(BlobData::Coded(_))
        ));
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

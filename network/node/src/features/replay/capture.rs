use tape_api::program::tapedrive::{snapshot_tape_pda, SYSTEM_ADDRESS};
use tape_blocks::{ParseError, ParsedInstruction};
use tape_core::snapshot::chunk::snapshot_chunk_key;
use tape_core::snapshot::replay::{ReplayTrack, ReplayableEvent};
use tape_core::track::data::TrackData;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{EpochNumber, SlotNumber};

use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::{RawTrack, ReplayBatch};

pub struct CapturedEvent {
    pub epoch: EpochNumber,
    pub event: ReplayableEvent,
}

struct CapturedTrackWrite {
    event: CapturedEvent,
    raw_track: Option<RawTrack>,
}

pub struct CaptureOutput {
    pub next_epoch: EpochNumber,
    pub events: Vec<CapturedEvent>,
    pub raw_tracks: Vec<RawTrack>,
}

impl CaptureOutput {
    pub fn into_batch(self, slot: SlotNumber) -> ReplayBatch {
        ReplayBatch {
            slot,
            events: self.events.into_iter().map(|entry| entry.event).collect(),
            raw_tracks: self.raw_tracks,
        }
    }
}

pub fn capture_block(
    initial_epoch: EpochNumber,
    block: &ParsedBlock,
) -> Result<CaptureOutput, NodeError> {
    let mut current_epoch = initial_epoch;
    let mut events = Vec::new();
    let mut raw_tracks = Vec::new();

    for instruction in &block.instructions {
        let Some(captured) = capture_instruction(&mut current_epoch, instruction)? else {
            continue;
        };
        if let Some(raw_track) = captured.raw_track {
            raw_tracks.push(raw_track);
        }
        events.push(captured.event);
    }

    Ok(CaptureOutput {
        next_epoch: current_epoch,
        events,
        raw_tracks,
    })
}

fn capture_instruction(
    current_epoch: &mut EpochNumber,
    instruction: &ParsedInstruction,
) -> Result<Option<CapturedTrackWrite>, NodeError> {
    let captured = match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            *current_epoch = event.new_epoch;

            CapturedTrackWrite {
                event: CapturedEvent {
                    epoch: event.new_epoch,
                    event: ReplayableEvent::AdvanceEpoch {
                        old_epoch: event.old_epoch,
                        new_epoch: event.new_epoch,
                    },
                },
                raw_track: None,
            }
        },
        ParsedInstruction::SyncEpoch { event } => CapturedTrackWrite {
            event: CapturedEvent {
                    epoch: *current_epoch,
                    event: ReplayableEvent::SyncEpoch {
                        node: event.node,
                        node_id: event.id,
                        epoch: event.epoch,
                        spools_hash: event.spools_hash,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::ReserveSnapshot { event } => {
            let snapshot_tape = snapshot_tape_pda(event.epoch).0;
            CapturedTrackWrite {
                event: CapturedEvent {
                    epoch: event.epoch,
                    event: ReplayableEvent::ReserveTape {
                        tape: snapshot_tape,
                        authority: SYSTEM_ADDRESS,
                        active_epoch: event.epoch,
                        expiry_epoch: EpochNumber(u64::MAX),
                    },
                },
                raw_track: None,
            }
        }
        ParsedInstruction::WriteSnapshot {
            group,
            chunk_index,
            blob,
            event,
        } => {
            let snapshot_tape = snapshot_tape_pda(event.epoch).0;
            let key = snapshot_chunk_key(event.epoch, *group, *chunk_index);
            let track = CompressedTrack {
                tape: snapshot_tape,
                key,
                track_number: event.track_number,
                kind: TrackKind::Blob as u64,
                state: TrackState::Certified as u64,
                size: blob.size,
                spool_group: *group,
                value_hash: blob.get_hash(),
            };

            // Sanity check: the reconstructed track must commit to the same hash the
            // program logged. If this fires, capture is drifting from the program's
            // CompressedTrack derivation.
            if track.get_hash() != event.track_hash {
                return Err(ParseError::Deserialization(
                    "snapshot chunk hash mismatch between capture and program".into(),
                )
                .into());
            }

            CapturedTrackWrite {
                event: CapturedEvent {
                    epoch: event.epoch,
                    event: ReplayableEvent::Track(ReplayTrack {
                        state: track,
                        epoch: event.epoch,
                        blob: Some(*blob),
                    }),
                },
                raw_track: None,
            }
        }
        ParsedInstruction::SignSnapshot { .. } => return Ok(None),
        ParsedInstruction::TrackWrite {
            track,
            key,
            value,
            event,
            ..
        } => {
            let meta = value
                .meta()
                .ok_or_else(|| ParseError::Deserialization("invalid track payload".into()))?;

            CapturedTrackWrite {
                event: CapturedEvent {
                    epoch: *current_epoch,
                    event: ReplayableEvent::Track(ReplayTrack {
                        state: CompressedTrack {
                            tape: event.tape,
                            key: *key,
                            track_number: event.track_number,
                            kind: meta.kind as u64,
                            state: meta.initial_state as u64,
                            size: meta.size,
                            spool_group: event.spool_group,
                            value_hash: meta.value_hash,
                        },
                        epoch: event.epoch,
                        blob: match value {
                            TrackData::Raw(_) => None,
                            TrackData::Blob(blob) => Some(*blob),
                        },
                    }),
                },
                raw_track: match value {
                    TrackData::Raw(bytes) => Some(RawTrack {
                        track: *track,
                        spool_group: event.spool_group,
                        data: bytes.clone(),
                    }),
                    TrackData::Blob(_) => None,
                },
            }
        },
        ParsedInstruction::DeleteTrack { track, .. } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::DeleteTrack {
                    track: (*track).into(),
                    epoch: *current_epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::CertifyTrack { track, event } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::CertifyTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::InvalidateTrack { track, event } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::InvalidateTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::ReserveTape { tape, event, .. } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::ReserveTape {
                    tape: (*tape).into(),
                    authority: event.authority,
                    active_epoch: event.active_epoch,
                    expiry_epoch: event.expiry_epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::DestroyTape { tape, .. } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::DestroyTape {
                    tape: (*tape).into(),
                    epoch: *current_epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::RegisterNode {
            authority,
            node,
            ..
        } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::RegisterNode {
                    authority: (*authority).into(),
                    node: (*node).into(),
                },
            },
            raw_track: None,
        },
        ParsedInstruction::JoinNetwork { node, .. } => CapturedTrackWrite {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::JoinNetwork {
                    node: (*node).into(),
                },
            },
            raw_track: None,
        },
        ParsedInstruction::AdvancePool { .. } => return Ok(None),
    };

    Ok(Some(captured))
}

#[cfg(test)]
mod tests {
    use tape_api::event::{
        EpochAdvanced, SnapshotReserved, SnapshotSigned, SnapshotWritten, TapeReserved,
        TrackCertified, TrackWritten,
    };
    use tape_api::program::tapedrive::{snapshot_tape_pda, SYSTEM_ADDRESS};
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::chunk::snapshot_chunk_key;
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{ChunkNumber, EpochNumber, SlotNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_crypto::Hash;

    use super::capture_block;
    use crate::features::block::ingestor::ParsedBlock;

    fn blob_info(slices: &[Vec<u8>]) -> BlobInfo {
        let leaves = core::array::from_fn(|index| hash_leaf(&slices[index]));
        let commitment = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves);

        BlobInfo {
            size: StorageUnits::from_bytes(64 * slices.len() as u64),
            commitment,
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(slices.len() as u64),
            leaves,
        }
    }

    fn default_blob() -> BlobInfo {
        let payload = vec![vec![0xAA; 64]; SPOOL_GROUP_SIZE];
        blob_info(&payload)
    }

    fn blob_track_write_instruction(track: Address, tape: Address, epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track,
            key: Hash::new_unique(),
            value: TrackData::Blob(default_blob()),
            event: TrackWritten {
                epoch,
                track,
                tape,
                track_number: TrackNumber(7),
                spool_group: SpoolGroup(3),
                track_hash: Hash::new_unique(),
            },
        }
    }

    fn snapshot_block(epoch: EpochNumber, group: SpoolGroup, chunk_index: ChunkNumber) -> ParsedBlock {
        let blob = default_blob();
        let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
        let key = snapshot_chunk_key(epoch, group, chunk_index);
        let track_number = TrackNumber(0);
        let expected_track = CompressedTrack {
            tape: snapshot_tape,
            key,
            track_number,
            kind: TrackKind::Blob as u64,
            state: TrackState::Certified as u64,
            size: blob.size,
            spool_group: group,
            value_hash: blob.get_hash(),
        };

        ParsedBlock {
            slot: SlotNumber(7),
            instructions: vec![
                ParsedInstruction::ReserveSnapshot {
                    event: SnapshotReserved { epoch },
                },
                ParsedInstruction::WriteSnapshot {
                    group,
                    chunk_index,
                    blob,
                    event: SnapshotWritten {
                        epoch,
                        group,
                        track: Address::new_unique(),
                        track_number,
                        track_hash: expected_track.get_hash(),
                    },
                },
                ParsedInstruction::SignSnapshot {
                    event: SnapshotSigned {
                        epoch,
                        group,
                        state: 0,
                    },
                },
            ],
        }
    }

    fn raw_track_write_instruction(track: Address, tape: Address, epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track,
            key: Hash::new_unique(),
            value: TrackData::Raw(vec![0xAB; 4 * 1024]),
            event: TrackWritten {
                epoch,
                track,
                tape,
                track_number: TrackNumber(8),
                spool_group: SpoolGroup(4),
                track_hash: Hash::new_unique(),
            },
        }
    }

    fn certify_track_instruction(track: Address, epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::CertifyTrack {
            track,
            event: TrackCertified {
                track,
                epoch,
                signer_count: 7u64.to_le_bytes(),
                signer_weight: 9u64.to_le_bytes(),
            },
        }
    }

    fn reserve_tape_instruction(tape: Address, active_epoch: EpochNumber, expiry_epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::ReserveTape {
            owner: Address::new_unique(),
            tape,
            event: TapeReserved {
                tape,
                authority: Address::new_unique(),
                capacity: StorageUnits::mb(10),
                active_epoch,
                expiry_epoch,
                cost: 11u64.to_le_bytes(),
            },
        }
    }

    fn advance_epoch_instruction(old_epoch: EpochNumber, new_epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch,
                new_epoch,
                timestamp: 0u64.to_le_bytes(),
                committee_size: 128u64.to_le_bytes(),
                total_stake: 1_000u64.to_le_bytes(),
                storage_price: 5u64.to_le_bytes(),
                storage_capacity: StorageUnits::mb(1_000),
                nonce: Hash::new_unique(),
                phase: 0,
            },
        }
    }

    #[test]
    fn keeps_order() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let block = ParsedBlock {
            slot: SlotNumber(42),
            instructions: vec![
                blob_track_write_instruction(track, tape, EpochNumber(7)),
                certify_track_instruction(track, EpochNumber(8)),
                reserve_tape_instruction(tape, EpochNumber(7), EpochNumber(12)),
            ],
        };

        let captured = capture_block(EpochNumber(7), &block).unwrap();
        let batch = captured.into_batch(block.slot);

        assert_eq!(batch.events.len(), 3);
        assert!(batch.raw_tracks.is_empty());
        assert!(matches!(batch.events[0], ReplayableEvent::Track(_)));
        assert!(matches!(
            batch.events[1],
            ReplayableEvent::CertifyTrack { .. }
        ));
        assert!(matches!(
            batch.events[2],
            ReplayableEvent::ReserveTape { .. }
        ));
    }

    #[test]
    fn rebuckets_events() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let block = ParsedBlock {
            slot: SlotNumber(100),
            instructions: vec![
                blob_track_write_instruction(track, tape, EpochNumber(4)),
                advance_epoch_instruction(EpochNumber(4), EpochNumber(5)),
                reserve_tape_instruction(tape, EpochNumber(5), EpochNumber(10)),
            ],
        };

        let captured = capture_block(EpochNumber(4), &block).unwrap();

        assert_eq!(captured.next_epoch, EpochNumber(5));
        assert_eq!(captured.events.len(), 3);
        assert_eq!(captured.events[0].epoch, EpochNumber(4));
        assert_eq!(captured.events[1].epoch, EpochNumber(5));
        assert_eq!(captured.events[2].epoch, EpochNumber(5));
        assert!(matches!(
            captured.events[1].event,
            ReplayableEvent::AdvanceEpoch {
                old_epoch: EpochNumber(4),
                new_epoch: EpochNumber(5),
            }
        ));
    }

    #[test]
    fn captures_raw_track_write() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let block = ParsedBlock {
            slot: SlotNumber(5),
            instructions: vec![raw_track_write_instruction(track, tape, EpochNumber(9))],
        };

        let captured = capture_block(EpochNumber(9), &block).unwrap();
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.raw_tracks.len(), 1);

        match &captured.events[0].event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, tape);
                assert_eq!(track.state.track_number, TrackNumber(8));
                assert_eq!(u64::from(track.state.spool_group), 4);
                assert_eq!(track.state.kind, TrackKind::Raw as u64);
                assert_eq!(track.state.state, TrackState::Certified as u64);
                assert!(track.blob.is_none());
            }
            _ => panic!("expected ReplayableEvent::Track"),
        }

        assert_eq!(captured.raw_tracks[0].track, track.into());
        assert_eq!(u64::from(captured.raw_tracks[0].spool_group), 4);
        assert_eq!(captured.raw_tracks[0].data, vec![0xAB; 4 * 1024]);
    }

    #[test]
    fn captures_snapshot_chunks() {
        let epoch = EpochNumber(7);
        let group = SpoolGroup(3);
        let chunk_index = ChunkNumber(0);

        let block = snapshot_block(epoch, group, chunk_index);
        let captured = capture_block(epoch, &block).unwrap();

        // Reserve emits ReserveTape for the snapshot tape, Write emits a Track,
        // Sign emits nothing.
        assert_eq!(captured.events.len(), 2);
        assert!(captured.raw_tracks.is_empty());
        assert_eq!(captured.next_epoch, epoch);

        let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
        let expected_key = snapshot_chunk_key(epoch, group, chunk_index);

        match &captured.events[0].event {
            ReplayableEvent::ReserveTape {
                tape,
                authority,
                active_epoch,
                expiry_epoch,
            } => {
                assert_eq!(*tape, snapshot_tape);
                assert_eq!(*authority, SYSTEM_ADDRESS);
                assert_eq!(*active_epoch, epoch);
                assert_eq!(*expiry_epoch, EpochNumber(u64::MAX));
            }
            other => panic!("expected ReserveTape for snapshot tape, got {other:?}"),
        }

        match &captured.events[1].event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, snapshot_tape);
                assert_eq!(track.state.key, expected_key);
                assert_eq!(track.state.track_number, TrackNumber(0));
                assert_eq!(track.state.spool_group, group);
                assert_eq!(track.state.kind, TrackKind::Blob as u64);
                assert_eq!(track.state.state, TrackState::Certified as u64);
                assert_eq!(track.epoch, epoch);
                assert!(track.blob.is_some());
            }
            other => panic!("expected Track event from WriteSnapshot, got {other:?}"),
        }
    }
}

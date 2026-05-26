use bytemuck::bytes_of;
use tape_api::event::{TapeDestroyed, TapeReserved, TrackDeleted, TrackWritten};
use tape_api::program::tapedrive::{track_pda, SYSTEM_ADDRESS};
use tape_blocks::{ParseError, ParsedInstruction};
use tape_core::snapshot::replay::{ReplayTrack, ReplayableEvent};
use tape_core::tape::{snapshot_tape_number, TapeFlags};
use tape_core::track::data::TrackDataSlice;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SlotNumber, SpoolIndex};
use tape_crypto::Hash;

use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::{RawTrack, ReplayBatch};

pub struct CapturedEvent {
    pub epoch: EpochNumber,
    pub event: ReplayableEvent,
}

struct Captured {
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
) -> Result<Option<Captured>, NodeError> {
    let captured = match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            *current_epoch = event.new_epoch;

            Captured {
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
        ParsedInstruction::SyncSpool { event, .. } => Captured {
            event: CapturedEvent {
                    epoch: *current_epoch,
                    event: ReplayableEvent::SyncSpool {
                        node: event.node,
                        epoch: event.epoch,
                        group: event.group,
                        spool: SpoolIndex::from(u64::from_le_bytes(event.spool)),
                },
            },
            raw_track: None,
        },
        ParsedInstruction::FinalizeSnapshot { event, .. } => {
            Captured {
                event: CapturedEvent {
                    epoch: *current_epoch,
                    event: ReplayableEvent::ReserveTape {
                        tape: event.snapshot_tape,
                        id: snapshot_tape_number(event.epoch),
                        flags: TapeFlags::SYSTEM,
                        authority: SYSTEM_ADDRESS,
                        active_epoch: event.epoch,
                        expiry_epoch: EpochNumber(u64::MAX),
                    },
                },
                raw_track: None,
            }
        }
        ParsedInstruction::TrackWrite {
            key,
            value,
            event,
            ..
        } => capture_track(*current_epoch, event, *key, value.as_slice())?,
        ParsedInstruction::DeleteTrack { event, .. } => capture_delete(*current_epoch, event),
        ParsedInstruction::CertifyTrack { track, event } => Captured {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::CertifyTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::InvalidateTrack { track, event } => Captured {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::InvalidateTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::ReserveTape { event, .. } => capture_tape(*current_epoch, event),
        ParsedInstruction::DestroyTape { event, .. } => capture_destroy(*current_epoch, event),
        ParsedInstruction::RegisterNode { authority, event, .. } => Captured {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::RegisterNode {
                    authority: (*authority).into(),
                    node: event.node,
                    id: event.id,
                },
            },
            raw_track: None,
        },
        ParsedInstruction::JoinCommittee { node, .. } => Captured {
            event: CapturedEvent {
                epoch: *current_epoch,
                event: ReplayableEvent::JoinCommittee {
                    node: (*node).into(),
                },
            },
            raw_track: None,
        },
        ParsedInstruction::AddToBlacklist { entry, event, .. } => {
            capture_track(
                *current_epoch,
                event,
                entry.key(),
                TrackDataSlice::Raw(bytes_of(entry)),
            )?
        }
        ParsedInstruction::RemoveFromBlacklist { event, .. } => capture_delete(*current_epoch, event),
        ParsedInstruction::AdvancePool {
            span,
            track_event,
            ..
        } => {
            capture_track(
                *current_epoch,
                track_event,
                span.key(),
                TrackDataSlice::Raw(bytes_of(span)),
            )?
        }

        // Empty variants that don't produce replay events.
        ParsedInstruction::StakeWithPool { .. }
        | ParsedInstruction::RequestStakeUnlock { .. }
        | ParsedInstruction::UnstakeFromPool { .. }
        | ParsedInstruction::ClaimCommission { .. }
        | ParsedInstruction::CommitEpoch { .. }
        | ParsedInstruction::ProposeSnapshot { .. }
        | ParsedInstruction::VoteSnapshot { .. }
        | ParsedInstruction::ProposeAssignment { .. }
        | ParsedInstruction::VoteAssignment { .. }
        | ParsedInstruction::FinalizeGroup { .. }
        | ParsedInstruction::CreateEpoch { .. }
        | ParsedInstruction::CreateCommittee { .. }
        | ParsedInstruction::ResizeCommittee { .. }
        | ParsedInstruction::ResizePeerSet { .. } => return Ok(None),
    };

    Ok(Some(captured))
}

fn capture_track(
    epoch: EpochNumber,
    event: &TrackWritten,
    key: Hash,
    data: TrackDataSlice<'_>,
) -> Result<Captured, NodeError> {
    let meta = data
        .meta()
        .ok_or_else(|| ParseError::Deserialization("invalid track payload".into()))?;

    let state = CompressedTrack {
        tape: event.tape,
        key,
        track_number: event.track_number,
        kind: meta.kind as u64,
        state: meta.state as u64,
        size: meta.size,
        group: event.group,
        value_hash: meta.value_hash,
    };

    if track_pda(event.tape, event.track_number).0 != event.track {
        return Err(ParseError::EventMismatch("unexpected TrackWritten track address").into());
    }

    if state.get_hash() != event.track_hash {
        return Err(ParseError::EventMismatch("unexpected TrackWritten track hash").into());
    }

    Ok(Captured {
        event: CapturedEvent {
            epoch,
            event: ReplayableEvent::Track(ReplayTrack {
                state,
                epoch: event.epoch,
                blob: match data {
                    TrackDataSlice::Raw(_) => None,
                    TrackDataSlice::Blob(blob) => Some(blob),
                },
            }),
        },
        raw_track: match data {
            TrackDataSlice::Raw(bytes) => Some(RawTrack {
                track: event.track,
                group: event.group,
                data: bytes.to_vec(),
            }),
            TrackDataSlice::Blob(_) => None,
        },
    })
}

fn capture_tape(epoch: EpochNumber, event: &TapeReserved) -> Captured {
    Captured {
        event: CapturedEvent {
            epoch,
            event: ReplayableEvent::ReserveTape {
                tape: event.tape,
                id: event.id,
                flags: event.flags,
                authority: event.authority,
                active_epoch: event.active_epoch,
                expiry_epoch: event.expiry_epoch,
            },
        },
        raw_track: None,
    }
}

fn capture_delete(epoch: EpochNumber, event: &TrackDeleted) -> Captured {
    Captured {
        event: CapturedEvent {
            epoch,
            event: ReplayableEvent::DeleteTrack {
                track: event.track,
                epoch,
            },
        },
        raw_track: None,
    }
}

fn capture_destroy(epoch: EpochNumber, event: &TapeDestroyed) -> Captured {
    Captured {
        event: CapturedEvent {
            epoch,
            event: ReplayableEvent::DestroyTape {
                tape: event.tape,
                epoch,
            },
        },
        raw_track: None,
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::{bytes_of, Zeroable};
    use tape_api::event::{
        EpochAdvanced, SnapshotFinalized, TapeReserved, TrackCertified, TrackWritten,
    };
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda, SYSTEM_ADDRESS};
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{BlacklistEntry, NodePreferences};
    use tape_core::tape::{snapshot_tape_number, TapeFlags};
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_crypto::Hash;

    use super::capture_block;
    use crate::features::block::ingestor::ParsedBlock;

    fn blob_info(slices: &[Vec<u8>]) -> BlobInfo {
        let leaves = core::array::from_fn(|index| hash_leaf(&slices[index]));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

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
        let payload = vec![vec![0xAA; 64]; GROUP_SIZE];
        blob_info(&payload)
    }

    fn track_written_event(
        tape: Address,
        key: Hash,
        value: &TrackData,
        track_number: TrackNumber,
        group: GroupIndex,
        epoch: EpochNumber,
    ) -> TrackWritten {
        let meta = value.meta().unwrap();
        let state = CompressedTrack {
            tape,
            key,
            track_number,
            kind: meta.kind as u64,
            state: meta.state as u64,
            size: meta.size,
            group,
            value_hash: meta.value_hash,
        };
        let track = track_pda(tape, track_number).0;

        TrackWritten {
            epoch,
            track,
            tape,
            track_number,
            group,
            track_hash: state.get_hash(),
        }
    }

    fn blob_track_write_instruction(_track: Address, tape: Address, epoch: EpochNumber) -> ParsedInstruction {
        let value = TrackData::Blob(default_blob());
        let key = Hash::new_unique();
        let event = track_written_event(tape, key, &value, TrackNumber(7), GroupIndex(3), epoch);

        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track: event.track,
            key,
            value,
            event,
        }
    }

    fn finalize_snapshot_instruction(epoch: EpochNumber) -> ParsedInstruction {
        let snapshot_tape = Address::from(snapshot_tape_pda(epoch).0);
        ParsedInstruction::FinalizeSnapshot {
            epoch,
            event: SnapshotFinalized {
                epoch,
                hash: Hash::new_unique(),
                snapshot_tape,
            },
        }
    }

    fn raw_track_write_instruction(_track: Address, tape: Address, epoch: EpochNumber) -> ParsedInstruction {
        let value = TrackData::Raw(vec![0xAB; 4 * 1024]);
        let key = Hash::new_unique();
        let event = track_written_event(tape, key, &value, TrackNumber(8), GroupIndex(4), epoch);

        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track: event.track,
            key,
            value,
            event,
        }
    }

    fn blacklist_add_instruction(
        node: Address,
        _track: Address,
        tape: Address,
        entry: BlacklistEntry,
        epoch: EpochNumber,
    ) -> ParsedInstruction {
        let value = TrackData::Raw(bytes_of(&entry).to_vec());
        let event = track_written_event(tape, entry.key(), &value, TrackNumber(9), GroupIndex(6), epoch);

        ParsedInstruction::AddToBlacklist {
            node,
            entry,
            event,
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
                id: TapeNumber(1),
                flags: 0,
                authority: Address::new_unique(),
                capacity: StorageUnits::mb(10),
                active_epoch,
                expiry_epoch,
                cost: 11u64.to_le_bytes(),
                burned: 1u64.to_le_bytes(),
                scheduled: 10u64.to_le_bytes(),
            },
        }
    }

    fn advance_epoch_instruction(old_epoch: EpochNumber, new_epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch,
                new_epoch,
                timestamp: 0u64.to_le_bytes(),
                total_stake: 1_000u64.to_le_bytes(),
                committee_count: 128u64.to_le_bytes(),
                preferences: NodePreferences::zeroed(),
                subsidy: 0u64.to_le_bytes(),
                nonce: Hash::new_unique(),
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
            ..Default::default()
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
            ..Default::default()
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
        let tape = Address::new_unique();
        let track = track_pda(tape, TrackNumber(8)).0;
        let block = ParsedBlock {
            slot: SlotNumber(5),
            instructions: vec![raw_track_write_instruction(track, tape, EpochNumber(9))],
            ..Default::default()
        };

        let captured = capture_block(EpochNumber(9), &block).unwrap();
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.raw_tracks.len(), 1);

        match &captured.events[0].event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, tape);
                assert_eq!(track.state.track_number, TrackNumber(8));
                assert_eq!(u64::from(track.state.group), 4);
                assert_eq!(track.state.kind, TrackKind::Raw as u64);
                assert_eq!(track.state.state, TrackState::Certified as u64);
                assert!(track.blob.is_none());
            }
            _ => panic!("expected ReplayableEvent::Track"),
        }

        assert_eq!(captured.raw_tracks[0].track, track.into());
        assert_eq!(u64::from(captured.raw_tracks[0].group), 4);
        assert_eq!(captured.raw_tracks[0].data, vec![0xAB; 4 * 1024]);
    }

    #[test]
    fn captures_blacklist_add_as_raw_track() {
        let node = Address::new_unique();
        let tape = Address::new_unique();
        let track = track_pda(tape, TrackNumber(9)).0;
        let entry = BlacklistEntry::tape(Address::new_unique());
        let block = ParsedBlock {
            slot: SlotNumber(6),
            instructions: vec![blacklist_add_instruction(
                node,
                track,
                tape,
                entry,
                EpochNumber(9),
            )],
            ..Default::default()
        };

        let captured = capture_block(EpochNumber(9), &block).unwrap();
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.raw_tracks.len(), 1);

        match &captured.events[0].event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, tape);
                assert_eq!(track.state.track_number, TrackNumber(9));
                assert_eq!(track.state.key, entry.key());
                assert_eq!(u64::from(track.state.group), 6);
                assert_eq!(track.state.kind, TrackKind::Raw as u64);
                assert_eq!(track.state.state, TrackState::Certified as u64);
                assert_eq!(
                    track.state.size,
                    StorageUnits::from_bytes(bytes_of(&entry).len() as u64)
                );
                assert_eq!(track.state.value_hash, entry.key());
                assert!(track.blob.is_none());
            }
            _ => panic!("expected ReplayableEvent::Track"),
        }

        assert_eq!(captured.raw_tracks[0].track, track);
        assert_eq!(u64::from(captured.raw_tracks[0].group), 6);
        assert_eq!(captured.raw_tracks[0].data, bytes_of(&entry).to_vec());
    }

    #[test]
    fn captures_snapshot_finalization() {
        let snapshot_epoch = EpochNumber(7);

        let block = ParsedBlock {
            slot: SlotNumber(7),
            instructions: vec![finalize_snapshot_instruction(snapshot_epoch)],
            ..Default::default()
        };
        let captured = capture_block(snapshot_epoch, &block).unwrap();

        assert_eq!(captured.events.len(), 1);
        assert!(captured.raw_tracks.is_empty());
        assert_eq!(captured.next_epoch, snapshot_epoch);

        let snapshot_tape = Address::from(snapshot_tape_pda(snapshot_epoch).0);

        match &captured.events[0].event {
            ReplayableEvent::ReserveTape {
                tape,
                id,
                flags,
                authority,
                active_epoch,
                expiry_epoch,
            } => {
                assert_eq!(*tape, snapshot_tape);
                assert_eq!(*id, snapshot_tape_number(snapshot_epoch));
                assert_eq!(*flags, TapeFlags::SYSTEM);
                assert_eq!(*authority, SYSTEM_ADDRESS);
                assert_eq!(*active_epoch, snapshot_epoch);
                assert_eq!(*expiry_epoch, EpochNumber(u64::MAX));
            }
            other => panic!("expected ReserveTape for snapshot tape, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_finalization_buckets_by_current_epoch() {
        let old_epoch = EpochNumber(7);
        let new_epoch = EpochNumber(8);
        let snapshot_epoch = old_epoch;

        let block = ParsedBlock {
            slot: SlotNumber(42),
            instructions: vec![
                advance_epoch_instruction(old_epoch, new_epoch),
                finalize_snapshot_instruction(snapshot_epoch),
            ],
            ..Default::default()
        };

        let captured = capture_block(old_epoch, &block).unwrap();

        assert_eq!(captured.next_epoch, new_epoch);
        assert_eq!(captured.events.len(), 2);

        // AdvanceEpoch is tagged with the new epoch (established by the
        // `rebuckets_events` test).
        assert_eq!(captured.events[0].epoch, new_epoch);

        // FinalizeSnapshot references `snapshot_epoch`, but it lands in the
        // current event bucket after AdvanceEpoch has moved the replay cursor.
        assert_eq!(captured.events[1].epoch, new_epoch);

        match &captured.events[1].event {
            ReplayableEvent::ReserveTape { active_epoch, .. } => {
                assert_eq!(*active_epoch, snapshot_epoch);
            }
            other => panic!("expected ReserveTape, got {other:?}"),
        }
    }
}

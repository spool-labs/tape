//! Capture parsed Tapedrive instructions into replayable events.

use bytemuck::bytes_of;
use tape_api::event::{TapeDestroyed, TapeReserved, TrackDeleted, TrackWritten};
use tape_api::program::tapedrive::track_pda;
use tape_core::snapshot::replay::{ReplayRecord, ReplayTrack, ReplayTrackObject, ReplayableEvent};
use tape_core::spooler::GroupIndex;
use tape_core::track::data::BlobDataSlice;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SlotNumber};
use tape_crypto::address::Address;
use tape_crypto::tx::Txid;
use tape_crypto::Hash;

use crate::{ParseError, ParsedInstruction};

/// An event captured from a parsed instruction, ready for replay
pub struct CapturedEvent {
    pub epoch: EpochNumber,
    pub record: ReplayRecord,
}

/// A raw track payload captured from a TrackWrite instruction
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTrack {
    pub track: Address,
    pub group: GroupIndex,
    pub data: Vec<u8>,
}

/// The output of capturing a block's instructions
pub struct CaptureOutput {
    pub next_epoch: EpochNumber,
    pub events: Vec<CapturedEvent>,
    pub raw_tracks: Vec<RawTrack>,
}


/// Capture a block's instructions into replayable events.
pub fn capture_block(
    initial_epoch: EpochNumber,
    slot: SlotNumber,
    instructions: &[ParsedInstruction],
    tx_ids: &[Txid],
) -> Result<CaptureOutput, ParseError> {
    let mut current_epoch = initial_epoch;
    let mut events = Vec::new();
    let mut raw_tracks = Vec::new();

    for (index, instruction) in instructions.iter().enumerate() {
        let tx_id = *tx_ids.get(index).ok_or_else(|| {
            ParseError::Deserialization(format!(
                "missing tx id for instruction {index} at slot {}",
                slot.0
            ))
        })?;

        let Some(captured) = capture_instruction(&mut current_epoch, instruction, tx_id)? else {
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

struct Captured {
    event: CapturedEvent,
    raw_track: Option<RawTrack>,
}

fn capture_instruction(
    current_epoch: &mut EpochNumber,
    instruction: &ParsedInstruction,
    tx_id: Txid,
) -> Result<Option<Captured>, ParseError> {
    let actor = actor_for(instruction);
    let captured = match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            *current_epoch = event.new_epoch;

            Captured {
                event: captured_event(
                    event.new_epoch,
                    tx_id,
                    actor,
                    ReplayableEvent::AdvanceEpoch {
                        old_epoch: event.old_epoch,
                        new_epoch: event.new_epoch,
                        timestamp: event.timestamp,
                        total_stake: event.total_stake,
                        committee_count: event.committee_count,
                        preferences: event.preferences,
                        subsidy: event.subsidy,
                        nonce: event.nonce,
                    },
                ),
                raw_track: None,
            }
        }
        ParsedInstruction::StartNetwork => {
            // Genesis epoch transition (0 -> 1). Emits no replayable event, 
            // but the replay epoch counter MUST advance here.
            if current_epoch.is_zero() {
                *current_epoch = EpochNumber(1);
            }
            return Ok(None);
        }
        ParsedInstruction::SyncSpool { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::SyncSpool {
                    node: event.node,
                    epoch: event.epoch,
                    group: event.group,
                    spool: event.spool,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::FinalizeSnapshot { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::SnapshotFinalized {
                    epoch: event.epoch,
                    hash: event.hash,
                    snapshot_tape: event.snapshot_tape,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::FinalizeGroup { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::AssignmentFinalized {
                    epoch: event.epoch,
                    hash: event.hash,
                    group: event.group,
                    group_account: event.group_account,
                    size: event.size,
                    total_groups: event.total_groups,
                    total_assigned: event.total_assigned,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::StakeWithPool { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::StakeDeposited {
                    stake: event.stake,
                    authority: event.authority,
                    pool: event.pool,
                    amount: event.amount,
                    activation_epoch: event.activation_epoch,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::RequestStakeUnlock { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::StakeUnlockRequested {
                    stake: event.stake,
                    authority: event.authority,
                    pool: event.pool,
                    amount: event.amount,
                    withdraw_epoch: event.withdraw_epoch,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::UnstakeFromPool { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::StakeWithdrawn {
                    stake: event.stake,
                    authority: event.authority,
                    pool: event.pool,
                    principal: event.principal,
                    rewards: event.rewards,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::ProposeSnapshot { event, .. }
        | ParsedInstruction::ProposeAssignment { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::VoteProposed {
                    kind: event.kind,
                    vote: event.vote,
                    voting_epoch: event.voting_epoch,
                    target_epoch: event.target_epoch,
                    hash: event.hash,
                    total_groups: event.total_groups,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::VoteSnapshot { event, .. }
        | ParsedInstruction::VoteAssignment { event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::VoteRecorded {
                    kind: event.kind,
                    vote: event.vote,
                    voting_epoch: event.voting_epoch,
                    target_epoch: event.target_epoch,
                    hash: event.hash,
                    group: event.group,
                    signer_count: event.signer_count,
                    signed_groups: event.signed_groups,
                    total_groups: event.total_groups,
                    signers: *event.bitmap.as_bytes(),
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::TrackWrite {
            key,
            object,
            value,
            event,
            ..
        } => capture_track(
            *current_epoch,
            tx_id,
            actor,
            event,
            *key,
            object.clone(),
            value.as_slice(),
        )?,
        ParsedInstruction::DeleteTrack { event, .. } => {
            capture_delete(*current_epoch, tx_id, actor, event)
        }
        ParsedInstruction::CertifyTrack { track, event } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::CertifyTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::InvalidateTrack { track, event } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::InvalidateTrack {
                    track: (*track).into(),
                    epoch: event.epoch,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::ReserveTape { event, .. } => {
            capture_tape(*current_epoch, tx_id, actor, event)
        }
        ParsedInstruction::DestroyTape { event, .. } => {
            capture_destroy(*current_epoch, tx_id, actor, event)
        }
        ParsedInstruction::RegisterNode { authority, event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::RegisterNode {
                    authority: (*authority).into(),
                    node: event.node,
                    id: event.id,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::JoinCommittee { node, event, .. } => Captured {
            event: captured_event(
                *current_epoch,
                tx_id,
                actor,
                ReplayableEvent::JoinCommittee {
                    node: (*node).into(),
                    stake: event.stake,
                    key: event.key,
                    preferences: event.preferences,
                    activation_epoch: event.activation_epoch,
                },
            ),
            raw_track: None,
        },
        ParsedInstruction::AddToBlacklist { entry, event, .. } => capture_track(
            *current_epoch,
            tx_id,
            actor,
            event,
            entry.key(),
            None,
            BlobDataSlice::Inline(bytes_of(entry)),
        )?,
        ParsedInstruction::RemoveFromBlacklist { event, .. } => {
            capture_delete(*current_epoch, tx_id, actor, event)
        }
        ParsedInstruction::AdvancePool { span, track_event, .. } => capture_track(
            *current_epoch,
            tx_id,
            actor,
            track_event,
            span.key(),
            None,
            BlobDataSlice::Inline(bytes_of(span)),
        )?,

        // No corresponding ReplayableEvent variant yet — silently drop.
        // These can be added when a use case appears.
        ParsedInstruction::ClaimCommission { .. }
        | ParsedInstruction::CommitEpoch { .. }
        | ParsedInstruction::CreateEpoch { .. }
        | ParsedInstruction::CreateCommittee { .. }
        | ParsedInstruction::ResizeCommittee { .. }
        | ParsedInstruction::ResizePeerSet { .. } => return Ok(None),
    };

    Ok(Some(captured))
}

fn captured_event(
    epoch: EpochNumber,
    tx_id: Txid,
    actor: Option<Address>,
    event: ReplayableEvent,
) -> CapturedEvent {
    CapturedEvent {
        epoch,
        record: ReplayRecord { tx_id, actor, event },
    }
}

fn capture_track(
    epoch: EpochNumber,
    tx_id: Txid,
    actor: Option<Address>,
    event: &TrackWritten,
    key: Hash,
    object: Option<ReplayTrackObject>,
    data: BlobDataSlice<'_>,
) -> Result<Captured, ParseError> {
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
        return Err(ParseError::EventMismatch("unexpected TrackWritten track address"));
    }

    if state.get_hash() != event.track_hash {
        return Err(ParseError::EventMismatch("unexpected TrackWritten track hash"));
    }

    Ok(Captured {
        event: captured_event(
            epoch,
            tx_id,
            actor,
            ReplayableEvent::Track(ReplayTrack {
                state,
                epoch: event.epoch,
                blob: match data {
                    BlobDataSlice::Inline(_) => None,
                    BlobDataSlice::Coded(blob) => Some(blob),
                },
                object,
            }),
        ),
        raw_track: match data {
            BlobDataSlice::Inline(bytes) => Some(RawTrack {
                track: event.track,
                group: event.group,
                data: bytes.to_vec(),
            }),
            BlobDataSlice::Coded(_) => None,
        },
    })
}

fn capture_tape(
    epoch: EpochNumber,
    tx_id: Txid,
    actor: Option<Address>,
    event: &TapeReserved,
) -> Captured {
    Captured {
        event: captured_event(
            epoch,
            tx_id,
            actor,
            ReplayableEvent::ReserveTape {
                tape: event.tape,
                id: event.id,
                flags: event.flags,
                authority: event.authority,
                capacity: event.capacity,
                active_epoch: event.active_epoch,
                expiry_epoch: event.expiry_epoch,
                cost: event.cost,
                burned: event.burned,
                scheduled: event.scheduled,
            },
        ),
        raw_track: None,
    }
}

fn capture_delete(
    epoch: EpochNumber,
    tx_id: Txid,
    actor: Option<Address>,
    event: &TrackDeleted,
) -> Captured {
    Captured {
        event: captured_event(
            epoch,
            tx_id,
            actor,
            ReplayableEvent::DeleteTrack { track: event.track, epoch },
        ),
        raw_track: None,
    }
}

fn capture_destroy(
    epoch: EpochNumber,
    tx_id: Txid,
    actor: Option<Address>,
    event: &TapeDestroyed,
) -> Captured {
    Captured {
        event: captured_event(
            epoch,
            tx_id,
            actor,
            ReplayableEvent::DestroyTape { tape: event.tape, epoch },
        ),
        raw_track: None,
    }
}

fn actor_for(instruction: &ParsedInstruction) -> Option<Address> {
    match instruction {
        ParsedInstruction::SyncSpool { node, .. }
        | ParsedInstruction::AdvancePool { node, .. }
        | ParsedInstruction::JoinCommittee { node, .. }
        | ParsedInstruction::AddToBlacklist { node, .. }
        | ParsedInstruction::RemoveFromBlacklist { node, .. } => Some((*node).into()),

        ParsedInstruction::TrackWrite { authority, .. }
        | ParsedInstruction::RegisterNode { authority, .. }
        | ParsedInstruction::StakeWithPool { authority, .. }
        | ParsedInstruction::RequestStakeUnlock { authority, .. }
        | ParsedInstruction::UnstakeFromPool { authority, .. }
        | ParsedInstruction::ClaimCommission { authority, .. } => Some((*authority).into()),

        ParsedInstruction::DeleteTrack { owner, .. }
        | ParsedInstruction::ReserveTape { owner, .. }
        | ParsedInstruction::DestroyTape { owner, .. } => Some((*owner).into()),

        ParsedInstruction::ProposeSnapshot { proposer, .. }
        | ParsedInstruction::ProposeAssignment { proposer, .. } => Some(*proposer),

        ParsedInstruction::VoteSnapshot { submitter, .. }
        | ParsedInstruction::VoteAssignment { submitter, .. } => Some(*submitter),

        ParsedInstruction::CreateEpoch { .. }
        | ParsedInstruction::CreateCommittee { .. }
        | ParsedInstruction::ResizeCommittee { .. }
        | ParsedInstruction::ResizePeerSet { .. }
        | ParsedInstruction::CommitEpoch { .. }
        | ParsedInstruction::AdvanceEpoch { .. }
        | ParsedInstruction::FinalizeSnapshot { .. }
        | ParsedInstruction::FinalizeGroup { .. }
        | ParsedInstruction::CertifyTrack { .. }
        | ParsedInstruction::InvalidateTrack { .. }
        | ParsedInstruction::StartNetwork => None,
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::{bytes_of, Zeroable};
    use tape_api::event::{
        EpochAdvanced, SnapshotFinalized, TapeReserved, TrackCertified, TrackWritten,
    };
    use tape_api::program::tapedrive::{snapshot_tape_pda, track_pda};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{BlacklistEntry, NodePreferences};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{
        EpochNumber, SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber,
    };
    use tape_crypto::address::Address;
    use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};
    use tape_crypto::tx::Txid;
    use tape_crypto::Hash;

    use super::{capture_block, CaptureOutput};
    use crate::ParsedInstruction;

    fn blob_encoding(slices: &[Vec<u8>]) -> BlobEncoding {
        let leaves = core::array::from_fn(|index| hash_leaf(&slices[index]));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        BlobEncoding {
            size: StorageUnits::from_bytes(64 * slices.len() as u64),
            commitment,
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(slices.len() as u64),
            leaves,
        }
    }

    fn default_blob() -> BlobEncoding {
        let payload = vec![vec![0xAA; 64]; GROUP_SIZE];
        blob_encoding(&payload)
    }

    fn track_written_event(
        tape: Address,
        key: Hash,
        value: &BlobData,
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
        let value = BlobData::Coded(default_blob());
        let key = Hash::new_unique();
        let event = track_written_event(tape, key, &value, TrackNumber(7), GroupIndex(3), epoch);

        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track: event.track,
            key,
            object: None,
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
        let value = BlobData::Inline(vec![0xAB; 4 * 1024]);
        let key = Hash::new_unique();
        let event = track_written_event(tape, key, &value, TrackNumber(8), GroupIndex(4), epoch);

        ParsedInstruction::TrackWrite {
            authority: Address::new_unique(),
            track: event.track,
            key,
            object: None,
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
        let value = BlobData::Inline(bytes_of(&entry).to_vec());
        let event = track_written_event(tape, entry.key(), &value, TrackNumber(9), GroupIndex(6), epoch);

        ParsedInstruction::AddToBlacklist { node, entry, event }
    }

    fn certify_track_instruction(track: Address, epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::CertifyTrack {
            track,
            event: TrackCertified {
                track,
                epoch,
                signer_count: 7,
                signer_weight: 9,
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
                cost: TAPE(11),
                burned: TAPE(1),
                scheduled: TAPE(10),
            },
        }
    }

    fn advance_epoch_instruction(old_epoch: EpochNumber, new_epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::AdvanceEpoch {
            event: EpochAdvanced {
                old_epoch,
                new_epoch,
                timestamp: 0,
                total_stake: TAPE(1_000),
                committee_count: 128,
                preferences: NodePreferences::zeroed(),
                subsidy: TAPE(0),
                nonce: Hash::new_unique(),
            },
        }
    }

    fn capture(
        epoch: EpochNumber,
        slot: SlotNumber,
        instructions: Vec<ParsedInstruction>,
    ) -> CaptureOutput {
        let tx_ids = vec![Txid::default(); instructions.len()];
        capture_block(epoch, slot, &instructions, &tx_ids).unwrap()
    }

    #[test]
    fn keeps_order() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let captured = capture(
            EpochNumber(7),
            SlotNumber(42),
            vec![
                blob_track_write_instruction(track, tape, EpochNumber(7)),
                certify_track_instruction(track, EpochNumber(8)),
                reserve_tape_instruction(tape, EpochNumber(7), EpochNumber(12)),
            ],
        );

        assert_eq!(captured.events.len(), 3);
        assert!(captured.raw_tracks.is_empty());
        assert!(matches!(captured.events[0].record.event, ReplayableEvent::Track(_)));
        assert!(matches!(
            captured.events[1].record.event,
            ReplayableEvent::CertifyTrack { .. }
        ));
        assert!(matches!(
            captured.events[2].record.event,
            ReplayableEvent::ReserveTape { .. }
        ));
    }

    #[test]
    fn rebuckets_events() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let captured = capture(
            EpochNumber(4),
            SlotNumber(100),
            vec![
                blob_track_write_instruction(track, tape, EpochNumber(4)),
                advance_epoch_instruction(EpochNumber(4), EpochNumber(5)),
                reserve_tape_instruction(tape, EpochNumber(5), EpochNumber(10)),
            ],
        );

        assert_eq!(captured.next_epoch, EpochNumber(5));
        assert_eq!(captured.events.len(), 3);
        assert_eq!(captured.events[0].epoch, EpochNumber(4));
        assert_eq!(captured.events[1].epoch, EpochNumber(5));
        assert_eq!(captured.events[2].epoch, EpochNumber(5));
        assert!(matches!(
            captured.events[1].record.event,
            ReplayableEvent::AdvanceEpoch {
                old_epoch: EpochNumber(4),
                new_epoch: EpochNumber(5),
                ..
            }
        ));
    }

    #[test]
    fn start_network_advances_replay_epoch() {
        let tape = Address::new_unique();
        let captured = capture(
            EpochNumber(0),
            SlotNumber(50),
            vec![
                reserve_tape_instruction(tape, EpochNumber(99), EpochNumber(99)),
                ParsedInstruction::StartNetwork,
                reserve_tape_instruction(tape, EpochNumber(99), EpochNumber(99)),
            ],
        );

        // StartNetwork itself produces no event; only the two reserves.
        assert_eq!(captured.events.len(), 2);
        assert_eq!(captured.events[0].epoch, EpochNumber(0));
        assert_eq!(captured.events[1].epoch, EpochNumber(1));
        assert_eq!(captured.next_epoch, EpochNumber(1));
    }

    #[test]
    fn start_network_is_noop_after_genesis() {
        let tape = Address::new_unique();
        let captured = capture(
            EpochNumber(1),
            SlotNumber(50),
            vec![
                ParsedInstruction::StartNetwork,
                reserve_tape_instruction(tape, EpochNumber(99), EpochNumber(99)),
            ],
        );

        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.events[0].epoch, EpochNumber(1));
        assert_eq!(captured.next_epoch, EpochNumber(1));
    }

    #[test]
    fn captures_raw_track_write() {
        let tape = Address::new_unique();
        let track = track_pda(tape, TrackNumber(8)).0;
        let captured = capture(
            EpochNumber(9),
            SlotNumber(5),
            vec![raw_track_write_instruction(track, tape, EpochNumber(9))],
        );
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.raw_tracks.len(), 1);

        match &captured.events[0].record.event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, tape);
                assert_eq!(track.state.track_number, TrackNumber(8));
                assert_eq!(u64::from(track.state.group), 4);
                assert_eq!(track.state.kind, TrackKind::Inline as u64);
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
        let captured = capture(
            EpochNumber(9),
            SlotNumber(6),
            vec![blacklist_add_instruction(node, track, tape, entry, EpochNumber(9))],
        );
        assert_eq!(captured.events.len(), 1);
        assert_eq!(captured.raw_tracks.len(), 1);

        match &captured.events[0].record.event {
            ReplayableEvent::Track(track) => {
                assert_eq!(track.state.tape, tape);
                assert_eq!(track.state.track_number, TrackNumber(9));
                assert_eq!(track.state.key, entry.key());
                assert_eq!(u64::from(track.state.group), 6);
                assert_eq!(track.state.kind, TrackKind::Inline as u64);
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
        let captured = capture(
            snapshot_epoch,
            SlotNumber(7),
            vec![finalize_snapshot_instruction(snapshot_epoch)],
        );

        assert_eq!(captured.events.len(), 1);
        assert!(captured.raw_tracks.is_empty());
        assert_eq!(captured.next_epoch, snapshot_epoch);

        let expected_tape = Address::from(snapshot_tape_pda(snapshot_epoch).0);

        match &captured.events[0].record.event {
            ReplayableEvent::SnapshotFinalized { epoch, snapshot_tape, .. } => {
                assert_eq!(*epoch, snapshot_epoch);
                assert_eq!(*snapshot_tape, expected_tape);
            }
            other => panic!("expected SnapshotFinalized, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_finalization_buckets_by_current_epoch() {
        let old_epoch = EpochNumber(7);
        let new_epoch = EpochNumber(8);
        let snapshot_epoch = old_epoch;

        let captured = capture(
            old_epoch,
            SlotNumber(42),
            vec![
                advance_epoch_instruction(old_epoch, new_epoch),
                finalize_snapshot_instruction(snapshot_epoch),
            ],
        );

        assert_eq!(captured.next_epoch, new_epoch);
        assert_eq!(captured.events.len(), 2);
        assert_eq!(captured.events[0].epoch, new_epoch);
        assert_eq!(captured.events[1].epoch, new_epoch);

        match &captured.events[1].record.event {
            ReplayableEvent::SnapshotFinalized { epoch, .. } => {
                assert_eq!(*epoch, snapshot_epoch);
            }
            other => panic!("expected SnapshotFinalized, got {other:?}"),
        }
    }
}

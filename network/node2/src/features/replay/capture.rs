use tape_blocks::ParsedInstruction;
use tape_core::snapshot::ReplayableEvent;
use tape_core::types::EpochNumber;

use crate::core::error::NodeError;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::types::ReplayBatch;

pub struct CapturedEvent {
    pub epoch: EpochNumber,
    pub event: ReplayableEvent,
}

pub struct CaptureOutput {
    pub next_epoch: EpochNumber,
    pub events: Vec<CapturedEvent>,
}

impl CaptureOutput {
    pub fn into_batch(self, slot: tape_core::types::SlotNumber) -> ReplayBatch {
        ReplayBatch {
            slot,
            events: self.events.into_iter().map(|entry| entry.event).collect(),
        }
    }
}

pub fn capture_block(
    initial_epoch: EpochNumber,
    block: &ParsedBlock,
) -> Result<CaptureOutput, NodeError> {
    let mut current_epoch = initial_epoch;
    let mut events = Vec::new();

    for instruction in &block.instructions {
        let Some(captured) = capture_instruction(&mut current_epoch, instruction)? else {
            continue;
        };
        events.push(captured);
    }

    Ok(CaptureOutput {
        next_epoch: current_epoch,
        events,
    })
}

fn capture_instruction(
    current_epoch: &mut EpochNumber,
    instruction: &ParsedInstruction,
) -> Result<Option<CapturedEvent>, NodeError> {
    let captured = match instruction {
        ParsedInstruction::AdvanceEpoch { event } => {
            *current_epoch = event.new_epoch;

            CapturedEvent {
                epoch: event.new_epoch,
                event: ReplayableEvent::AdvanceEpoch {
                    old_epoch: event.old_epoch,
                    new_epoch: event.new_epoch,
                },
            }
        }
        ParsedInstruction::SyncEpoch { event } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::SyncEpoch {
                node: event.node.to_bytes(),
                node_id: event.id,
                epoch: event.epoch,
                spools_hash: event.spools_hash,
            },
        },
        ParsedInstruction::RegisterTrack { track, event, .. } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::RegisterTrack {
                track: track.to_bytes(),
                event_data: bytemuck::bytes_of(event).to_vec(),
            },
        },
        ParsedInstruction::DeleteTrack { track, .. } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::DeleteTrack {
                track: track.to_bytes(),
                epoch: *current_epoch,
            },
        },
        ParsedInstruction::CertifyTrack { track, event } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::CertifyTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        },
        ParsedInstruction::InvalidateTrack { track, event } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::InvalidateTrack {
                track: track.to_bytes(),
                epoch: event.epoch,
            },
        },
        ParsedInstruction::ReserveTape { tape, event, .. } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::ReserveTape {
                tape: tape.to_bytes(),
                authority: event.authority.to_bytes(),
                active_epoch: event.active_epoch,
                expiry_epoch: event.expiry_epoch,
            },
        },
        ParsedInstruction::DestroyTape { tape, .. } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::DestroyTape {
                tape: tape.to_bytes(),
                epoch: *current_epoch,
            },
        },
        ParsedInstruction::RegisterNode {
            authority,
            node,
            ..
        } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::RegisterNode {
                authority: authority.to_bytes(),
                node: node.to_bytes(),
            },
        },
        ParsedInstruction::JoinNetwork { node, .. } => CapturedEvent {
            epoch: *current_epoch,
            event: ReplayableEvent::JoinNetwork {
                node: node.to_bytes(),
            },
        },
        ParsedInstruction::AdvancePool { .. } => return Ok(None),
    };

    Ok(Some(captured))
}

#[cfg(test)]
mod tests {
    use solana_sdk::pubkey::Pubkey;
    use tape_api::event::{
        EpochAdvanced, TapeReserved, TrackCertified, TrackRegistered,
    };
    use tape_blocks::ParsedInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::snapshot::ReplayableEvent;
    use tape_core::types::{EpochNumber, SlotNumber, StorageUnits};
    use tape_crypto::Hash;

    use super::capture_block;
    use crate::features::block::ingestor::ParsedBlock;

    fn register_track_instruction(track: Pubkey, tape: Pubkey, epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::RegisterTrack {
            owner: Pubkey::new_unique(),
            track,
            key: Hash::new_unique(),
            root: Hash::new_unique(),
            commitment: Hash::new_unique(),
            size: StorageUnits::mb(1),
            event: TrackRegistered {
                track,
                tape,
                key: Hash::new_unique(),
                size: StorageUnits::mb(1),
                commitment: Hash::new_unique(),
                epoch,
                profile: EncodingProfile::default(),
                spool_group: 3u64.to_le_bytes(),
                stripe_size: 64u64.to_le_bytes(),
                stripe_count: 2u64.to_le_bytes(),
                leaves: [Hash::default(); SPOOL_GROUP_SIZE],
            },
        }
    }

    fn certify_track_instruction(track: Pubkey, epoch: EpochNumber) -> ParsedInstruction {
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

    fn reserve_tape_instruction(tape: Pubkey, active_epoch: EpochNumber, expiry_epoch: EpochNumber) -> ParsedInstruction {
        ParsedInstruction::ReserveTape {
            owner: Pubkey::new_unique(),
            tape,
            event: TapeReserved {
                tape,
                authority: Pubkey::new_unique(),
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
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = ParsedBlock {
            slot: SlotNumber(42),
            instructions: vec![
                register_track_instruction(track, tape, EpochNumber(7)),
                certify_track_instruction(track, EpochNumber(8)),
                reserve_tape_instruction(tape, EpochNumber(7), EpochNumber(12)),
            ],
        };

        let captured = capture_block(EpochNumber(7), &block).unwrap();
        let batch = captured.into_batch(block.slot);

        assert_eq!(batch.events.len(), 3);
        assert!(matches!(
            batch.events[0],
            ReplayableEvent::RegisterTrack { .. }
        ));
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
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let block = ParsedBlock {
            slot: SlotNumber(100),
            instructions: vec![
                register_track_instruction(track, tape, EpochNumber(4)),
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
}

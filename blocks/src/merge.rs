//! Merge instructions with their corresponding events.

use crate::error::ParseError;
use crate::event::TapedriveEvent;
use crate::instruction::{ParsedInstruction, RawInstruction};
use std::collections::VecDeque;

/// Merge raw instructions with their corresponding events.
///
/// This function matches events to instructions based on their order
/// in the transaction. Some instructions require events (AdvanceEpoch,
/// CertifyTrack, SyncEpoch), while others have optional events.
///
/// # Arguments
/// * `instructions` - Raw instructions parsed from the transaction
/// * `events` - Events parsed from transaction logs
///
/// # Returns
/// * `Ok(Vec<ParsedInstruction>)` - Instructions with events merged in
/// * `Err(ParseError::EventMismatch)` - Required event was missing
///
/// # Example
/// ```ignore
/// let parsed = tape_blocks::parse(&block)?;
/// let merged = tape_blocks::merge(parsed.raw_instructions, parsed.events)?;
/// ```
pub fn merge(
    instructions: Vec<RawInstruction>,
    events: Vec<TapedriveEvent>,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut result = Vec::new();
    let mut events: VecDeque<TapedriveEvent> = events.into();

    for ix in instructions {
        let parsed = match ix {
            RawInstruction::AdvanceEpoch => {
                // AdvanceEpoch always has an event
                let event = match events.pop_front() {
                    Some(TapedriveEvent::EpochAdvanced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected EpochAdvanced event")),
                };
                ParsedInstruction::AdvanceEpoch { event }
            }

            RawInstruction::SyncEpoch => {
                // SyncEpoch always emits NodeSynced event
                let event = match events.pop_front() {
                    Some(TapedriveEvent::NodeSynced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected NodeSynced event")),
                };
                ParsedInstruction::SyncEpoch { event }
            }

            RawInstruction::RegisterTrack {
                owner,
                track,
                key,
                root,
                commitment,
                size,
            } => {
                let event = match events.front() {
                    Some(TapedriveEvent::TrackRegistered(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::RegisterTrack {
                    owner,
                    track,
                    key,
                    root,
                    commitment,
                    size,
                    event,
                }
            }

            RawInstruction::CertifyTrack { track } => {
                // CertifyTrack always has an event with the epoch
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackCertified(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected TrackCertified event")),
                };
                ParsedInstruction::CertifyTrack { track, event }
            }

            RawInstruction::DeleteTrack { owner, track } => {
                let event = match events.front() {
                    Some(TapedriveEvent::TrackDeleted(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::DeleteTrack {
                    owner,
                    track,
                    event,
                }
            }

            RawInstruction::InvalidateTrack { track } => {
                let event = match events.front() {
                    Some(TapedriveEvent::TrackInvalidated(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::InvalidateTrack {
                    track,
                    event,
                }
            }

            RawInstruction::ReserveTape { owner, tape } => {
                // TapeReserved event is now included
                let event = match events.front() {
                    Some(TapedriveEvent::TapeReserved(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::ReserveTape {
                    owner,
                    tape,
                    event,
                }
            }

            RawInstruction::DestroyTape { owner, tape } => {
                let event = match events.front() {
                    Some(TapedriveEvent::TapeDestroyed(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::DestroyTape {
                    owner,
                    tape,
                    event,
                }
            }

            RawInstruction::RegisterNode { authority, node } => {
                let event = match events.front() {
                    Some(TapedriveEvent::NodeRegistered(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::RegisterNode {
                    authority,
                    node,
                    event,
                }
            }

            RawInstruction::JoinNetwork { node } => {
                let event = match events.front() {
                    Some(TapedriveEvent::NodeJoinedCommittee(e)) => Some(*e),
                    _ => None,
                };
                if event.is_some() {
                    let _ = events.pop_front();
                }
                ParsedInstruction::JoinNetwork {
                    node,
                    event,
                }
            }
        };

        result.push(parsed);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use tape_api::event::{EpochAdvanced, NodeSynced, TrackCertified, TrackRegistered};
    use tape_core::prelude::*;
    use tape_crypto::Hash;

    #[test]
    fn test_merge_advance_epoch() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
            nonce: Hash::default(),
        };

        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![TapedriveEvent::EpochAdvanced(event)];

        let merged = merge(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.old_epoch, EpochNumber(5));
                assert_eq!(event.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }
    }

    #[test]
    fn test_merge_certify_track() {
        let track = Pubkey::new_unique();
        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let instructions = vec![RawInstruction::CertifyTrack { track }];
        let events = vec![TapedriveEvent::TrackCertified(event)];

        let merged = merge(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::CertifyTrack { track: t, event } => {
                assert_eq!(*t, track);
                assert_eq!(event.epoch, EpochNumber(10));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_merge_advance_epoch_missing_event() {
        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![]; // No events!

        let result = merge(instructions, events);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
        }
    }

    #[test]
    fn test_merge_certify_track_missing_event() {
        let track = Pubkey::new_unique();
        let instructions = vec![RawInstruction::CertifyTrack { track }];
        let events = vec![]; // No events!

        let result = merge(instructions, events);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
        }
    }


    #[test]
    fn test_merge_multiple_instructions() {
        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let epoch_event = EpochAdvanced {
            old_epoch: EpochNumber(1),
            new_epoch: EpochNumber(2),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
            nonce: Hash::default(),
        };

        let register_event = TrackRegistered {
            track: track1,
            tape: Pubkey::new_unique(),
            key: Hash::default(),
            size: StorageUnits(100),
            commitment: Hash::default(),
            epoch: EpochNumber(2),
            profile: tape_core::encoding::EncodingProfile::clay_default(),
            spool_group: 0u64.to_le_bytes(),
            stripe_size: 0u64.to_le_bytes(),
            stripe_count: 0u64.to_le_bytes(),
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
        };

        let certify_event = TrackCertified {
            track: track2,
            epoch: EpochNumber(2),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let instructions = vec![
            RawInstruction::AdvanceEpoch,
            RawInstruction::RegisterTrack {
                owner,
                track: track1,
                key: Hash::default(),
                root: Hash::default(),
                commitment: Hash::default(),
                size: StorageUnits(100),
            },
            RawInstruction::CertifyTrack { track: track2 },
        ];

        let events = vec![
            TapedriveEvent::EpochAdvanced(epoch_event),
            TapedriveEvent::TrackRegistered(register_event),
            TapedriveEvent::TrackCertified(certify_event),
        ];

        let merged = merge(instructions, events).unwrap();

        assert_eq!(merged.len(), 3);

        // Check AdvanceEpoch
        match &merged[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.new_epoch, EpochNumber(2));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }

        // Check RegisterTrack
        match &merged[1] {
            ParsedInstruction::RegisterTrack { track, event, .. } => {
                assert_eq!(*track, track1);
                assert!(event.is_some());
                assert_eq!(event.as_ref().unwrap().epoch, EpochNumber(2));
            }
            _ => panic!("Expected RegisterTrack"),
        }

        // Check CertifyTrack
        match &merged[2] {
            ParsedInstruction::CertifyTrack { track, event } => {
                assert_eq!(*track, track2);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_merge_optional_events() {
        // DeleteTrack, InvalidateTrack, etc. have optional events
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let instructions = vec![RawInstruction::DeleteTrack { owner, track }];
        let events = vec![]; // No event (optional)

        let merged = merge(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::DeleteTrack {
                track: t,
                event,
                ..
            } => {
                assert_eq!(*t, track);
                assert!(event.is_none()); // Event is optional
            }
            _ => panic!("Expected DeleteTrack"),
        }
    }

    #[test]
    fn test_merge_sync_epoch_with_event() {
        let node = Pubkey::new_unique();
        let instructions = vec![RawInstruction::SyncEpoch];
        let events = vec![TapedriveEvent::NodeSynced(NodeSynced {
            node,
            id: NodeId::new(1),
            epoch: EpochNumber(5),
            spools_hash: Hash::default(),
        })];

        let merged = merge(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::SyncEpoch { event } => {
                assert_eq!(event.node, node);
                assert_eq!(event.epoch, EpochNumber(5));
            }
            _ => panic!("Expected SyncEpoch"),
        }
    }

    #[test]
    fn test_merge_wrong_event_type() {
        // AdvanceEpoch instruction with TrackCertified event should fail
        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![TapedriveEvent::TrackCertified(TrackCertified {
            track: Pubkey::new_unique(),
            epoch: EpochNumber(1),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        })];

        let result = merge(instructions, events);
        assert!(result.is_err());
    }
}

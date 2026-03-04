//! Merge instructions with their corresponding events.

use crate::error::ParseError;
use crate::event::TapedriveEvent;
use crate::instruction::{ParsedInstruction, RawInstruction};
use std::collections::VecDeque;

/// Merge raw instructions with their corresponding events.
///
/// This function matches events to instructions based on their order
/// in the transaction. Some instructions require events (AdvanceEpoch,
/// CertifyTrack, SyncEpoch, RegisterTrack, DeleteTrack, InvalidateTrack,
/// ReserveTape, DestroyTape, RegisterNode, JoinNetwork).
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
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackRegistered(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TrackRegistered event",
                        ))
                    }
                };
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
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackDeleted(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TrackDeleted event",
                        ))
                    }
                };
                ParsedInstruction::DeleteTrack {
                    owner,
                    track,
                    event,
                }
            }

            RawInstruction::InvalidateTrack { track } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackInvalidated(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TrackInvalidated event",
                        ))
                    }
                };
                ParsedInstruction::InvalidateTrack {
                    track,
                    event,
                }
            }

            RawInstruction::ReserveTape { owner, tape } => {
                // TapeReserved event is now included
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TapeReserved(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TapeReserved event",
                        ))
                    }
                };
                ParsedInstruction::ReserveTape {
                    owner,
                    tape,
                    event,
                }
            }

            RawInstruction::DestroyTape { owner, tape } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TapeDestroyed(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TapeDestroyed event",
                        ))
                    }
                };
                ParsedInstruction::DestroyTape {
                    owner,
                    tape,
                    event,
                }
            }

            RawInstruction::RegisterNode { authority, node } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::NodeRegistered(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected NodeRegistered event",
                        ))
                    }
                };
                ParsedInstruction::RegisterNode {
                    authority,
                    node,
                    event,
                }
            }

            RawInstruction::JoinNetwork { node } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::NodeJoinedCommittee(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected NodeJoinedCommittee event",
                        ))
                    }
                };
                ParsedInstruction::JoinNetwork {
                    node,
                    event,
                }
            }

            RawInstruction::AdvancePool { node } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::PoolAdvanced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected PoolAdvanced event")),
                };
                ParsedInstruction::AdvancePool { node, event }
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
    use tape_api::event::{
        EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, TapeDestroyed, TapeReserved,
        TrackCertified, TrackDeleted, TrackInvalidated, TrackRegistered,
    };
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
            phase: 1, // Syncing
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
            phase: 1, // Syncing
        };

        let register_event = TrackRegistered {
            track: track1,
            tape: Pubkey::new_unique(),
            key: Hash::default(),
            size: StorageUnits::mb(100),
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
                size: StorageUnits::mb(100),
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
                assert_eq!(event.epoch, EpochNumber(2));
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
    fn test_merge_required_events_missing() {
        // DeleteTrack requires TrackDeleted event
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let instructions = vec![RawInstruction::DeleteTrack { owner, track }];
        let events = vec![]; // No event

        let result = merge(instructions, events);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
        }
    }

    fn required_events_missing_cases() -> Vec<RawInstruction> {
        vec![
            RawInstruction::RegisterTrack {
                owner: Pubkey::new_unique(),
                track: Pubkey::new_unique(),
                key: Hash::default(),
                root: Hash::default(),
                commitment: Hash::default(),
                size: StorageUnits::mb(1_024),
            },
            RawInstruction::DeleteTrack {
                owner: Pubkey::new_unique(),
                track: Pubkey::new_unique(),
            },
            RawInstruction::InvalidateTrack {
                track: Pubkey::new_unique(),
            },
            RawInstruction::ReserveTape {
                owner: Pubkey::new_unique(),
                tape: Pubkey::new_unique(),
            },
            RawInstruction::DestroyTape {
                owner: Pubkey::new_unique(),
                tape: Pubkey::new_unique(),
            },
            RawInstruction::RegisterNode {
                authority: Pubkey::new_unique(),
                node: Pubkey::new_unique(),
            },
            RawInstruction::JoinNetwork {
                node: Pubkey::new_unique(),
            },
        ]
    }

    fn required_event_mismatch_case() -> TapedriveEvent {
        TapedriveEvent::EpochAdvanced(EpochAdvanced {
            old_epoch: EpochNumber(1),
            new_epoch: EpochNumber(2),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
            nonce: Hash::default(),
            phase: 1, // Syncing
        })
    }

    #[test]
    fn test_merge_required_events_missing_all() {
        for raw in required_events_missing_cases() {
            let result = merge(vec![raw], vec![]);
            assert!(
                result.is_err(),
                "expected error when required event was missing"
            );
            match result {
                Err(ParseError::EventMismatch(_)) => {}
                _ => panic!("Expected EventMismatch error"),
            }
        }
    }

    fn required_events_success_cases() -> Vec<(RawInstruction, TapedriveEvent)> {
        let register_track = Pubkey::new_unique();
        let register_tape = Pubkey::new_unique();
        let delete_track = Pubkey::new_unique();
        let delete_tape = Pubkey::new_unique();
        let invalid_track = Pubkey::new_unique();
        let reserve_tape = Pubkey::new_unique();
        let destroy_tape = Pubkey::new_unique();
        let register_node = Pubkey::new_unique();
        let join_node = Pubkey::new_unique();

        vec![
            (
                RawInstruction::RegisterTrack {
                    owner: Pubkey::new_unique(),
                    track: register_track,
                    key: Hash::default(),
                    root: Hash::default(),
                    commitment: Hash::default(),
                    size: StorageUnits::mb(1_024),
                },
                TapedriveEvent::TrackRegistered(TrackRegistered {
                    track: register_track,
                    tape: register_tape,
                    key: Hash::default(),
                    size: StorageUnits::mb(1_024),
                    commitment: Hash::default(),
                    epoch: EpochNumber(2),
                    profile: tape_core::encoding::EncodingProfile::basic_default(),
                    spool_group: 0u64.to_le_bytes(),
                    stripe_size: 0u64.to_le_bytes(),
                    stripe_count: 0u64.to_le_bytes(),
                    leaves: [Hash::default(); SPOOL_GROUP_SIZE],
                }),
            ),
            (
                RawInstruction::DeleteTrack {
                    owner: Pubkey::new_unique(),
                    track: delete_track,
                },
                TapedriveEvent::TrackDeleted(TrackDeleted {
                    track: delete_track,
                    tape: delete_tape,
                    key: Hash::default(),
                    size: StorageUnits::mb(1_024),
                }),
            ),
            (
                RawInstruction::InvalidateTrack {
                    track: invalid_track,
                },
                TapedriveEvent::TrackInvalidated(TrackInvalidated {
                    track: invalid_track,
                    epoch: EpochNumber(3),
                }),
            ),
            (
                RawInstruction::ReserveTape {
                    owner: Pubkey::new_unique(),
                    tape: reserve_tape,
                },
                TapedriveEvent::TapeReserved(TapeReserved {
                    tape: reserve_tape,
                    authority: Pubkey::new_unique(),
                    capacity: StorageUnits::mb(10_000),
                    active_epoch: EpochNumber(1),
                    expiry_epoch: EpochNumber(10),
                    cost: [0; 8],
                }),
            ),
            (
                RawInstruction::DestroyTape {
                    owner: Pubkey::new_unique(),
                    tape: destroy_tape,
                },
                TapedriveEvent::TapeDestroyed(TapeDestroyed {
                    tape: destroy_tape,
                    authority: Pubkey::new_unique(),
                }),
            ),
            (
                RawInstruction::RegisterNode {
                    authority: Pubkey::new_unique(),
                    node: register_node,
                },
                TapedriveEvent::NodeRegistered(NodeRegistered {
                    node: register_node,
                    id: NodeId::new(1),
                    authority: Pubkey::new_unique(),
                    epoch: EpochNumber(0),
                }),
            ),
            (
                RawInstruction::JoinNetwork {
                    node: join_node,
                },
                TapedriveEvent::NodeJoinedCommittee(NodeJoinedCommittee {
                    node: join_node,
                    id: NodeId::new(2),
                    stake: [0; 8],
                    activation_epoch: EpochNumber(1),
                }),
            ),
        ]
    }

    #[test]
    fn test_merge_required_events_success_all() {
        for (raw, event) in required_events_success_cases() {
            let merged = merge(vec![raw], vec![event]).unwrap();
            assert_eq!(merged.len(), 1);

            match &merged[0] {
                ParsedInstruction::RegisterTrack { .. }
                | ParsedInstruction::DeleteTrack { .. }
                | ParsedInstruction::InvalidateTrack { .. }
                | ParsedInstruction::ReserveTape { .. }
                | ParsedInstruction::DestroyTape { .. }
                | ParsedInstruction::RegisterNode { .. }
                | ParsedInstruction::JoinNetwork { .. } => {}
                _ => panic!("expected one of the required instruction variants"),
            }
        }
    }

    #[test]
    fn test_merge_required_events_wrong_event_type_all() {
        for raw in required_events_missing_cases() {
            let result = merge(vec![raw], vec![required_event_mismatch_case()]);
            assert!(
                result.is_err(),
                "expected error when event type mismatched required event"
            );
            match result {
                Err(ParseError::EventMismatch(_)) => {}
                _ => panic!("Expected EventMismatch error"),
            }
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
            phase: 1, // Syncing
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

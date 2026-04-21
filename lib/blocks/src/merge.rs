//! Merge instructions with their corresponding events.

use crate::error::ParseError;
use crate::event::TapedriveEvent;
use crate::instruction::{ParsedInstruction, RawInstruction};
use std::collections::VecDeque;

/// Merge raw instructions with their corresponding events.
///
/// This function matches events to instructions based on their order
/// in the transaction. Some instructions require events (AdvanceEpoch,
/// CertifyTrack, SyncEpoch, TrackWrite, DeleteTrack, InvalidateTrack,
/// ReserveTape, DestroyTape, RegisterNode, JoinNetwork, CloseVote).
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

            RawInstruction::ReserveSnapshot => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::SnapshotReserved(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch("expected SnapshotReserved event"))
                    }
                };
                ParsedInstruction::ReserveSnapshot { event }
            }

            RawInstruction::WriteSnapshot {
                group,
                chunk,
                blob,
            } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::SnapshotWritten(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch("expected SnapshotWritten event"))
                    }
                };
                ParsedInstruction::WriteSnapshot {
                    group,
                    chunk,
                    blob,
                    event,
                }
            }

            RawInstruction::SignSnapshot => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::SnapshotSigned(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch("expected SnapshotSigned event"))
                    }
                };
                ParsedInstruction::SignSnapshot { event }
            }

            RawInstruction::CloseVote { vote } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::VoteClosed(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected VoteClosed event")),
                };
                ParsedInstruction::CloseVote { vote, event }
            }

            RawInstruction::TrackWrite {
                authority,
                key,
                value,
            } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackWritten(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected TrackWritten event",
                        ))
                    }
                };
                ParsedInstruction::TrackWrite {
                    authority,
                    track: event.track,
                    key,
                    value,
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
    use bytemuck::Zeroable;
    use tape_api::event::{
        EpochAdvanced, NodeJoinedCommittee, NodeRegistered, NodeSynced, SnapshotReserved,
        SnapshotSigned, SnapshotWritten, TapeDestroyed, TapeReserved, TrackCertified,
        TrackDeleted, TrackInvalidated, TrackWritten, VoteClosed,
    };
    use tape_core::bls::BlsPubkey;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::prelude::*;
    use tape_core::spooler::SpoolGroup;
    use tape_core::types::ChunkNumber;
    use tape_core::system::NodePreferences;
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::data::TrackData;
    use tape_core::types::{StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::address::Address;
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
    fn test_merge_snapshot_events() {
        let reserved = SnapshotReserved {
            epoch: EpochNumber(7),
        };
        let written = SnapshotWritten {
            epoch: EpochNumber(7),
            group: SpoolGroup(3),
            track: Address::new_unique(),
            track_number: TrackNumber(9),
            track_hash: Hash::from([0x44; 32]),
        };
        let signed = SnapshotSigned {
            epoch: EpochNumber(7),
            group: SpoolGroup(3),
            state: 0,
        };

        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_024),
            commitment: Hash::default(),
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(1),
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
        };

        let merged = merge(
            vec![
                RawInstruction::ReserveSnapshot,
                RawInstruction::WriteSnapshot {
                    group: SpoolGroup(3),
                    chunk: ChunkNumber(0),
                    blob: blob.clone(),
                },
                RawInstruction::SignSnapshot,
            ],
            vec![
                TapedriveEvent::SnapshotReserved(reserved),
                TapedriveEvent::SnapshotWritten(written),
                TapedriveEvent::SnapshotSigned(signed),
            ],
        )
        .unwrap();

        assert_eq!(merged.len(), 3);
        match &merged[0] {
            ParsedInstruction::ReserveSnapshot { event } => {
                assert_eq!(event.epoch, reserved.epoch);
            }
            _ => panic!("Expected ReserveSnapshot"),
        }
        match &merged[1] {
            ParsedInstruction::WriteSnapshot {
                group,
                chunk,
                blob: parsed_blob,
                event,
            } => {
                assert_eq!(*group, SpoolGroup(3));
                assert_eq!(*chunk, ChunkNumber(0));
                assert_eq!(*parsed_blob, blob);
                assert_eq!(event.epoch, written.epoch);
                assert_eq!(event.track_hash, written.track_hash);
            }
            _ => panic!("Expected WriteSnapshot"),
        }
        match &merged[2] {
            ParsedInstruction::SignSnapshot { event } => {
                assert_eq!(event.epoch, signed.epoch);
                assert_eq!(event.group, signed.group);
            }
            _ => panic!("Expected SignSnapshot"),
        }
    }

    #[test]
    fn test_merge_certify_track() {
        let track = Address::new_unique();
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
        let track = Address::new_unique();
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
        let track1 = Address::new_unique();
        let track2 = Address::new_unique();
        let owner = Address::new_unique();

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

        let register_event = TrackWritten {
            epoch: EpochNumber(2),
            track: track1,
            tape: Address::new_unique(),
            spool_group: SpoolGroup(0),
            track_number: TrackNumber(0),
            track_hash: Hash::default(),
        };

        let certify_event = TrackCertified {
            track: track2,
            epoch: EpochNumber(2),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let instructions = vec![
            RawInstruction::AdvanceEpoch,
            RawInstruction::TrackWrite {
                authority: owner,
                key: Hash::default(),
                value: TrackData::Blob(BlobInfo {
                    size: StorageUnits::mb(100),
                    commitment: Hash::default(),
                    profile: EncodingProfile::default(),
                    stripe_size: StorageUnits::from_bytes(64),
                    stripe_count: StripeCount(1),
                    leaves: [Hash::default(); SPOOL_GROUP_SIZE],
                }),
            },
            RawInstruction::CertifyTrack { track: track2 },
        ];

        let events = vec![
            TapedriveEvent::EpochAdvanced(epoch_event),
            TapedriveEvent::TrackWritten(register_event),
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

        // Check TrackWrite
        match &merged[1] {
            ParsedInstruction::TrackWrite { track, event, .. } => {
                assert_eq!(*track, track1);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected TrackWrite"),
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
        let track = Address::new_unique();
        let owner = Address::new_unique();

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
            RawInstruction::TrackWrite {
                authority: Address::new_unique(),
                key: Hash::default(),
                value: TrackData::Blob(BlobInfo {
                    size: StorageUnits::mb(1_024),
                    commitment: Hash::default(),
                    profile: EncodingProfile::default(),
                    stripe_size: StorageUnits::from_bytes(64),
                    stripe_count: StripeCount(1),
                    leaves: [Hash::default(); SPOOL_GROUP_SIZE],
                }),
            },
            RawInstruction::DeleteTrack {
                owner: Address::new_unique(),
                track: Address::new_unique(),
            },
            RawInstruction::InvalidateTrack {
                track: Address::new_unique(),
            },
            RawInstruction::ReserveTape {
                owner: Address::new_unique(),
                tape: Address::new_unique(),
            },
            RawInstruction::DestroyTape {
                owner: Address::new_unique(),
                tape: Address::new_unique(),
            },
            RawInstruction::RegisterNode {
                authority: Address::new_unique(),
                node: Address::new_unique(),
            },
            RawInstruction::JoinNetwork {
                node: Address::new_unique(),
            },
            RawInstruction::CloseVote {
                vote: Address::new_unique(),
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
        let register_track = Address::new_unique();
        let register_tape = Address::new_unique();
        let delete_track = Address::new_unique();
        let delete_tape = Address::new_unique();
        let invalid_track = Address::new_unique();
        let reserve_tape = Address::new_unique();
        let destroy_tape = Address::new_unique();
        let register_node = Address::new_unique();
        let join_node = Address::new_unique();
        let vote = Address::new_unique();

        vec![
            (
                RawInstruction::TrackWrite {
                    authority: Address::new_unique(),
                    key: Hash::default(),
                    value: TrackData::Blob(BlobInfo {
                        size: StorageUnits::mb(1_024),
                        commitment: Hash::default(),
                        profile: EncodingProfile::default(),
                        stripe_size: StorageUnits::from_bytes(64),
                        stripe_count: StripeCount(1),
                        leaves: [Hash::default(); SPOOL_GROUP_SIZE],
                    }),
                },
                TapedriveEvent::TrackWritten(TrackWritten {
                    epoch: EpochNumber(2),
                    track: register_track,
                    tape: register_tape,
                    spool_group: SpoolGroup(0),
                    track_number: TrackNumber(0),
                    track_hash: Hash::default(),
                }),
            ),
            (
                RawInstruction::DeleteTrack {
                    owner: Address::new_unique(),
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
                    owner: Address::new_unique(),
                    tape: reserve_tape,
                },
                TapedriveEvent::TapeReserved(TapeReserved {
                    tape: reserve_tape,
                    authority: Address::new_unique(),
                    capacity: StorageUnits::mb(10_000),
                    active_epoch: EpochNumber(1),
                    expiry_epoch: EpochNumber(10),
                    cost: [0; 8],
                }),
            ),
            (
                RawInstruction::DestroyTape {
                    owner: Address::new_unique(),
                    tape: destroy_tape,
                },
                TapedriveEvent::TapeDestroyed(TapeDestroyed {
                    tape: destroy_tape,
                    authority: Address::new_unique(),
                }),
            ),
            (
                RawInstruction::RegisterNode {
                    authority: Address::new_unique(),
                    node: register_node,
                },
                TapedriveEvent::NodeRegistered(NodeRegistered {
                    node: register_node,
                    id: NodeId::new(1),
                    authority: Address::new_unique(),
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
                    key: BlsPubkey::new_unique(),
                    blacklist: StorageUnits::default(),
                    preferences: NodePreferences::zeroed(),
                    activation_epoch: EpochNumber(1),
                }),
            ),
            (
                RawInstruction::CloseVote { vote },
                TapedriveEvent::VoteClosed(VoteClosed {
                    epoch: EpochNumber(2),
                    kind: VoteKind::Snapshot as u64,
                    vote,
                    registered_by: NodeId::new(3),
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
                ParsedInstruction::TrackWrite { .. }
                | ParsedInstruction::DeleteTrack { .. }
                | ParsedInstruction::InvalidateTrack { .. }
                | ParsedInstruction::ReserveTape { .. }
                | ParsedInstruction::DestroyTape { .. }
                | ParsedInstruction::RegisterNode { .. }
                | ParsedInstruction::JoinNetwork { .. }
                | ParsedInstruction::CloseVote { .. } => {}
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
        let node = Address::new_unique();
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
            track: Address::new_unique(),
            epoch: EpochNumber(1),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        })];

        let result = merge(instructions, events);
        assert!(result.is_err());
    }
}

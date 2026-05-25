//! Merge instructions with their corresponding events.

use crate::error::ParseError;
use crate::event::TapedriveEvent;
use crate::instruction::{ParsedInstruction, RawInstruction};
use std::collections::VecDeque;
use tape_core::system::VoteKind;

/// Merge raw instructions with their corresponding events.
///
/// This function matches events to instructions based on their order
/// in the transaction.
pub fn merge(
    instructions: Vec<RawInstruction>,
    events: Vec<TapedriveEvent>,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut result = Vec::new();
    let mut events: VecDeque<TapedriveEvent> = events.into();

    for ix in instructions {
        let parsed = match ix {
            RawInstruction::CreateEpoch { epoch } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::EpochCreated(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected EpochCreated event")),
                };
                if event.epoch != epoch {
                    return Err(ParseError::EventMismatch("unexpected EpochCreated event"));
                }
                ParsedInstruction::CreateEpoch { epoch, event }
            }

            RawInstruction::CreateCommittee { epoch } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::CommitteeCreated(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected CommitteeCreated event")),
                };
                if event.epoch != epoch {
                    return Err(ParseError::EventMismatch("unexpected CommitteeCreated event"));
                }
                ParsedInstruction::CreateCommittee { epoch, event }
            }

            RawInstruction::ResizeCommittee => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::CommitteeResized(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected CommitteeResized event")),
                };
                ParsedInstruction::ResizeCommittee { event }
            }

            RawInstruction::ResizePeerSet => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::PeerSetResized(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected PeerSetResized event")),
                };
                ParsedInstruction::ResizePeerSet { event }
            }

            RawInstruction::CommitEpoch => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::EpochCommitted(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected EpochCommitted event")),
                };
                ParsedInstruction::CommitEpoch { event }
            }

            RawInstruction::AdvanceEpoch => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::EpochAdvanced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected EpochAdvanced event")),
                };
                ParsedInstruction::AdvanceEpoch { event }
            }

            RawInstruction::SyncSpool { node, spool } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::SpoolSynced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected SpoolSynced event")),
                };
                ParsedInstruction::SyncSpool { node, spool, event }
            }

            RawInstruction::ProposeSnapshot { hash } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::VoteProposed(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected VoteProposed event")),
                };
                if event.kind != VoteKind::Snapshot as u64 || event.hash != hash {
                    return Err(ParseError::EventMismatch("unexpected VoteProposed event"));
                }
                ParsedInstruction::ProposeSnapshot { hash, event }
            }

            RawInstruction::VoteSnapshot { hash, group } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::VoteRecorded(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected VoteRecorded event")),
                };
                if event.kind != VoteKind::Snapshot as u64
                    || event.hash != hash
                    || event.group != group
                {
                    return Err(ParseError::EventMismatch("unexpected VoteRecorded event"));
                }
                ParsedInstruction::VoteSnapshot { hash, group, event }
            }

            RawInstruction::FinalizeSnapshot { epoch } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::SnapshotFinalized(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected SnapshotFinalized event",
                        ))
                    }
                };
                if event.epoch != epoch {
                    return Err(ParseError::EventMismatch("unexpected SnapshotFinalized event"));
                }
                ParsedInstruction::FinalizeSnapshot { epoch, event }
            }

            RawInstruction::ProposeAssignment { hash } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::VoteProposed(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected VoteProposed event")),
                };
                if event.kind != VoteKind::Assignment as u64 || event.hash != hash {
                    return Err(ParseError::EventMismatch("unexpected VoteProposed event"));
                }
                ParsedInstruction::ProposeAssignment { hash, event }
            }

            RawInstruction::VoteAssignment { hash, group } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::VoteRecorded(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected VoteRecorded event")),
                };
                if event.kind != VoteKind::Assignment as u64
                    || event.hash != hash
                    || event.group != group
                {
                    return Err(ParseError::EventMismatch("unexpected VoteRecorded event"));
                }
                ParsedInstruction::VoteAssignment { hash, group, event }
            }

            RawInstruction::FinalizeGroup { epoch, group } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::AssignmentFinalized(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected AssignmentFinalized event",
                        ))
                    }
                };
                if event.epoch != epoch || event.group != group {
                    return Err(ParseError::EventMismatch(
                        "unexpected AssignmentFinalized event",
                    ));
                }
                ParsedInstruction::FinalizeGroup { epoch, group, event }
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

            RawInstruction::JoinCommittee { node } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::NodeJoinedCommittee(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected NodeJoinedCommittee event",
                        ))
                    }
                };
                ParsedInstruction::JoinCommittee {
                    node,
                    event,
                }
            }

            RawInstruction::AddToBlacklist { node, entry } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackWritten(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected TrackWritten event")),
                };
                ParsedInstruction::AddToBlacklist { node, entry, event }
            }

            RawInstruction::RemoveFromBlacklist { node, track } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::TrackDeleted(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected TrackDeleted event")),
                };
                ParsedInstruction::RemoveFromBlacklist { node, track, event }
            }

            RawInstruction::AdvancePool { node } => {
                let track_event = match events.pop_front() {
                    Some(TapedriveEvent::TrackWritten(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected TrackWritten event")),
                };
                let event = match events.pop_front() {
                    Some(TapedriveEvent::PoolAdvanced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected PoolAdvanced event")),
                };
                if event.node != node
                    || event.span.node != node
                    || event.epoch.checked_next() != Some(event.span.end_epoch)
                {
                    return Err(ParseError::EventMismatch("unexpected PoolAdvanced event"));
                }
                let span = event.span;
                ParsedInstruction::AdvancePool {
                    node,
                    span,
                    track_event,
                    event,
                }
            }

            RawInstruction::StakeWithPool {
                authority,
                pool,
                stake,
                amount,
            } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::StakeDeposited(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected StakeDeposited event")),
                };
                if event.authority != authority
                    || event.pool != pool
                    || event.stake != stake
                    || event.amount != amount
                {
                    return Err(ParseError::EventMismatch("unexpected StakeDeposited event"));
                }
                ParsedInstruction::StakeWithPool {
                    authority,
                    pool,
                    stake,
                    event,
                }
            }

            RawInstruction::RequestStakeUnlock {
                authority,
                pool,
                stake,
            } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::StakeUnlockRequested(e)) => e,
                    _ => {
                        return Err(ParseError::EventMismatch(
                            "expected StakeUnlockRequested event",
                        ))
                    }
                };
                if event.authority != authority || event.pool != pool || event.stake != stake {
                    return Err(ParseError::EventMismatch(
                        "unexpected StakeUnlockRequested event",
                    ));
                }
                ParsedInstruction::RequestStakeUnlock {
                    authority,
                    pool,
                    stake,
                    event,
                }
            }

            RawInstruction::UnstakeFromPool {
                authority,
                pool,
                stake,
            } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::StakeWithdrawn(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected StakeWithdrawn event")),
                };
                if event.authority != authority || event.pool != pool || event.stake != stake {
                    return Err(ParseError::EventMismatch("unexpected StakeWithdrawn event"));
                }
                ParsedInstruction::UnstakeFromPool {
                    authority,
                    pool,
                    stake,
                    event,
                }
            }

            RawInstruction::ClaimCommission { authority, node } => {
                let event = match events.pop_front() {
                    Some(TapedriveEvent::CommissionClaimed(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected CommissionClaimed event")),
                };
                if event.authority != authority || event.node != node {
                    return Err(ParseError::EventMismatch("unexpected CommissionClaimed event"));
                }
                ParsedInstruction::ClaimCommission {
                    authority,
                    node,
                    event,
                }
            }
        };

        result.push(parsed);
    }

    if !events.is_empty() {
        return Err(ParseError::EventMismatch("unmatched event"));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_api::event::{
        AssignmentFinalized, EpochAdvanced, EpochCommitted, NodeJoinedCommittee,
        NodeRegistered, PoolAdvanced, SnapshotFinalized, SpoolSynced, TapeDestroyed,
        TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated, TrackWritten,
        VoteProposed, VoteRecorded,
    };
    use tape_core::bls::BlsPubkey;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::prelude::*;
    use tape_core::spooler::GroupIndex;
    use tape_core::staking::RateSpan;
    use tape_core::system::{EpochPhase, ExchangeRate, NodePreferences, VoteKind};
    use tape_core::track::data::TrackData;
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;

    fn epoch_advanced_event() -> EpochAdvanced {
        EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            total_stake: [0; 8],
            committee_count: [0; 8],
            preferences: NodePreferences::zeroed(),
            nonce: Hash::default(),
        }
    }

    #[test]
    fn merge_commit_epoch() {
        let event = EpochCommitted {
            epoch: EpochNumber(5),
            next_nonce: Hash::from([0x42; 32]),
            preferences: NodePreferences::zeroed(),
        };

        let merged = merge(
            vec![RawInstruction::CommitEpoch],
            vec![TapedriveEvent::EpochCommitted(event)],
        )
        .unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::CommitEpoch { event } => {
                assert_eq!(event.epoch, EpochNumber(5));
            }
            _ => panic!("Expected CommitEpoch"),
        }
    }

    #[test]
    fn merge_advance_epoch() {
        let event = epoch_advanced_event();

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
    fn merge_snapshot_events() {
        let voted = VoteRecorded {
            kind: VoteKind::Snapshot as u64,
            vote: Address::new_unique(),
            voting_epoch: EpochNumber(8),
            target_epoch: EpochNumber(7),
            hash: Hash::from([0x55; 32]),
            group: GroupIndex(3),
            signer_count: [14, 0, 0, 0, 0, 0, 0, 0],
            signed_groups: 1u64.to_le_bytes(),
            total_groups: 5u64.to_le_bytes(),
        };

        let merged = merge(
            vec![
                RawInstruction::VoteSnapshot {
                    hash: Hash::from([0x55; 32]),
                    group: GroupIndex(3),
                },
            ],
            vec![TapedriveEvent::VoteRecorded(voted)],
        )
        .unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::VoteSnapshot { group, event, .. } => {
                assert_eq!(*group, GroupIndex(3));
                assert_eq!(event.target_epoch, voted.target_epoch);
                assert_eq!(event.group, voted.group);
            }
            _ => panic!("Expected VoteSnapshot"),
        }
    }

    #[test]
    fn merge_propose_and_finalize_events() {
        let snapshot_hash = Hash::from([0x55; 32]);
        let snapshot_vote = VoteProposed {
            kind: VoteKind::Snapshot as u64,
            vote: Address::new_unique(),
            voting_epoch: EpochNumber(8),
            target_epoch: EpochNumber(7),
            hash: snapshot_hash,
            total_groups: 5u64.to_le_bytes(),
        };
        let snapshot_finalized = SnapshotFinalized {
            epoch: EpochNumber(7),
            hash: snapshot_hash,
            snapshot_tape: Address::new_unique(),
        };
        let assignment_hash = Hash::from([0x66; 32]);
        let assignment_vote = VoteProposed {
            kind: VoteKind::Assignment as u64,
            vote: Address::new_unique(),
            voting_epoch: EpochNumber(8),
            target_epoch: EpochNumber(9),
            hash: assignment_hash,
            total_groups: 5u64.to_le_bytes(),
        };
        let group_finalized = AssignmentFinalized {
            epoch: EpochNumber(9),
            hash: assignment_hash,
            group: GroupIndex(2),
            group_account: Address::new_unique(),
            size: StorageUnits::mb(10),
            total_groups: 1u64.to_le_bytes(),
            total_assigned: StorageUnits::mb(200),
        };

        let merged = merge(
            vec![
                RawInstruction::ProposeSnapshot {
                    hash: snapshot_hash,
                },
                RawInstruction::FinalizeSnapshot {
                    epoch: EpochNumber(7),
                },
                RawInstruction::ProposeAssignment {
                    hash: assignment_hash,
                },
                RawInstruction::FinalizeGroup {
                    epoch: EpochNumber(9),
                    group: GroupIndex(2),
                },
            ],
            vec![
                TapedriveEvent::VoteProposed(snapshot_vote),
                TapedriveEvent::SnapshotFinalized(snapshot_finalized),
                TapedriveEvent::VoteProposed(assignment_vote),
                TapedriveEvent::AssignmentFinalized(group_finalized),
            ],
        )
        .unwrap();

        assert_eq!(merged.len(), 4);
        assert!(matches!(
            &merged[0],
            ParsedInstruction::ProposeSnapshot { event, .. }
                if event.kind == VoteKind::Snapshot as u64
        ));
        assert!(matches!(
            &merged[1],
            ParsedInstruction::FinalizeSnapshot { event, .. }
                if event.snapshot_tape == snapshot_finalized.snapshot_tape
        ));
        assert!(matches!(
            &merged[2],
            ParsedInstruction::ProposeAssignment { event, .. }
                if event.kind == VoteKind::Assignment as u64
        ));
        assert!(matches!(
            &merged[3],
            ParsedInstruction::FinalizeGroup { event, .. }
                if event.group_account == group_finalized.group_account
        ));
    }

    #[test]
    fn merge_certify_track() {
        let track = Address::new_unique();
        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let merged = merge(
            vec![RawInstruction::CertifyTrack { track }],
            vec![TapedriveEvent::TrackCertified(event)],
        )
        .unwrap();

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
    fn merge_advance_epoch_missing_event() {
        let result = merge(vec![RawInstruction::AdvanceEpoch], vec![]);
        assert!(matches!(result, Err(ParseError::EventMismatch(_))));
    }

    #[test]
    fn merge_certify_track_missing_event() {
        let track = Address::new_unique();
        let result = merge(vec![RawInstruction::CertifyTrack { track }], vec![]);
        assert!(matches!(result, Err(ParseError::EventMismatch(_))));
    }

    #[test]
    fn merge_multiple_instructions() {
        let track1 = Address::new_unique();
        let track2 = Address::new_unique();
        let owner = Address::new_unique();

        let epoch_event = epoch_advanced_event();

        let register_event = TrackWritten {
            epoch: EpochNumber(2),
            track: track1,
            tape: Address::new_unique(),
            group: GroupIndex(0),
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
                    leaves: [Hash::default(); GROUP_SIZE],
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

        match &merged[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }

        match &merged[1] {
            ParsedInstruction::TrackWrite { track, event, .. } => {
                assert_eq!(*track, track1);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected TrackWrite"),
        }

        match &merged[2] {
            ParsedInstruction::CertifyTrack { track, event } => {
                assert_eq!(*track, track2);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn merge_required_events_missing() {
        let track = Address::new_unique();
        let owner = Address::new_unique();
        let result = merge(vec![RawInstruction::DeleteTrack { owner, track }], vec![]);
        assert!(matches!(result, Err(ParseError::EventMismatch(_))));
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
                    leaves: [Hash::default(); GROUP_SIZE],
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
            RawInstruction::JoinCommittee {
                node: Address::new_unique(),
            },
        ]
    }

    fn required_event_mismatch_case() -> TapedriveEvent {
        TapedriveEvent::EpochAdvanced(epoch_advanced_event())
    }

    #[test]
    fn merge_required_events_missing_all() {
        for raw in required_events_missing_cases() {
            let result = merge(vec![raw], vec![]);
            assert!(matches!(result, Err(ParseError::EventMismatch(_))));
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
                        leaves: [Hash::default(); GROUP_SIZE],
                    }),
                },
                TapedriveEvent::TrackWritten(TrackWritten {
                    epoch: EpochNumber(2),
                    track: register_track,
                    tape: register_tape,
                    group: GroupIndex(0),
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
                    id: TapeNumber(1),
                    flags: 0,
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
                RawInstruction::JoinCommittee {
                    node: join_node,
                },
                TapedriveEvent::NodeJoinedCommittee(NodeJoinedCommittee {
                    node: join_node,
                    stake: [0; 8],
                    key: BlsPubkey::new_unique(),
                    preferences: NodePreferences::zeroed(),
                    activation_epoch: EpochNumber(1),
                }),
            ),
        ]
    }

    #[test]
    fn merge_required_events_success_all() {
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
                | ParsedInstruction::JoinCommittee { .. } => {}
                _ => panic!("expected one of the required instruction variants"),
            }
        }
    }

    #[test]
    fn merge_required_events_wrong_event_type_all() {
        for raw in required_events_missing_cases() {
            let result = merge(vec![raw], vec![required_event_mismatch_case()]);
            assert!(matches!(result, Err(ParseError::EventMismatch(_))));
        }
    }

    #[test]
    fn merge_sync_spool_with_event() {
        let node = Address::new_unique();
        let event = SpoolSynced {
            node,
            epoch: EpochNumber(5),
            group: GroupIndex(7),
            spool: 3u64.to_le_bytes(),
            phase: EpochPhase::Sync as u64,
        };
        let merged = merge(
            vec![RawInstruction::SyncSpool { node, spool: 3 }],
            vec![TapedriveEvent::SpoolSynced(event)],
        )
        .unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::SyncSpool { node: n, spool, event } => {
                assert_eq!(*n, node);
                assert_eq!(*spool, 3);
                assert_eq!(event.epoch, EpochNumber(5));
                assert_eq!(event.group, GroupIndex(7));
            }
            _ => panic!("Expected SyncSpool"),
        }
    }

    #[test]
    fn merge_advance_pool_with_event() {
        let node = Address::new_unique();
        let span = RateSpan {
            node,
            start_epoch: EpochNumber(1),
            end_epoch: EpochNumber(4),
            rate: ExchangeRate::flat(),
        };
        let event = PoolAdvanced {
            node,
            epoch: EpochNumber(3),
            span,
        };
        let track_event = TrackWritten::zeroed();
        let merged = merge(
            vec![RawInstruction::AdvancePool { node }],
            vec![
                TapedriveEvent::TrackWritten(track_event),
                TapedriveEvent::PoolAdvanced(event),
            ],
        )
        .unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::AdvancePool { node: n, event, .. } => {
                assert_eq!(*n, node);
                assert_eq!(event.epoch, EpochNumber(3));
            }
            _ => panic!("Expected AdvancePool"),
        }
    }

    #[test]
    fn merge_wrong_event_type() {
        let result = merge(
            vec![RawInstruction::AdvanceEpoch],
            vec![TapedriveEvent::TrackCertified(TrackCertified {
                track: Address::new_unique(),
                epoch: EpochNumber(1),
                signer_count: [0; 8],
                signer_weight: [0; 8],
            })],
        );
        assert!(result.is_err());
    }
}

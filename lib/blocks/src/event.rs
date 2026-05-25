//! Tapedrive event parsing from transaction logs.

use tape_api::event::{
    AssignmentFinalized, CommitteeCreated, CommitteeResized, EpochAdvanced, EpochCommitted,
    EpochCreated, EventType, CommissionClaimed, NodeJoinedCommittee, NodeRegistered,
    PeerSetResized, PoolAdvanced, SnapshotFinalized, SpoolSynced, StakeDeposited,
    StakeUnlockRequested, StakeWithdrawn, TapeDestroyed, TapeReserved, TrackCertified,
    TrackDeleted, TrackInvalidated, TrackWritten, VoteProposed, VoteRecorded,
};

use crate::error::ParseError;

/// Parsed tapedrive event from transaction logs.
///
/// This enum wraps all event types emitted by the tapedrive program.
/// Events contain execution-time state that enables correct processing
/// during historical catch-up.
#[derive(Debug, Clone)]
pub enum TapedriveEvent {
    VoteProposed(VoteProposed),
    VoteRecorded(VoteRecorded),
    SnapshotFinalized(SnapshotFinalized),
    AssignmentFinalized(AssignmentFinalized),
    EpochCreated(EpochCreated),
    CommitteeCreated(CommitteeCreated),
    CommitteeResized(CommitteeResized),
    PeerSetResized(PeerSetResized),
    EpochCommitted(EpochCommitted),
    EpochAdvanced(EpochAdvanced),
    TrackCertified(TrackCertified),
    TrackDeleted(TrackDeleted),
    TrackInvalidated(TrackInvalidated),
    TrackWritten(TrackWritten),
    TapeReserved(TapeReserved),
    TapeDestroyed(TapeDestroyed),
    NodeRegistered(NodeRegistered),
    NodeJoinedCommittee(NodeJoinedCommittee),
    SpoolSynced(SpoolSynced),
    PoolAdvanced(PoolAdvanced),
    StakeDeposited(StakeDeposited),
    StakeUnlockRequested(StakeUnlockRequested),
    StakeWithdrawn(StakeWithdrawn),
    CommissionClaimed(CommissionClaimed),
}

/// Parse event data from a "Program data:" log line.
pub fn parse_event_data(log: &str) -> Result<Option<TapedriveEvent>, ParseError> {
    let Some(encoded_data) = log.strip_prefix("Program data: ") else {
        return Ok(None);
    };

    let data = base64::decode(encoded_data).map_err(|_| ParseError::InvalidEvent)?;

    if data.len() < 8 {
        return Ok(None);
    }

    // First byte of discriminator is the EventType
    let discriminator = data[0];
    let event_type = EventType::try_from(discriminator).ok();

    let Some(event_type) = event_type else {
        return Ok(None);
    };

    // Event data starts after 8-byte discriminator
    let event_data = &data[8..];

    match event_type {
        EventType::VoteProposed => {
            let event = bytemuck::try_from_bytes::<VoteProposed>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::VoteProposed(*event)))
        }
        EventType::VoteRecorded => {
            let event = bytemuck::try_from_bytes::<VoteRecorded>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::VoteRecorded(*event)))
        }
        EventType::SnapshotFinalized => {
            let event = bytemuck::try_from_bytes::<SnapshotFinalized>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::SnapshotFinalized(*event)))
        }
        EventType::AssignmentFinalized => {
            let event = bytemuck::try_from_bytes::<AssignmentFinalized>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::AssignmentFinalized(*event)))
        }
        EventType::EpochCommitted => {
            let event = bytemuck::try_from_bytes::<EpochCommitted>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::EpochCommitted(*event)))
        }
        EventType::EpochCreated => {
            let event = bytemuck::try_from_bytes::<EpochCreated>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::EpochCreated(*event)))
        }
        EventType::CommitteeCreated => {
            let event = bytemuck::try_from_bytes::<CommitteeCreated>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::CommitteeCreated(*event)))
        }
        EventType::CommitteeResized => {
            let event = bytemuck::try_from_bytes::<CommitteeResized>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::CommitteeResized(*event)))
        }
        EventType::PeerSetResized => {
            let event = bytemuck::try_from_bytes::<PeerSetResized>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::PeerSetResized(*event)))
        }
        EventType::EpochAdvanced => {
            let event = bytemuck::try_from_bytes::<EpochAdvanced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::EpochAdvanced(*event)))
        }
        EventType::TrackCertified => {
            let event = bytemuck::try_from_bytes::<TrackCertified>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackCertified(*event)))
        }
        EventType::TrackDeleted => {
            let event = bytemuck::try_from_bytes::<TrackDeleted>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackDeleted(*event)))
        }
        EventType::TrackInvalidated => {
            let event = bytemuck::try_from_bytes::<TrackInvalidated>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackInvalidated(*event)))
        }
        EventType::TrackWritten => {
            let event = bytemuck::try_from_bytes::<TrackWritten>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackWritten(*event)))
        }
        EventType::TapeReserved => {
            let event = bytemuck::try_from_bytes::<TapeReserved>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TapeReserved(*event)))
        }
        EventType::TapeDestroyed => {
            let event = bytemuck::try_from_bytes::<TapeDestroyed>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TapeDestroyed(*event)))
        }
        EventType::NodeRegistered => {
            let event = bytemuck::try_from_bytes::<NodeRegistered>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::NodeRegistered(*event)))
        }
        EventType::NodeJoinedCommittee => {
            let event = bytemuck::try_from_bytes::<NodeJoinedCommittee>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::NodeJoinedCommittee(*event)))
        }
        EventType::SpoolSynced => {
            let event = bytemuck::try_from_bytes::<SpoolSynced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::SpoolSynced(*event)))
        }
        EventType::PoolAdvanced => {
            let event = bytemuck::try_from_bytes::<PoolAdvanced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::PoolAdvanced(*event)))
        }
        EventType::StakeDeposited => {
            let event = bytemuck::try_from_bytes::<StakeDeposited>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::StakeDeposited(*event)))
        }
        EventType::StakeUnlockRequested => {
            let event = bytemuck::try_from_bytes::<StakeUnlockRequested>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::StakeUnlockRequested(*event)))
        }
        EventType::StakeWithdrawn => {
            let event = bytemuck::try_from_bytes::<StakeWithdrawn>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::StakeWithdrawn(*event)))
        }
        EventType::CommissionClaimed => {
            let event = bytemuck::try_from_bytes::<CommissionClaimed>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::CommissionClaimed(*event)))
        }
        EventType::Unknown => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::prelude::*;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{NodePreferences, VoteKind};
    use tape_core::types::{TrackNumber, VersionId};
    use tape_core::types::coin::TAPE;
    use tape_crypto::address::Address;
    use tape_crypto::Hash;

    fn encode_event<T: bytemuck::Pod>(event_type: EventType, event: &T) -> String {
        let mut data = vec![event_type as u8];
        data.extend_from_slice(&[0u8; 7]); // Rest of 8-byte discriminator
        data.extend_from_slice(bytemuck::bytes_of(event));
        let encoded = base64::encode(&data);
        format!("Program data: {}", encoded)
    }

    #[test]
    fn parse_epoch_committed_event() {
        let event = EpochCommitted {
            epoch: EpochNumber(5),
            next_nonce: Hash::from([0x42; 32]),
            preferences: NodePreferences::zeroed(),
        };

        let log = encode_event(EventType::EpochCommitted, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::EpochCommitted(e) => {
                assert_eq!(e.epoch, EpochNumber(5));
                assert_eq!(e.next_nonce, Hash::from([0x42; 32]));
            }
            _ => panic!("Expected EpochCommitted event"),
        }
    }

    #[test]
    fn parse_epoch_advanced_event() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            total_stake: [0; 8],
            committee_count: [10, 0, 0, 0, 0, 0, 0, 0],
            preferences: NodePreferences {
                storage_capacity: StorageUnits::mb(1000),
                storage_price: TAPE(0),
                committee_size: 0,
                spool_groups: 0,
                min_version: VersionId(0),
            },
            nonce: Hash::default(),
        };

        let log = encode_event(EventType::EpochAdvanced, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::EpochAdvanced(e) => {
                assert_eq!(e.old_epoch, EpochNumber(5));
                assert_eq!(e.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected EpochAdvanced event"),
        }
    }

    #[test]
    fn parse_track_certified_event() {
        let track = Address::new_unique();
        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [5, 0, 0, 0, 0, 0, 0, 0],
            signer_weight: [100, 0, 0, 0, 0, 0, 0, 0],
        };

        let log = encode_event(EventType::TrackCertified, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::TrackCertified(e) => {
                assert_eq!(e.track, track);
                assert_eq!(e.epoch, EpochNumber(10));
            }
            _ => panic!("Expected TrackCertified event"),
        }
    }

    #[test]
    fn parse_track_written_event() {
        let track = Address::new_unique();
        let tape = Address::new_unique();
        let event = TrackWritten {
            epoch: EpochNumber(3),
            track,
            tape,
            group: GroupIndex(5),
            track_number: TrackNumber(7),
            track_hash: Hash::default(),
        };

        let log = encode_event(EventType::TrackWritten, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::TrackWritten(e) => {
                assert_eq!(e.track, track);
                assert_eq!(e.tape, tape);
                assert_eq!(e.epoch, EpochNumber(3));
                assert_eq!(e.track_number, TrackNumber(7));
                assert_eq!(e.group, GroupIndex(5));
            }
            _ => panic!("Expected TrackWritten event"),
        }
    }

    #[test]
    fn parse_vote_proposed_event() {
        let vote = Address::new_unique();
        let proposed = VoteProposed {
            kind: VoteKind::Snapshot as u64,
            vote,
            voting_epoch: EpochNumber(21),
            target_epoch: EpochNumber(20),
            hash: Hash::from([0x55; 32]),
            total_groups: 5u64.to_le_bytes(),
        };

        let log = encode_event(EventType::VoteProposed, &proposed);

        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::VoteProposed(decoded) => {
                assert_eq!(decoded.kind, VoteKind::Snapshot as u64);
                assert_eq!(decoded.vote, vote);
                assert_eq!(decoded.target_epoch, EpochNumber(20));
                assert_eq!(decoded.hash, Hash::from([0x55; 32]));
            }
            _ => panic!("Expected VoteProposed event"),
        }
    }

    #[test]
    fn parse_vote_recorded_event() {
        let vote = Address::new_unique();
        let recorded = VoteRecorded {
            kind: VoteKind::Assignment as u64,
            vote,
            voting_epoch: EpochNumber(20),
            target_epoch: EpochNumber(21),
            hash: Hash::from([0x66; 32]),
            group: GroupIndex(3),
            signer_count: [14, 0, 0, 0, 0, 0, 0, 0],
            signed_groups: 4u64.to_le_bytes(),
            total_groups: 5u64.to_le_bytes(),
        };

        let log = encode_event(EventType::VoteRecorded, &recorded);

        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::VoteRecorded(decoded) => {
                assert_eq!(decoded.kind, VoteKind::Assignment as u64);
                assert_eq!(decoded.vote, vote);
                assert_eq!(decoded.group, recorded.group);
                assert_eq!(decoded.signer_count, recorded.signer_count);
                assert_eq!(decoded.signed_groups, 4u64.to_le_bytes());
            }
            _ => panic!("Expected VoteRecorded event"),
        }
    }

    #[test]
    fn parse_snapshot_finalized_event() {
        let snapshot_tape = Address::new_unique();
        let finalized = SnapshotFinalized {
            epoch: EpochNumber(20),
            hash: Hash::from([0x55; 32]),
            snapshot_tape,
        };

        let log = encode_event(EventType::SnapshotFinalized, &finalized);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::SnapshotFinalized(decoded) => {
                assert_eq!(decoded.epoch, finalized.epoch);
                assert_eq!(decoded.hash, finalized.hash);
                assert_eq!(decoded.snapshot_tape, snapshot_tape);
            }
            _ => panic!("Expected SnapshotFinalized event"),
        }
    }

    #[test]
    fn parse_assignment_finalized_event() {
        let group_account = Address::new_unique();
        let finalized = AssignmentFinalized {
            epoch: EpochNumber(21),
            hash: Hash::from([0x66; 32]),
            group: GroupIndex(3),
            group_account,
            size: StorageUnits::mb(10),
            total_groups: 4u64.to_le_bytes(),
            total_assigned: StorageUnits::mb(800),
        };

        let log = encode_event(EventType::AssignmentFinalized, &finalized);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::AssignmentFinalized(decoded) => {
                assert_eq!(decoded.epoch, finalized.epoch);
                assert_eq!(decoded.hash, finalized.hash);
                assert_eq!(decoded.group, finalized.group);
                assert_eq!(decoded.group_account, group_account);
                assert_eq!(decoded.total_groups, 4u64.to_le_bytes());
            }
            _ => panic!("Expected AssignmentFinalized event"),
        }
    }

    #[test]
    fn parse_tape_reserved_event() {
        let tape = Address::new_unique();
        let authority = Address::new_unique();
        let event = TapeReserved {
            tape,
            id: TapeNumber(1),
            flags: 0,
            authority,
            capacity: StorageUnits::mb(1000),
            active_epoch: EpochNumber(1),
            expiry_epoch: EpochNumber(10),
            cost: [100, 0, 0, 0, 0, 0, 0, 0],
        };

        let log = encode_event(EventType::TapeReserved, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::TapeReserved(e) => {
                assert_eq!(e.tape, tape);
                assert_eq!(e.authority, authority);
                assert_eq!(e.capacity, StorageUnits::mb(1000));
            }
            _ => panic!("Expected TapeReserved event"),
        }
    }

    #[test]
    fn parse_stake_and_commission_events() {
        let stake = Address::new_unique();
        let authority = Address::new_unique();
        let pool = Address::new_unique();
        let node = Address::new_unique();

        let deposited = StakeDeposited {
            stake,
            authority,
            pool,
            amount: 10u64.to_le_bytes(),
            activation_epoch: EpochNumber(8),
        };
        let log = encode_event(EventType::StakeDeposited, &deposited);
        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::StakeDeposited(event) => {
                assert_eq!(event.stake, stake);
                assert_eq!(event.amount, 10u64.to_le_bytes());
            }
            other => panic!("Expected StakeDeposited event, got {other:?}"),
        }

        let unlock = StakeUnlockRequested {
            stake,
            authority,
            pool,
            amount: 9u64.to_le_bytes(),
            withdraw_epoch: EpochNumber(10),
        };
        let log = encode_event(EventType::StakeUnlockRequested, &unlock);
        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::StakeUnlockRequested(event) => {
                assert_eq!(event.stake, stake);
                assert_eq!(event.withdraw_epoch, EpochNumber(10));
            }
            other => panic!("Expected StakeUnlockRequested event, got {other:?}"),
        }

        let withdrawn = StakeWithdrawn {
            stake,
            authority,
            pool,
            principal: 8u64.to_le_bytes(),
            rewards: 7u64.to_le_bytes(),
        };
        let log = encode_event(EventType::StakeWithdrawn, &withdrawn);
        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::StakeWithdrawn(event) => {
                assert_eq!(event.stake, stake);
                assert_eq!(event.rewards, 7u64.to_le_bytes());
            }
            other => panic!("Expected StakeWithdrawn event, got {other:?}"),
        }

        let commission = CommissionClaimed {
            node,
            authority,
            amount: 6u64.to_le_bytes(),
        };
        let log = encode_event(EventType::CommissionClaimed, &commission);
        match parse_event_data(&log).unwrap().unwrap() {
            TapedriveEvent::CommissionClaimed(event) => {
                assert_eq!(event.node, node);
                assert_eq!(event.amount, 6u64.to_le_bytes());
            }
            other => panic!("Expected CommissionClaimed event, got {other:?}"),
        }
    }

    #[test]
    fn parse_invalid_event_data() {
        // Too short
        let result = parse_event_data("Program data: AAAA").unwrap();
        assert!(result.is_none());

        // Invalid base64
        let result = parse_event_data("Program data: !!!invalid!!!");
        assert!(result.is_err());
    }

    #[test]
    fn parse_unknown_event_type() {
        // Create data with unknown discriminator (0xFF)
        let mut data = vec![0xFFu8; 8];
        data.extend_from_slice(&[0u8; 32]); // Some padding
        let encoded = base64::encode(&data);
        let log = format!("Program data: {}", encoded);

        let result = parse_event_data(&log).unwrap();
        assert!(result.is_none()); // Unknown events are skipped, not errors
    }
}

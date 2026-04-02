//! Tapedrive event parsing from transaction logs.

use tape_api::event::{
    EpochAdvanced, EventType, NodeJoinedCommittee, NodeRegistered, NodeSynced, PoolAdvanced,
    TapeDestroyed, TapeReserved, TrackCertified, TrackDeleted, TrackInvalidated, TrackWritten,
};

use crate::error::ParseError;

/// Parsed tapedrive event from transaction logs.
///
/// This enum wraps all event types emitted by the tapedrive program.
/// Events contain execution-time state that enables correct processing
/// during historical catch-up.
#[derive(Debug, Clone)]
pub enum TapedriveEvent {
    EpochAdvanced(EpochAdvanced),
    TrackCertified(TrackCertified),
    TrackDeleted(TrackDeleted),
    TrackInvalidated(TrackInvalidated),
    TrackWritten(TrackWritten),
    TapeReserved(TapeReserved),
    TapeDestroyed(TapeDestroyed),
    NodeRegistered(NodeRegistered),
    NodeJoinedCommittee(NodeJoinedCommittee),
    NodeSynced(NodeSynced),
    PoolAdvanced(PoolAdvanced),
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
        EventType::NodeSynced => {
            let event = bytemuck::try_from_bytes::<NodeSynced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::NodeSynced(*event)))
        }
        EventType::PoolAdvanced => {
            let event = bytemuck::try_from_bytes::<PoolAdvanced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::PoolAdvanced(*event)))
        }
        // Unknown event types are silently skipped
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::prelude::*;
    use tape_crypto::Hash;

    fn encode_event<T: bytemuck::Pod>(event_type: EventType, event: &T) -> String {
        let mut data = vec![event_type as u8];
        data.extend_from_slice(&[0u8; 7]); // Rest of 8-byte discriminator
        data.extend_from_slice(bytemuck::bytes_of(event));
        let encoded = base64::encode(&data);
        format!("Program data: {}", encoded)
    }

    #[test]
    fn test_parse_epoch_advanced_event() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [10, 0, 0, 0, 0, 0, 0, 0],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits::mb(1000),
            nonce: Hash::default(),
            phase: 1, // Syncing
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
    fn test_parse_track_certified_event() {
        let track = solana_sdk::pubkey::Pubkey::new_unique();
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
    fn test_parse_track_written_event() {
        let track = solana_sdk::pubkey::Pubkey::new_unique();
        let tape = solana_sdk::pubkey::Pubkey::new_unique();
        let event = TrackWritten {
            epoch: EpochNumber(3),
            track,
            tape,
            spool_group: 5u64.to_le_bytes(),
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
            }
            _ => panic!("Expected TrackWritten event"),
        }
    }

    #[test]
    fn test_parse_tape_reserved_event() {
        let tape = solana_sdk::pubkey::Pubkey::new_unique();
        let authority = solana_sdk::pubkey::Pubkey::new_unique();
        let event = TapeReserved {
            tape,
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
    fn test_parse_invalid_event_data() {
        // Too short
        let result = parse_event_data("Program data: AAAA").unwrap();
        assert!(result.is_none());

        // Invalid base64
        let result = parse_event_data("Program data: !!!invalid!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_event_type() {
        // Create data with unknown discriminator (0xFF)
        let mut data = vec![0xFFu8; 8];
        data.extend_from_slice(&[0u8; 32]); // Some padding
        let encoded = base64::encode(&data);
        let log = format!("Program data: {}", encoded);

        let result = parse_event_data(&log).unwrap();
        assert!(result.is_none()); // Unknown events are skipped, not errors
    }
}

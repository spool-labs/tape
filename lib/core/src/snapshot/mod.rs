//! Epoch snapshot types for fast node bootstrap.
//!
//! At each epoch boundary, committee members build a deterministic event log
//! (`SnapshotLog`) from the events processed during the epoch. This log can
//! be replayed through block processor handlers to reconstruct state without
//! replaying all Solana blocks from genesis.

mod types;
pub use types::{ReplayTrack, ReplayableEvent, SnapshotEntry, SnapshotLog};

#[cfg(test)]
mod tests {
    use solana_program::pubkey::Pubkey;

    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::SPOOL_GROUP_SIZE;
    use crate::spooler::SpoolGroup;
    use crate::track::blob::BlobInfo;
    use crate::track::types::{CompressedTrack, TrackKind, TrackState};
    use crate::types::{EpochNumber, NodeId, SlotNumber};
    use tape_crypto::hash::Hash;

    fn raw_replay_track() -> ReplayTrack {
        ReplayTrack {
            state: CompressedTrack {
                tape: Pubkey::new_from_array([1u8; 32]),
                key: Hash::default(),
                track_number: 0u64.into(),
                kind: TrackKind::Raw as u64,
                state: TrackState::Certified as u64,
                size: 1u64.into(),
                spool_group: SpoolGroup::from(0),
                value_hash: Hash::default(),
            },
            epoch: EpochNumber(10),
            blob: None,
        }
    }

    fn blob_replay_track() -> ReplayTrack {
        ReplayTrack {
            state: CompressedTrack {
                tape: Pubkey::new_from_array([2u8; 32]),
                key: Hash::from([3u8; 32]),
                track_number: 1u64.into(),
                kind: TrackKind::Blob as u64,
                state: TrackState::Registered as u64,
                size: 1024u64.into(),
                spool_group: SpoolGroup::from(1),
                value_hash: Hash::from([4u8; 32]),
            },
            epoch: EpochNumber(11),
            blob: Some(BlobInfo {
                size: 1024u64.into(),
                root: Hash::from([5u8; 32]),
                commitment: Hash::from([6u8; 32]),
                profile: EncodingProfile::basic_default(),
                stripe_size: 128,
                stripe_count: 4,
                leaves: [Hash::from([7u8; 32]); SPOOL_GROUP_SIZE],
            }),
        }
    }

    #[test]
    fn test_replayable_event_variants() {
        let events = vec![
            ReplayableEvent::Track(raw_replay_track()),
            ReplayableEvent::CertifyTrack {
                track: [2u8; 32],
                epoch: EpochNumber(10),
            },
            ReplayableEvent::DeleteTrack {
                track: [3u8; 32],
                epoch: EpochNumber(10),
            },
            ReplayableEvent::InvalidateTrack {
                track: [4u8; 32],
                epoch: EpochNumber(10),
            },
            ReplayableEvent::AdvanceEpoch {
                old_epoch: EpochNumber(9),
                new_epoch: EpochNumber(10),
            },
            ReplayableEvent::SyncEpoch {
                node: [5u8; 32],
                node_id: NodeId(1),
                epoch: EpochNumber(10),
                spools_hash: Hash::default(),
            },
            ReplayableEvent::ReserveTape {
                tape: [6u8; 32],
                authority: [7u8; 32],
                active_epoch: EpochNumber(10),
                expiry_epoch: EpochNumber(20),
            },
            ReplayableEvent::DestroyTape {
                tape: [8u8; 32],
                epoch: EpochNumber(10),
            },
            ReplayableEvent::RegisterNode {
                authority: [9u8; 32],
                node: [10u8; 32],
            },
            ReplayableEvent::JoinNetwork {
                node: [11u8; 32],
            },
        ];
        assert_eq!(events.len(), 10);
    }

    #[test]
    fn test_snapshot_log_construction() {
        let log = SnapshotLog {
            version: 1,
            epoch: EpochNumber(42),
            start_slot: SlotNumber(100),
            end_slot: SlotNumber(200),
            entries: vec![
                SnapshotEntry {
                    slot: SlotNumber(100),
                    events: vec![ReplayableEvent::AdvanceEpoch {
                        old_epoch: EpochNumber(41),
                        new_epoch: EpochNumber(42),
                    }],
                },
                SnapshotEntry {
                    slot: SlotNumber(150),
                    events: vec![
                        ReplayableEvent::Track(ReplayTrack {
                            epoch: EpochNumber(42),
                            ..raw_replay_track()
                        }),
                        ReplayableEvent::CertifyTrack {
                            track: [1u8; 32],
                            epoch: EpochNumber(42),
                        },
                    ],
                },
            ],
        };

        assert_eq!(log.version, 1);
        assert_eq!(log.epoch, EpochNumber(42));
        assert_eq!(log.entries.len(), 2);
        assert_eq!(log.entries[1].events.len(), 2);
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn test_snapshot_log_wincode_roundtrip() {
        let log = SnapshotLog {
            version: 1,
            epoch: EpochNumber(42),
            start_slot: SlotNumber(100),
            end_slot: SlotNumber(200),
            entries: vec![SnapshotEntry {
                slot: SlotNumber(150),
                events: vec![
                    ReplayableEvent::AdvanceEpoch {
                        old_epoch: EpochNumber(41),
                        new_epoch: EpochNumber(42),
                    },
                    ReplayableEvent::Track(ReplayTrack {
                        state: CompressedTrack {
                            tape: Pubkey::new_from_array([0xAB; 32]),
                            ..raw_replay_track().state
                        },
                        epoch: EpochNumber(42),
                        ..raw_replay_track()
                    }),
                    ReplayableEvent::SyncEpoch {
                        node: [0xCD; 32],
                        node_id: NodeId(7),
                        epoch: EpochNumber(42),
                        spools_hash: Hash::default(),
                    },
                ],
            }],
        };

        let bytes = wincode::serialize(&log).expect("serialize");
        let recovered: SnapshotLog = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, log);
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn test_replayable_event_wincode_roundtrip() {
        let events = vec![
            ReplayableEvent::Track(raw_replay_track()),
            ReplayableEvent::Track(blob_replay_track()),
            ReplayableEvent::CertifyTrack {
                track: [2u8; 32],
                epoch: EpochNumber(10),
            },
            ReplayableEvent::JoinNetwork {
                node: [3u8; 32],
            },
        ];

        for event in &events {
            let bytes = wincode::serialize(event).expect("serialize");
            let recovered: ReplayableEvent = wincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(&recovered, event);
        }
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn test_replay_track_with_blob_wincode_roundtrip() {
        let track = blob_replay_track();
        let bytes = wincode::serialize(&track).expect("serialize");
        let recovered: ReplayTrack = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(recovered, track);
    }
}

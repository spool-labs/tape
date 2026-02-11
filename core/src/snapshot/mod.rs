//! Epoch snapshot types for fast node bootstrap.
//!
//! At each epoch boundary, committee members build a deterministic event log
//! (`SnapshotLog`) from the events processed during the epoch. This log can
//! be replayed through block processor handlers to reconstruct state without
//! replaying all Solana blocks from genesis.

use crate::types::{EpochNumber, NodeId, SlotNumber};
use tape_crypto::hash::Hash;

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

/// Replayable event — mirrors block processing handler parameters.
///
/// Each variant captures exactly the data needed to replay an instruction
/// through the same handler used during live block processing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub enum ReplayableEvent {
    /// Track was registered. `event_data` stores the raw Pod bytes of
    /// `TrackRegistered` (808 bytes). During replay, parse with
    /// `bytemuck::try_from_bytes::<TrackRegistered>`.
    RegisterTrack {
        track: [u8; 32],
        event_data: Vec<u8>,
    },

    /// Track was certified.
    CertifyTrack {
        track: [u8; 32],
        epoch: EpochNumber,
    },

    /// Track was deleted.
    DeleteTrack {
        track: [u8; 32],
        epoch: EpochNumber,
    },

    /// Track was invalidated.
    InvalidateTrack {
        track: [u8; 32],
        epoch: EpochNumber,
    },

    /// Epoch advanced.
    AdvanceEpoch {
        old_epoch: EpochNumber,
        new_epoch: EpochNumber,
    },

    /// Node synced for epoch.
    SyncEpoch {
        node: [u8; 32],
        node_id: NodeId,
        epoch: EpochNumber,
        spools_hash: Hash,
    },

    /// Tape was reserved.
    ReserveTape {
        tape: [u8; 32],
        authority: [u8; 32],
        active_epoch: EpochNumber,
        expiry_epoch: EpochNumber,
    },

    /// Tape was destroyed.
    DestroyTape {
        tape: [u8; 32],
        epoch: EpochNumber,
    },

    /// Node was registered.
    RegisterNode {
        authority: [u8; 32],
        node: [u8; 32],
    },

    /// Node joined the network.
    JoinNetwork {
        node: [u8; 32],
    },
}

/// A single slot's events within a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotEntry {
    pub slot: SlotNumber,
    pub events: Vec<ReplayableEvent>,
}

/// Complete event log for one epoch, suitable for serialization
/// and erasure coding across spool groups.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotLog {
    /// Format version (currently 1).
    pub version: u8,
    /// Epoch this snapshot covers.
    pub epoch: EpochNumber,
    /// First slot in this epoch.
    pub start_slot: SlotNumber,
    /// Last slot in this epoch.
    pub end_slot: SlotNumber,
    /// Ordered entries (one per slot that had events).
    pub entries: Vec<SnapshotEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replayable_event_variants() {
        let events = vec![
            ReplayableEvent::RegisterTrack {
                track: [1u8; 32],
                event_data: vec![0u8; 808],
            },
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
                        ReplayableEvent::RegisterTrack {
                            track: [1u8; 32],
                            event_data: vec![0u8; 808],
                        },
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
                    ReplayableEvent::RegisterTrack {
                        track: [0xAB; 32],
                        event_data: vec![1, 2, 3, 4],
                    },
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
            ReplayableEvent::RegisterTrack {
                track: [1u8; 32],
                event_data: vec![0u8; 100],
            },
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
}

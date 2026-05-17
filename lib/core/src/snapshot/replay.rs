//! Epoch snapshot types for fast node bootstrap.
//!
//! At each epoch boundary, committee members build a deterministic event log
//! (`SnapshotLog`) from the events processed during the epoch. This log can
//! be replayed through block processor handlers to reconstruct state without
//! replaying all Solana blocks from genesis.

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode::containers::{Pod, Vec as WincodeVec};
#[cfg(feature = "wincode")]
use wincode::len::BincodeLen;
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

#[cfg(feature = "wincode")]
use crate::snapshot::error::SnapshotError;
use crate::track::blob::BlobInfo;
use crate::track::types::CompressedTrack;
use crate::types::{EpochNumber, NodeId, SlotNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;

/// Wire-format version for the framed snapshot binary.
pub const SNAPSHOT_VERSION: u8 = 1;

#[cfg(feature = "wincode")]
const SNAPSHOT_FRAME_LIMIT: usize = 4 * 1024 * 1024;

#[cfg(feature = "wincode")]
type SnapshotFrameBytes = WincodeVec<Pod<u8>, BincodeLen<SNAPSHOT_FRAME_LIMIT>>;

/// Replayable event, mirrors block processing handler parameters.
///
/// Each variant captures exactly the data needed to replay an instruction
/// through the same handler used during live block processing.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub enum ReplayableEvent {
    /// Track was written.
    Track(ReplayTrack),

    /// Track was certified.
    CertifyTrack {
        track: Address,
        epoch: EpochNumber,
    },

    /// Track was deleted.
    DeleteTrack {
        track: Address,
        epoch: EpochNumber,
    },

    /// Track was invalidated.
    InvalidateTrack {
        track: Address,
        epoch: EpochNumber,
    },

    /// Epoch advanced.
    AdvanceEpoch {
        old_epoch: EpochNumber,
        new_epoch: EpochNumber,
    },

    /// Node synced for epoch.
    /// TODO(v2): per-spool shape. See `solana/api/src/instruction/node.rs`
    /// `SyncSpool` for the on-chain ix shape that this event will mirror once
    /// block_ingestor lands. Fields kept as-is to avoid pulling network/node
    /// into the current rename pass.
    SyncSpool {
        node: Address,
        node_id: NodeId,
        epoch: EpochNumber,
        spools_hash: Hash,
    },

    /// Tape was reserved.
    ReserveTape {
        tape: Address,
        authority: Address,
        active_epoch: EpochNumber,
        expiry_epoch: EpochNumber,
    },

    /// Tape was destroyed.
    DestroyTape {
        tape: Address,
        epoch: EpochNumber,
    },

    /// Node was registered.
    RegisterNode {
        authority: Address,
        node: Address,
    },

    /// Node joined the next-epoch committee.
    JoinCommittee {
        node: Address,
    },
}

/// Replayable track metadata for the track-write flow.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct ReplayTrack {
    /// Compressed track record (key, kind, state, size, spool group, value hash).
    pub state: CompressedTrack,
    /// Epoch in which the track was written.
    pub epoch: EpochNumber,
    /// Blob commitment metadata, present only for blob-kind tracks.
    pub blob: Option<BlobInfo>,
}

/// A single slot's events within a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotEntry {
    /// Slot in which these events occurred.
    pub slot: SlotNumber,
    /// Events emitted during this slot, in processing order.
    pub events: Vec<ReplayableEvent>,
}

/// Complete event log for one epoch, suitable for serialization
/// and erasure coding across spool groups.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotLog {
    /// Epoch this snapshot covers.
    pub epoch: EpochNumber,
    /// First slot in this epoch.
    pub start_slot: SlotNumber,
    /// Last slot in this epoch.
    pub end_slot: SlotNumber,
    /// Ordered entries (one per slot that had events).
    pub entries: Vec<SnapshotEntry>,
}

/// Fixed-size header for the snapshot framed binary format
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotHeader {
    /// Wire-format version; must equal [`SNAPSHOT_VERSION`] on read.
    pub version: u8,
    /// Epoch this snapshot covers.
    pub epoch: EpochNumber,
    /// First slot in this epoch.
    pub start_slot: SlotNumber,
    /// Last slot in this epoch.
    pub end_slot: SlotNumber,
    /// Number of [`SnapshotEntryFrame`]s following the header.
    pub entry_count: u64,
}

/// Length-prefixed frame wrapping one serialized SnapshotEntry
#[cfg(feature = "wincode")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotEntryFrame {
    /// Length of `data` in bytes.
    pub len: u64,
    /// Wincode-serialized [`SnapshotEntry`] payload.
    #[wincode(with = "SnapshotFrameBytes")]
    pub data: Vec<u8>,
}

#[cfg(feature = "wincode")]
impl SnapshotLog {
    /// Serialize to the framed binary format
    ///
    /// Each entry is individually serialized with wincode to stay within
    /// the default preallocation limit. The wire format is:
    /// `[SnapshotHeader][SnapshotEntryFrame_0][SnapshotEntryFrame_1]...`
    pub fn to_bytes(&self) -> Result<Vec<u8>, SnapshotError> {
        let header = SnapshotHeader {
            version: SNAPSHOT_VERSION,
            epoch: self.epoch,
            start_slot: self.start_slot,
            end_slot: self.end_slot,
            entry_count: self.entries.len() as u64,
        };

        let mut buffer = wincode::serialize(&header)?;

        for entry in &self.entries {
            let entry_bytes = wincode::serialize(entry)?;
            let frame = SnapshotEntryFrame {
                len: entry_bytes.len() as u64,
                data: entry_bytes,
            };
            buffer.extend(wincode::serialize(&frame)?);
        }

        Ok(buffer)
    }

    /// Deserialize from the framed binary format
    pub fn from_bytes(data: &[u8]) -> Result<Self, SnapshotError> {
        let header: SnapshotHeader = wincode::deserialize(data)?;
        if header.version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(header.version));
        }
        let header_size = wincode::serialized_size(&header)? as usize;

        let mut cursor = header_size;
        let mut entries = Vec::with_capacity(header.entry_count as usize);

        for _ in 0..header.entry_count {
            let frame: SnapshotEntryFrame = wincode::deserialize(&data[cursor..])?;
            let frame_size = wincode::serialized_size(&frame)? as usize;
            cursor += frame_size;

            let entry: SnapshotEntry = wincode::deserialize(&frame.data)?;
            entries.push(entry);
        }

        Ok(SnapshotLog {
            epoch: header.epoch,
            start_slot: header.start_slot,
            end_slot: header.end_slot,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::spooler::SpoolGroup;
    use crate::track::types::{TrackKind, TrackState};
    #[cfg(feature = "wincode")]
    use crate::encoding::EncodingProfile;
    #[cfg(feature = "wincode")]
    use crate::erasure::GROUP_SIZE;
    #[cfg(feature = "wincode")]
    use crate::types::{StorageUnits, StripeCount};
    use super::*;

    fn raw_replay_track() -> ReplayTrack {
        ReplayTrack {
            state: CompressedTrack {
                tape: Address::from([1u8; 32]),
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

    #[cfg(feature = "wincode")]
    fn blob_replay_track() -> ReplayTrack {
        ReplayTrack {
            state: CompressedTrack {
                tape: Address::from([2u8; 32]),
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
                size: StorageUnits::from_bytes(1024),
                commitment: Hash::from([6u8; 32]),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(128),
                stripe_count: StripeCount(4),
                leaves: [Hash::from([7u8; 32]); GROUP_SIZE],
            }),
        }
    }

    #[test]
    fn test_replayable_event_variants() {
        let events = vec![
            ReplayableEvent::Track(raw_replay_track()),
            ReplayableEvent::CertifyTrack {
                track: Address::from([2u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::DeleteTrack {
                track: Address::from([3u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::InvalidateTrack {
                track: Address::from([4u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::AdvanceEpoch {
                old_epoch: EpochNumber(9),
                new_epoch: EpochNumber(10),
            },
            ReplayableEvent::SyncSpool {
                node: Address::from([5u8; 32]),
                node_id: NodeId(1),
                epoch: EpochNumber(10),
                spools_hash: Hash::default(),
            },
            ReplayableEvent::ReserveTape {
                tape: Address::from([6u8; 32]),
                authority: Address::from([7u8; 32]),
                active_epoch: EpochNumber(10),
                expiry_epoch: EpochNumber(20),
            },
            ReplayableEvent::DestroyTape {
                tape: Address::from([8u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::RegisterNode {
                authority: Address::from([9u8; 32]),
                node: Address::from([10u8; 32]),
            },
            ReplayableEvent::JoinCommittee {
                node: Address::from([11u8; 32]),
            },
        ];
        assert_eq!(events.len(), 10);
    }

    #[test]
    fn test_snapshot_log_construction() {
        let log = SnapshotLog {
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
                            track: Address::from([1u8; 32]),
                            epoch: EpochNumber(42),
                        },
                    ],
                },
            ],
        };

        assert_eq!(log.epoch, EpochNumber(42));
        assert_eq!(log.entries.len(), 2);
        assert_eq!(log.entries[1].events.len(), 2);
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn test_snapshot_log_wincode_roundtrip() {
        let log = SnapshotLog {
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
                            tape: Address::from([0xAB; 32]),
                            ..raw_replay_track().state
                        },
                        epoch: EpochNumber(42),
                        ..raw_replay_track()
                    }),
                    ReplayableEvent::SyncSpool {
                        node: Address::from([0xCD; 32]),
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
                track: Address::from([2u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::JoinCommittee {
                node: Address::from([3u8; 32]),
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

    // framed serialization roundtrips through to_bytes/from_bytes
    #[cfg(feature = "wincode")]
    #[test]
    fn snapshot_framed_roundtrip() {
        let log = SnapshotLog {
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
                        ReplayableEvent::Track(blob_replay_track()),
                        ReplayableEvent::CertifyTrack {
                            track: Address::from([1u8; 32]),
                            epoch: EpochNumber(42),
                        },
                        ReplayableEvent::SyncSpool {
                            node: Address::from([0xCD; 32]),
                            node_id: NodeId(7),
                            epoch: EpochNumber(42),
                            spools_hash: Hash::default(),
                        },
                    ],
                },
            ],
        };

        let bytes = log.to_bytes().expect("to_bytes");
        let recovered = SnapshotLog::from_bytes(&bytes).expect("from_bytes");

        assert_eq!(recovered, log);
    }

    // empty snapshot log roundtrips correctly
    #[cfg(feature = "wincode")]
    #[test]
    fn snapshot_empty_roundtrip() {
        let log = SnapshotLog {
            epoch: EpochNumber(5),
            start_slot: SlotNumber(0),
            end_slot: SlotNumber(0),
            entries: vec![],
        };

        let bytes = log.to_bytes().expect("to_bytes");
        let recovered = SnapshotLog::from_bytes(&bytes).expect("from_bytes");

        assert_eq!(recovered, log);
    }
}

#[cfg(feature = "wincode")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "wincode")]
use wincode::containers::{Pod, Vec as WincodeVec};
#[cfg(feature = "wincode")]
use wincode::len::BincodeLen;
#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::bls::BlsPubkey;
#[cfg(feature = "wincode")]
use crate::snapshot::error::SnapshotError;
use crate::system::NodePreferences;
use crate::track::blob::BlobEncoding;
use crate::spooler::GroupIndex;
use crate::track::types::CompressedTrack;
use crate::types::coin::{Coin, TAPE};
use crate::types::{EpochNumber, NodeId, SlotNumber, SpoolIndex, StorageUnits, TapeNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_crypto::tx::Txid;

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
        timestamp: i64,
        total_stake: Coin<TAPE>,
        committee_count: u64,
        preferences: NodePreferences,
        subsidy: Coin<TAPE>,
        nonce: Hash,
    },

    /// Node synced one spool for an epoch.
    SyncSpool {
        node: Address,
        epoch: EpochNumber,
        group: GroupIndex,
        spool: SpoolIndex,
    },

    /// Tape was reserved.
    ReserveTape {
        tape: Address,
        id: TapeNumber,
        flags: u64,
        authority: Address,
        capacity: StorageUnits,
        active_epoch: EpochNumber,
        expiry_epoch: EpochNumber,
        cost: Coin<TAPE>,
        burned: Coin<TAPE>,
        scheduled: Coin<TAPE>,
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
        id: NodeId,
    },

    /// Node joined the next-epoch committee.
    JoinCommittee {
        node: Address,
        stake: Coin<TAPE>,
        key: BlsPubkey,
        preferences: NodePreferences,
        activation_epoch: EpochNumber,
    },

    /// Canonical epoch snapshot tape was created.
    SnapshotFinalized {
        epoch: EpochNumber,
        hash: Hash,
        snapshot_tape: Address,
    },

    /// One spool group from the canonical assignment was finalized.
    AssignmentFinalized {
        epoch: EpochNumber,
        hash: Hash,
        group: GroupIndex,
        group_account: Address,
        size: StorageUnits,
        total_groups: u64,
        total_assigned: StorageUnits,
    },

    /// User staked TAPE.
    StakeDeposited {
        stake: Address,
        authority: Address,
        pool: Address,
        amount: Coin<TAPE>,
        activation_epoch: EpochNumber,
    },

    /// Unstake initiated (cooldown started).
    StakeUnlockRequested {
        stake: Address,
        authority: Address,
        pool: Address,
        amount: Coin<TAPE>,
        withdraw_epoch: EpochNumber,
    },

    /// Stake fully withdrawn.
    StakeWithdrawn {
        stake: Address,
        authority: Address,
        pool: Address,
        principal: Coin<TAPE>,
        rewards: Coin<TAPE>,
    },

    /// A snapshot/assignment candidate vote was proposed.
    VoteProposed {
        kind: u64,
        vote: Address,
        voting_epoch: EpochNumber,
        target_epoch: EpochNumber,
        hash: Hash,
        total_groups: u64,
    },

    /// A group recorded a vote for a snapshot/assignment candidate.
    VoteRecorded {
        kind: u64,
        vote: Address,
        voting_epoch: EpochNumber,
        target_epoch: EpochNumber,
        hash: Hash,
        group: GroupIndex,
        signer_count: u64,
        signed_groups: u64,
        total_groups: u64,
        signers: [u8; 8],
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
    pub blob: Option<BlobEncoding>,
}

/// Single replay event emitted during block processing, with associated metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct ReplayRecord {
    /// Transaction id that produced the replay event.
    pub tx_id: Txid,
    /// Authority/operator/owner when it is directly known from the instruction.
    pub actor: Option<Address>,
    /// Deterministic state transition.
    pub event: ReplayableEvent,
}

/// A single slot's replay records within a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(Serialize, Deserialize, SchemaRead, SchemaWrite))]
pub struct SnapshotEntry {
    /// Slot in which these events occurred.
    pub slot: SlotNumber,
    /// Slot wall-clock timestamp when available from Solana RPC.
    pub block_time: Option<i64>,
    /// Records emitted during this slot, in processing order.
    pub records: Vec<ReplayRecord>,
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
    use bytemuck::Zeroable;
    use crate::spooler::GroupIndex;
    use crate::track::types::{TrackKind, TrackState};
    use tape_crypto::hash::Hash;
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
                kind: TrackKind::Inline as u64,
                state: TrackState::Certified as u64,
                size: 1u64.into(),
                group: GroupIndex::from(0),
                value_hash: Hash::default(),
            },
            epoch: EpochNumber(10),
            blob: None,
        }
    }

    fn record(event: ReplayableEvent) -> ReplayRecord {
        ReplayRecord {
            tx_id: Txid::default(),
            actor: None,
            event,
        }
    }

    #[cfg(feature = "wincode")]
    fn blob_replay_track() -> ReplayTrack {
        ReplayTrack {
            state: CompressedTrack {
                tape: Address::from([2u8; 32]),
                key: Hash::from([3u8; 32]),
                track_number: 1u64.into(),
                kind: TrackKind::Coded as u64,
                state: TrackState::Registered as u64,
                size: 1024u64.into(),
                group: GroupIndex::from(1),
                value_hash: Hash::from([4u8; 32]),
            },
            epoch: EpochNumber(11),
            blob: Some(BlobEncoding {
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
                timestamp: 0,
                total_stake: TAPE(0),
                committee_count: 0,
                preferences: NodePreferences::zeroed(),
                subsidy: TAPE(0),
                nonce: Hash::default(),
            },
            ReplayableEvent::SyncSpool {
                node: Address::from([5u8; 32]),
                epoch: EpochNumber(10),
                group: GroupIndex(0),
                spool: SpoolIndex::from(1),
            },
            ReplayableEvent::ReserveTape {
                tape: Address::from([6u8; 32]),
                id: TapeNumber(1),
                flags: 0,
                authority: Address::from([7u8; 32]),
                capacity: StorageUnits::from_bytes(1024),
                active_epoch: EpochNumber(10),
                expiry_epoch: EpochNumber(20),
                cost: TAPE(0),
                burned: TAPE(0),
                scheduled: TAPE(0),
            },
            ReplayableEvent::DestroyTape {
                tape: Address::from([8u8; 32]),
                epoch: EpochNumber(10),
            },
            ReplayableEvent::RegisterNode {
                authority: Address::from([9u8; 32]),
                node: Address::from([10u8; 32]),
                id: NodeId(1),
            },
            ReplayableEvent::JoinCommittee {
                node: Address::from([11u8; 32]),
                stake: TAPE(0),
                key: BlsPubkey::zeroed(),
                preferences: NodePreferences::zeroed(),
                activation_epoch: EpochNumber(0),
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
                    block_time: Some(1_700_000_000),
                    records: vec![record(ReplayableEvent::AdvanceEpoch {
                        old_epoch: EpochNumber(41),
                        new_epoch: EpochNumber(42),
                        timestamp: 0,
                        total_stake: TAPE(0),
                        committee_count: 0,
                        preferences: NodePreferences::zeroed(),
                        subsidy: TAPE(0),
                        nonce: Hash::default(),
                    })],
                },
                SnapshotEntry {
                    slot: SlotNumber(150),
                    block_time: Some(1_700_000_050),
                    records: vec![
                        record(ReplayableEvent::Track(ReplayTrack {
                            epoch: EpochNumber(42),
                            ..raw_replay_track()
                        })),
                        record(ReplayableEvent::CertifyTrack {
                            track: Address::from([1u8; 32]),
                            epoch: EpochNumber(42),
                        }),
                    ],
                },
            ],
        };

        assert_eq!(log.epoch, EpochNumber(42));
        assert_eq!(log.entries.len(), 2);
        assert_eq!(log.entries[1].records.len(), 2);
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
                block_time: Some(1_700_000_050),
                records: vec![
                    record(ReplayableEvent::AdvanceEpoch {
                        old_epoch: EpochNumber(41),
                        new_epoch: EpochNumber(42),
                        timestamp: 0,
                        total_stake: TAPE(0),
                        committee_count: 0,
                        preferences: NodePreferences::zeroed(),
                        subsidy: TAPE(0),
                        nonce: Hash::default(),
                    }),
                    record(ReplayableEvent::Track(ReplayTrack {
                        state: CompressedTrack {
                            tape: Address::from([0xAB; 32]),
                            ..raw_replay_track().state
                        },
                        epoch: EpochNumber(42),
                        ..raw_replay_track()
                    })),
                    record(ReplayableEvent::SyncSpool {
                        node: Address::from([0xCD; 32]),
                        epoch: EpochNumber(42),
                        group: GroupIndex(0),
                        spool: SpoolIndex::from(7),
                    }),
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
                stake: TAPE(0),
                key: BlsPubkey::zeroed(),
                preferences: NodePreferences::zeroed(),
                activation_epoch: EpochNumber(0),
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
                    block_time: Some(1_700_000_000),
                    records: vec![record(ReplayableEvent::AdvanceEpoch {
                        old_epoch: EpochNumber(41),
                        new_epoch: EpochNumber(42),
                        timestamp: 0,
                        total_stake: TAPE(0),
                        committee_count: 0,
                        preferences: NodePreferences::zeroed(),
                        subsidy: TAPE(0),
                        nonce: Hash::default(),
                    })],
                },
                SnapshotEntry {
                    slot: SlotNumber(150),
                    block_time: Some(1_700_000_050),
                    records: vec![
                        record(ReplayableEvent::Track(blob_replay_track())),
                        record(ReplayableEvent::CertifyTrack {
                            track: Address::from([1u8; 32]),
                            epoch: EpochNumber(42),
                        }),
                        record(ReplayableEvent::SyncSpool {
                            node: Address::from([0xCD; 32]),
                            epoch: EpochNumber(42),
                            group: GroupIndex(0),
                            spool: SpoolIndex::from(7),
                        }),
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

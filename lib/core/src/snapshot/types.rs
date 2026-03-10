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

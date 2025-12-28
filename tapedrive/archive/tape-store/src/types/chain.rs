//! Storage representations of on-chain state
//!
//! These types mirror on-chain account data but are optimized for storage with serde/wincode
//! serialization. They are named with a `Data` suffix to distinguish them from the on-chain
//! zero-copy POD types.

use super::impls::Pubkey;
use serde::{Deserialize, Serialize};
use tape_api::state::{Tape, Track};
use tape_core::system::{Committee, CommitteeMember};
use tape_core::types::{EpochNumber, StorageUnits, TapeNumber, TrackNumber};
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Storage representation of on-chain tape account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeData {
    pub id: TapeNumber,
    pub authority: Pubkey,
    pub capacity: u64,
    pub used: u64,
    pub active_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
    pub track_count: u64,
}

/// Storage representation of on-chain track account data
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackData {
    pub id: TrackNumber,
    pub tape: Pubkey,
    pub key: Hash,
    pub size: u64,
    pub registered_epoch: EpochNumber,
    pub certified_epoch: EpochNumber,
    pub commitment_hash: Hash,
}

/// Storage representation of committee data for an epoch
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeData {
    pub epoch: EpochNumber,
    pub members: Vec<CommitteeMember>,
    pub total_stake: u64,
}

// ============================================================================
// From implementations: API types -> Store types
// ============================================================================

impl From<&Tape> for TapeData {
    fn from(tape: &Tape) -> Self {
        Self {
            id: tape.id,
            authority: tape.authority.into(),
            capacity: tape.capacity.0,
            used: tape.used.0,
            active_epoch: tape.active_epoch,
            expiry_epoch: tape.expiry_epoch,
            track_count: tape.track_count,
        }
    }
}

impl From<Tape> for TapeData {
    fn from(tape: Tape) -> Self {
        Self::from(&tape)
    }
}

impl From<&Track> for TrackData {
    fn from(track: &Track) -> Self {
        Self {
            id: track.id,
            tape: track.tape.into(),
            key: track.key,
            size: track.size.0,
            registered_epoch: track.data.registered_epoch,
            certified_epoch: track.data.state.certified_epoch,
            commitment_hash: track.data.commitment_hash,
        }
    }
}

impl From<Track> for TrackData {
    fn from(track: Track) -> Self {
        Self::from(&track)
    }
}

impl CommitteeData {
    /// Create CommitteeData from a Committee and epoch
    pub fn from_committee<const N: usize>(committee: &Committee<N>, epoch: EpochNumber) -> Self {
        let members: Vec<CommitteeMember> = committee.iter().cloned().collect();
        let total_stake = committee.total_stake().0;
        Self {
            epoch,
            members,
            total_stake,
        }
    }

    /// Convert back to a Committee with the specified capacity.
    /// Members beyond capacity N will be truncated.
    pub fn to_committee<const N: usize>(&self) -> Committee<N> {
        Committee::from_members(&self.members)
    }
}

// ============================================================================
// From implementations: Store types -> API types
// ============================================================================

impl From<&TapeData> for Tape {
    fn from(data: &TapeData) -> Self {
        Self {
            id: data.id,
            authority: solana_program::pubkey::Pubkey::new_from_array(data.authority.0),
            capacity: StorageUnits(data.capacity),
            used: StorageUnits(data.used),
            active_epoch: data.active_epoch,
            expiry_epoch: data.expiry_epoch,
            track_count: data.track_count,
        }
    }
}

impl From<TapeData> for Tape {
    fn from(data: TapeData) -> Self {
        Self::from(&data)
    }
}

impl From<&TrackData> for Track {
    fn from(data: &TrackData) -> Self {
        use tape_core::tape::TrackData as CoreTrackData;
        use tape_core::tape::TrackState;
        Self {
            id: data.id,
            tape: solana_program::pubkey::Pubkey::new_from_array(data.tape.0),
            key: data.key,
            size: StorageUnits(data.size),
            data: CoreTrackData {
                state: TrackState {
                    phase: 0,
                    certified_epoch: data.certified_epoch,
                },
                registered_epoch: data.registered_epoch,
                commitment_hash: data.commitment_hash,
            },
        }
    }
}

impl From<TrackData> for Track {
    fn from(data: TrackData) -> Self {
        Self::from(&data)
    }
}

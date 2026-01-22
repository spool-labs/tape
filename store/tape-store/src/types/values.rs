//! Value types for tape-store columns
//!
//! These structs are stored as values in the various column families.

use crate::types::{EncodingType, Pubkey, SliceType};
use serde::{Deserialize, Serialize};
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Information about a blob's erasure coding structure
///
/// Contains the hashes needed to verify slices for a given track.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceInfo {
    /// Type of erasure encoding used
    pub encoding_type: EncodingType,
    /// Original unencoded data length in bytes
    pub unencoded_length: u64,
    /// Hashes for primary slices (up to 1024, one per slice)
    /// Empty for some encoding types that don't use individual hashes
    pub primary: Vec<Hash>,
    /// Column roots for recovery slices (up to 1024, one per recovery column)
    /// Each column has 1024 parts; this stores the root hash
    /// Empty if no recovery layer is used
    pub recovery: Vec<Hash>,
}

impl Default for SliceInfo {
    fn default() -> Self {
        Self {
            encoding_type: EncodingType::Unknown,
            unencoded_length: 0,
            primary: Vec::new(),
            recovery: Vec::new(),
        }
    }
}

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeInfo {
    /// Epoch when the tape became active
    pub active_epoch: EpochNumber,
    /// Epoch when the tape expires
    pub expiry_epoch: EpochNumber,
    /// Authority pubkey that owns this tape
    pub authority: Pubkey,
}

/// Metadata about a track (individual blob)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TrackInfo {
    /// Whether slice info has been received for this track
    pub has_slice_info: bool,
    /// Address of the tape this track belongs to
    pub tape_address: Pubkey,
    /// Epoch when the track was registered on-chain
    pub registered_epoch: EpochNumber,
    /// Epoch when the track was certified (None if not yet certified)
    pub certified_epoch: Option<EpochNumber>,
}

impl TrackInfo {
    pub fn new(tape_address: Pubkey, registered_epoch: EpochNumber) -> Self {
        Self {
            has_slice_info: false,
            tape_address,
            registered_epoch,
            certified_epoch: None,
        }
    }
}

/// Sync progress for a spool within an epoch
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SyncProgress {
    /// Last track address that was synced (None if just starting)
    pub last_synced_track: Option<Pubkey>,
    /// Type of slice being synced
    pub slice_type: SliceType,
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self {
            last_synced_track: None,
            slice_type: SliceType::Primary,
        }
    }
}

/// Primary slice data (erasure-coded fragment)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct PrimarySliceData {
    /// Encoded symbols (typically ~1MB)
    pub symbols: Vec<u8>,
    /// Number of padding bytes added during encoding
    pub padding_len: u32,
}

impl PrimarySliceData {
    pub fn new(symbols: Vec<u8>, padding_len: u32) -> Self {
        Self {
            symbols,
            padding_len,
        }
    }
}

/// Recovery slice data (packed recovery column)
///
/// Each recovery column contains parts from all 1024 primary slices,
/// allowing reconstruction of any missing primary slice.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RecoverySliceData {
    /// Packed column symbols (typically ~1MB)
    pub symbols: Vec<u8>,
    /// Number of padding bytes
    pub padding_len: u32,
}

impl RecoverySliceData {
    pub fn new(symbols: Vec<u8>, padding_len: u32) -> Self {
        Self {
            symbols,
            padding_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_info_default() {
        let info = SliceInfo::default();
        assert_eq!(info.encoding_type, EncodingType::Unknown);
        assert_eq!(info.unencoded_length, 0);
        assert!(info.primary.is_empty());
        assert!(info.recovery.is_empty());
    }

    #[test]
    fn test_slice_info_roundtrip() {
        let info = SliceInfo {
            encoding_type: EncodingType::Rotated,
            unencoded_length: 1024 * 1024,
            primary: vec![Hash::default(); 1024],
            recovery: vec![Hash::default(); 1024],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: SliceInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let info = TapeInfo {
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            authority: Pubkey([0xAB; 32]),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TapeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_info_new() {
        let tape = Pubkey([1u8; 32]);
        let epoch = EpochNumber(50);

        let info = TrackInfo::new(tape, epoch);
        assert!(!info.has_slice_info);
        assert_eq!(info.tape_address, tape);
        assert_eq!(info.registered_epoch, epoch);
        assert!(info.certified_epoch.is_none());
    }

    #[test]
    fn test_track_info_roundtrip() {
        let info = TrackInfo {
            has_slice_info: true,
            tape_address: Pubkey([1u8; 32]),
            registered_epoch: EpochNumber(100),
            certified_epoch: Some(EpochNumber(101)),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TrackInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_sync_progress_default() {
        let progress = SyncProgress::default();
        assert!(progress.last_synced_track.is_none());
        assert_eq!(progress.slice_type, SliceType::Primary);
    }

    #[test]
    fn test_sync_progress_roundtrip() {
        let progress = SyncProgress {
            last_synced_track: Some(Pubkey([0xFF; 32])),
            slice_type: SliceType::Recovery,
        };

        let bytes = wincode::serialize(&progress).unwrap();
        let decoded: SyncProgress = wincode::deserialize(&bytes).unwrap();
        assert_eq!(progress, decoded);
    }

    #[test]
    fn test_primary_slice_data_roundtrip() {
        let data = PrimarySliceData::new(vec![0xAB; 1024], 128);

        let bytes = wincode::serialize(&data).unwrap();
        let decoded: PrimarySliceData = wincode::deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_recovery_slice_data_roundtrip() {
        let data = RecoverySliceData::new(vec![0xCD; 2048], 64);

        let bytes = wincode::serialize(&data).unwrap();
        let decoded: RecoverySliceData = wincode::deserialize(&bytes).unwrap();
        assert_eq!(data, decoded);
    }
}

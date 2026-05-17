//! Value types for tape-store columns

use serde::{Deserialize, Serialize};
use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{EpochNumber, SpoolIndex, TrackNumber};
use wincode::containers::{Pod, Vec as WincodeVec};
use wincode::len::BincodeLen;
use wincode_derive::{SchemaRead, SchemaWrite};

const SLICE_BYTES_LIMIT: usize = 10 * 1024 * 1024;

/// A wrapper around a byte vector with a widened decode limit for track slice data.
type SliceBytes = WincodeVec<Pod<u8>, BincodeLen<SLICE_BYTES_LIMIT>>;

/// Stored slice bytes with a widened decode limit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SliceValue(#[wincode(with = "SliceBytes")] pub Vec<u8>);

/// Snapshot build artifact retained until the corresponding `WriteSnapshot`
/// event lands locally and the staged slice is flushed into `SliceCol`.
///
/// `spool_index` is the exact key the slice belongs at, captured at build time
/// so the event handler doesn't re-derive it from protocol state. The bytes in
/// `slice` are the Clay slice at position (`spool_index - group.base()`); they
/// verify against `blob.leaves[position]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotArtifact {
    pub blob: BlobInfo,
    pub spool_index: SpoolIndex,
    #[wincode(with = "SliceBytes")]
    pub slice: Vec<u8>,
}

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeInfo {
    /// Epoch when the tape expires
    pub end_epoch: EpochNumber,

    /// Next monotonic track number expected for this tape.
    pub next_track_number: TrackNumber,
}

/// Proof data needed to submit an on-chain track invalidation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct InvalidationProof {
    pub bitmap: u128,
    pub signature: BlsSignature,
    pub computed_root: [u8; 32],
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, GROUP_SIZE};
    use tape_core::track::blob::BlobInfo;
    use tape_core::track::types::{CompressedTrack, PackedTrack};
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;

    #[test]
    fn test_tape_info_roundtrip() {
        let info = TapeInfo {
            end_epoch: EpochNumber(200),
            next_track_number: TrackNumber(0),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TapeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_packed_track_roundtrip() {
        let info: PackedTrack = [1u8; core::mem::size_of::<CompressedTrack>()];

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: PackedTrack = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_blob_data_roundtrip() {
        let info = BlobInfo {
            size: StorageUnits(512),
            commitment: Hash::from([3u8; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
            leaves: [Hash::default(); GROUP_SIZE],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: BlobInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_blob_commitment_root() {
        let leaves = [Hash::default(); GROUP_SIZE];
        let info = BlobInfo {
            size: StorageUnits(1024),
            commitment: root_from_leaf_hashes::<{ SLICE_TREE_HEIGHT }>(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(1),
            leaves,
        };

        assert_eq!(info.commitment_root(), info.commitment);
    }
}

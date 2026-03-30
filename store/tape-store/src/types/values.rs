//! Value types for tape-store columns

use serde::{Deserialize, Serialize};
use tape_core::bls::BlsSignature;
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

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

/// Snapshot chunk encoding metadata (stored during build, consumed during registration).
///
/// This is intentionally separate from the compressed-track catalog: snapshots are built before
/// on-chain registration creates any track state, and we only store local slices
/// (not all group slices). Persisting this metadata lets `RegisterSnapshot` resume
/// after crashes without re-running full snapshot encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotChunkMeta {
    /// Per-slice leaf hashes (SPOOL_GROUP_SIZE entries)
    pub leaves: Vec<Hash>,
    /// Stripe size used during encoding
    pub stripe_size: u64,
    /// Number of stripes
    pub stripe_count: u64,
    /// Encoding type discriminant
    pub encoding_type: u64,
    /// Encoding params
    pub encoding_params: u64,
}

/// Collected BLS certification for a snapshot chunk
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotCertResult {
    /// Committee member indices that signed
    pub member_indices: Vec<u8>,
    /// Aggregated BLS signature bytes
    pub signature: BlsSignature,
    /// Epoch of the certification
    pub epoch: u64,
}

/// Single partial BLS signature for a snapshot chunk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotPartialSignature {
    /// Committee member index that produced this signature.
    pub member_index: u8,
    /// Partial BLS signature bytes.
    pub signature: BlsSignature,
    /// Snapshot target epoch for this signature.
    pub epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

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
            root: Hash::from([2u8; 32]),
            commitment: Hash::from([3u8; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: 64,
            stripe_count: 2,
            leaves: [Hash::default(); SPOOL_GROUP_SIZE],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: BlobInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_blob_commitment_root() {
        let leaves = [Hash::default(); SPOOL_GROUP_SIZE];
        let info = BlobInfo {
            size: StorageUnits(1024),
            root: Hash::default(),
            commitment: tape_crypto::merkle::root_from_leaf_hashes::<
                { tape_core::erasure::COMMITMENT_TREE_HEIGHT },
            >(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: 128,
            stripe_count: 1,
            leaves,
        };

        assert_eq!(info.commitment_root(), info.commitment);
    }

    #[test]
    fn partial_signature_roundtrip() {
        use tape_crypto::bls12254::min_sig::G1CompressedPoint;

        let sig = SnapshotPartialSignature {
            member_index: 3,
            signature: BlsSignature(G1CompressedPoint([0x55; 32])),
            epoch: 42,
        };

        let bytes = wincode::serialize(&sig).unwrap();
        let decoded: SnapshotPartialSignature = wincode::deserialize(&bytes).unwrap();
        assert_eq!(sig, decoded);
    }
}

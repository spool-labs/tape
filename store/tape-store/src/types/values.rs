//! Value types for tape-store columns

use serde::{Deserialize, Serialize};
use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobEncoding;
use tape_core::types::{
    ContentType, EpochNumber, SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber,
};
use tape_crypto::address::Address;
use tape_crypto::Hash;
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
/// `slice` are the Clay slice at position (`spool_index - group.base_spool()`); they
/// verify against `blob.leaves[position]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SnapshotArtifact {
    pub blob: BlobEncoding,
    pub spool_index: SpoolIndex,
    #[wincode(with = "SliceBytes")]
    pub slice: Vec<u8>,
}

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TapeInfo {
    /// Unique tape identifier.
    pub id: TapeNumber,

    /// Tape behavior flags.
    pub flags: u64,

    /// Epoch when the tape expires
    pub end_epoch: EpochNumber,

    /// Next monotonic track number expected for this tape.
    pub next_track_number: TrackNumber,
}

impl TapeInfo {
    pub fn new(id: TapeNumber, flags: u64, end_epoch: EpochNumber, next_track_number: TrackNumber) -> Self {
        Self {
            id,
            flags,
            end_epoch,
            next_track_number,
        }
    }
}

/// Proof data needed to submit an on-chain track invalidation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct InvalidationProof {
    pub bitmap: u128,
    pub signature: BlsSignature,
    pub computed_root: [u8; 32],
}

/// Listing-plane metadata for one object, keyed in `object_list` by
/// `[bucket][name]`. Carries exactly what an S3 listing page returns per object
/// (size, etag, last-modified) plus a pointer to the object track, so a listing
/// is a single range scan with no per-object lookups.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ObjectListEntry {
    /// Object size in bytes.
    pub size: StorageUnits,
    /// ETag: the object track's commitment / content root.
    pub etag: Hash,
    /// Wall-clock last-modified time (unix seconds), when the block had one.
    pub block_time: Option<i64>,
    /// Slot the write was applied at — the precise monotonic order/tiebreak.
    pub slot: SlotNumber,
    /// Data tape holding the object-representing track.
    pub data_tape: Address,
    /// Track number of the object-representing track on `data_tape`.
    pub track_number: TrackNumber,
    /// Storage kind discriminator (`TrackKind::Inline` / `TrackKind::Coded`).
    pub kind: u64,
    /// Hot content type; precise custom strings are deferred to the data plane.
    pub content_type: ContentType,
}

/// Name metadata keyed by object track address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ObjectMetadata {
    /// Plaintext object name as provided on the write path.
    pub name: Vec<u8>,
    /// Hot content type.
    pub content_type: ContentType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SLICE_TREE_HEIGHT, GROUP_SIZE};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, PackedTrack};
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;

    #[test]
    fn object_list_entry_roundtrip() {
        let entry = ObjectListEntry {
            size: StorageUnits(4096),
            etag: Hash::from([7u8; 32]),
            block_time: Some(1_700_000_123),
            slot: SlotNumber(42),
            data_tape: Address::new([9u8; 32]),
            track_number: TrackNumber(3),
            kind: 1,
            content_type: ContentType::ImageJpeg,
        };
        let bytes = wincode::serialize(&entry).unwrap();
        let decoded: ObjectListEntry = wincode::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn object_metadata_roundtrip() {
        let metadata = ObjectMetadata {
            name: b"photos/cat.jpg".to_vec(),
            content_type: ContentType::ImageJpeg,
        };

        let bytes = wincode::serialize(&metadata).unwrap();
        let decoded: ObjectMetadata = wincode::deserialize(&bytes).unwrap();
        assert_eq!(metadata, decoded);
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let info = TapeInfo {
            id: TapeNumber(1),
            flags: 0,
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
        let info = BlobEncoding {
            size: StorageUnits(512),
            commitment: Hash::from([3u8; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
            leaves: [Hash::default(); GROUP_SIZE],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: BlobEncoding = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn test_track_blob_commitment_root() {
        let leaves = [Hash::default(); GROUP_SIZE];
        let info = BlobEncoding {
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

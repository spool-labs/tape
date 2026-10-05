//! Chunk manifest for multi-track stream storage.
//!
//! When a byte stream exceeds the single-track size limit, the SDK splits it
//! into chunks stored across multiple tracks. A manifest track is written last
//! so the original stream can be reconstructed during reads.

use serde::{Deserialize, Serialize};
use tape_core::encoding::ErasureCode;
use tape_core::track::TRACK_TREE_HEIGHT;
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_protocol::api::SLICE_BYTES_LIMIT;
use tape_slicer::{stripe_chunk_len, SliceMetadata, STRIPE_CAP};
use wincode_derive::{SchemaRead, SchemaWrite};

use super::error::StreamError;

/// Manifest format version.
pub const MANIFEST_VERSION: u8 = 1;

/// Ceiling on a direct coded track / stream chunk track, on every network.
///
/// 2^SUB_TREE_HEIGHT sample leaves of SUB_LEAF_BYTES, which is the largest
/// slice the sub-leaf commitment can cover. Tapenet's profile fits 70 MiB in
/// 10 MiB slices, so tapenet's largest track is this ceiling. A profile
/// with a smaller k gets less: see `max_track_size`.
pub const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

/// Maximum track slots in one tape.
pub const MAX_TRACKS_PER_TAPE: u64 = 1_u64 << TRACK_TREE_HEIGHT;

/// Maximum data chunks in a single-tape stream.
///
/// One track is reserved for the manifest.
pub const MAX_SINGLE_TAPE_STREAM_CHUNKS: u64 = MAX_TRACKS_PER_TAPE - 1;

/// Largest direct coded track / stream chunk track a network's profile allows.
///
/// A slice holds one chunk per stripe plus its metadata, and a node accepts
/// at most `SLICE_BYTES_LIMIT` per slice. Counting whole stripes at the
/// stripe cap keeps every smaller track inside that limit too.
pub fn max_track_size(profile: ErasureCode) -> usize {
    let chunk = stripe_chunk_len(profile, STRIPE_CAP, STRIPE_CAP);
    let stripes = (SLICE_BYTES_LIMIT - SliceMetadata::SIZE) / chunk;

    (stripes * STRIPE_CAP).min(MAX_TRACK_SIZE)
}

/// Maximum payload bytes for one stream stored on an otherwise empty tape.
pub fn max_single_tape_stream_size(profile: ErasureCode) -> u64 {
    max_track_size(profile) as u64 * MAX_SINGLE_TAPE_STREAM_CHUNKS
}

/// Describes a byte stream stored across one or more tracks on a tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChunkManifest {
    /// Format version for future evolution.
    pub version: u8,
    /// Original stream size in bytes.
    pub total_size: StorageUnits,
    /// Number of chunk tracks.
    pub chunk_count: TrackNumber,
    /// Bytes per chunk (the last chunk may be smaller).
    pub chunk_size: StorageUnits,
    /// Stream-level content key provided by the caller.
    pub key: Hash,
    /// Ordered chunk entries.
    pub chunks: Vec<ChunkEntry>,
}

/// One chunk within a manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChunkEntry {
    /// Address of the chunk's track.
    pub track: Address,
    /// Byte offset of this chunk in the original stream.
    pub offset: StorageUnits,
    /// Actual byte count stored in this chunk.
    pub size: StorageUnits,
}

impl ChunkManifest {
    /// Serialize to bytes using wincode.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StreamError> {
        wincode::serialize(self).map_err(|error| StreamError::Manifest(error.to_string()))
    }

    /// Deserialize and validate from bytes using wincode.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StreamError> {
        let manifest: Self = wincode::deserialize(bytes)
            .map_err(|error| StreamError::Manifest(error.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Check manifest invariants.
    fn validate(&self) -> Result<(), StreamError> {
        if self.version != MANIFEST_VERSION {
            return Err(StreamError::Manifest(format!(
                "unsupported manifest version: got {}, expected {MANIFEST_VERSION}",
                self.version
            )));
        }

        if self.chunk_count.as_usize() != self.chunks.len() {
            return Err(StreamError::Manifest(format!(
                "chunk_count mismatch: header says {}, but {} entries present",
                self.chunk_count,
                self.chunks.len()
            )));
        }

        if self.chunks.is_empty() {
            return Err(StreamError::Manifest("manifest has no chunks".into()));
        }

        let mut expected_offset = StorageUnits::zero();
        for (chunk_index, entry) in self.chunks.iter().enumerate() {
            if entry.offset != expected_offset {
                return Err(StreamError::Manifest(format!(
                    "chunk {chunk_index} offset mismatch: expected {expected_offset}, got {}",
                    entry.offset
                )));
            }

            if entry.size.is_zero() {
                return Err(StreamError::Manifest(format!(
                    "chunk {chunk_index} has zero size",
                )));
            }

            expected_offset = expected_offset
                .checked_add(entry.size)
                .ok_or_else(|| StreamError::Manifest(format!("chunk {chunk_index} offset overflow")))?;
        }

        if expected_offset != self.total_size {
            return Err(StreamError::Manifest(format!(
                "total_size mismatch: chunks sum to {expected_offset}, header says {}",
                self.total_size
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest(chunk_count: usize) -> ChunkManifest {
        let chunks: Vec<ChunkEntry> = (0..chunk_count)
            .map(|chunk_index| ChunkEntry {
                track: Address::new_unique(),
                offset: StorageUnits::from_bytes(chunk_index as u64 * MAX_TRACK_SIZE as u64),
                size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
            })
            .collect();

        ChunkManifest {
            version: MANIFEST_VERSION,
            total_size: StorageUnits::from_bytes(chunk_count as u64 * MAX_TRACK_SIZE as u64),
            chunk_count: TrackNumber(chunk_count as u64),
            chunk_size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
            key: Hash::from([0xAB; 32]),
            chunks,
        }
    }

    // wincode roundtrips a valid manifest.
    #[test]
    fn roundtrip() {
        let manifest = sample_manifest(16);
        let bytes = manifest.to_bytes().expect("serialize manifest");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(manifest, recovered);
    }

    // single-chunk manifests preserve their single entry.
    #[test]
    fn single() {
        let manifest = sample_manifest(1);
        let bytes = manifest.to_bytes().expect("serialize manifest");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(recovered.chunk_count, TrackNumber(1));
        assert_eq!(recovered.chunks.len(), 1);
    }

    // large manifests still fit inside one blob track.
    #[test]
    fn max() {
        let manifest = sample_manifest(65_535);
        let bytes = manifest.to_bytes().expect("serialize manifest");
        assert!(bytes.len() < 4 * 1024 * 1024, "manifest should fit in a single blob track");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(recovered.chunk_count, TrackNumber(65_535));
    }

    // a short final chunk survives manifest validation.
    #[test]
    fn smaller() {
        let total_size = StorageUnits::from_bytes(MAX_TRACK_SIZE as u64 * 3 + 1000);
        let manifest = ChunkManifest {
            version: MANIFEST_VERSION,
            total_size,
            chunk_count: TrackNumber(4),
            chunk_size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
            key: Hash::from([0xCD; 32]),
            chunks: vec![
                ChunkEntry {
                    track: Address::new_unique(),
                    offset: StorageUnits::zero(),
                    size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
                },
                ChunkEntry {
                    track: Address::new_unique(),
                    offset: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
                    size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
                },
                ChunkEntry {
                    track: Address::new_unique(),
                    offset: StorageUnits::from_bytes(2 * MAX_TRACK_SIZE as u64),
                    size: StorageUnits::from_bytes(MAX_TRACK_SIZE as u64),
                },
                ChunkEntry {
                    track: Address::new_unique(),
                    offset: StorageUnits::from_bytes(3 * MAX_TRACK_SIZE as u64),
                    size: StorageUnits::from_bytes(1000),
                },
            ],
        };

        let bytes = manifest.to_bytes().expect("serialize manifest");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(recovered.chunks[3].size, StorageUnits::from_bytes(1000));
    }

    // stripe sizing holds across the whole track range: a full track still
    // splits into stripes at the cap, so peak encode memory does not grow
    // with the track size.
    #[test]
    fn stripe_sizing_holds_at_track_cap() {
        use tape_core::encoding::TAPENET_CLAY;
        use tape_slicer::{derive_stripe_size, num_stripes};

        let align = TAPENET_CLAY.stripe_alignment() as usize;
        let stripe = derive_stripe_size(MAX_TRACK_SIZE, align, STRIPE_CAP);

        assert!(stripe <= STRIPE_CAP.div_ceil(align) * align);
        assert_eq!(num_stripes(MAX_TRACK_SIZE, stripe), 68);
    }

    // tapenet's largest track and largest single-tape stream stay where they were
    #[test]
    fn tapenet_track() {
        use tape_core::encoding::TAPENET_PROFILE;

        assert_eq!(max_track_size(TAPENET_PROFILE), 64 * 1024 * 1024);
        assert_eq!(
            max_single_tape_stream_size(TAPENET_PROFILE),
            64 * 1024 * 1024 * 65_535
        );
    }

    // a smaller k fits fewer bytes per track: about k x 10 MiB up to the ceiling
    #[test]
    fn small_tracks() {
        use tape_core::encoding::{ClayParams, RSParams, RepParams};

        let cases = [
            (ErasureCode::Replication(RepParams::new(2)), 10_000_000),
            (ErasureCode::Clay(ClayParams::new(4, 2, 3)), 20_000_000),
            (ErasureCode::ReedSolomon(RSParams::new(3, 1)), 31_000_000),
            (ErasureCode::ReedSolomon(RSParams::new(4, 2)), 41_000_000),
            (ErasureCode::ReedSolomon(RSParams::new(6, 2)), 62_000_000),
            (ErasureCode::ReedSolomon(RSParams::new(7, 3)), MAX_TRACK_SIZE),
        ];

        for (profile, expected) in cases {
            assert_eq!(max_track_size(profile), expected, "{profile:?}");
        }
    }

    // no track up to a profile's maximum codes to a slice a node would refuse
    #[test]
    fn slices_fit() {
        use tape_core::encoding::{ClayParams, RSParams, RepParams, TAPENET_PROFILE};
        use tape_slicer::{coded_slice_len, derive_stripe_size, num_stripes, Slicer, ErasureCoder};

        let profiles = [
            TAPENET_PROFILE,
            ErasureCode::Clay(ClayParams::new(4, 2, 3)),
            ErasureCode::Clay(ClayParams::new(6, 4, 5)),
            ErasureCode::ReedSolomon(RSParams::new(3, 1)),
            ErasureCode::ReedSolomon(RSParams::new(4, 2)),
            ErasureCode::ReedSolomon(RSParams::new(6, 2)),
            ErasureCode::Replication(RepParams::new(1)),
            ErasureCode::Replication(RepParams::new(2)),
        ];

        for profile in profiles {
            let align = Slicer::try_from_profile(profile).expect("valid profile").stripe_alignment();
            let max = max_track_size(profile);

            // Sizes on both sides of every stripe-count step, where padding peaks.
            let steps = (1..=max / STRIPE_CAP).flat_map(|count| {
                let edge = count * STRIPE_CAP;
                [edge - 1, edge, edge + 1]
            });
            for size in steps.chain([1, max - 1, max]).filter(|size| *size <= max) {
                let stripe = derive_stripe_size(size, align, STRIPE_CAP);
                let slice = coded_slice_len(profile, size, stripe, num_stripes(size, stripe));

                assert!(slice <= SLICE_BYTES_LIMIT, "{profile:?} size {size} slice {slice}");
            }
        }
    }
}

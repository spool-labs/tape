//! Chunk manifest for multi-track stream storage.
//!
//! When a byte stream exceeds the single-track size limit, the SDK splits it
//! into chunks stored across multiple tracks. A manifest track is written last
//! so the original stream can be reconstructed during reads.

use serde::{Deserialize, Serialize};
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

use super::error::StreamError;

/// Manifest format version.
pub const MANIFEST_VERSION: u8 = 1;

/// Maximum bytes per chunk track.
///
/// Clay(20,7) with 10 MiB slices gives a theoretical 70 MiB per track. The SDK
/// uses 64 MiB to leave headroom for stripe padding and metadata suffixes.
pub const CHUNK_SIZE: usize = 64 * 1024 * 1024;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChunkEntry {
    /// Track number on the tape that holds this chunk.
    pub track_number: TrackNumber,
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
                track_number: TrackNumber(chunk_index as u64),
                offset: StorageUnits::from_bytes(chunk_index as u64 * CHUNK_SIZE as u64),
                size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
            })
            .collect();

        ChunkManifest {
            version: MANIFEST_VERSION,
            total_size: StorageUnits::from_bytes(chunk_count as u64 * CHUNK_SIZE as u64),
            chunk_count: TrackNumber(chunk_count as u64),
            chunk_size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
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
        assert!(bytes.len() < 2 * 1024 * 1024, "manifest should fit in a single blob track");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(recovered.chunk_count, TrackNumber(65_535));
    }

    // a short final chunk survives manifest validation.
    #[test]
    fn smaller() {
        let total_size = StorageUnits::from_bytes(CHUNK_SIZE as u64 * 3 + 1000);
        let manifest = ChunkManifest {
            version: MANIFEST_VERSION,
            total_size,
            chunk_count: TrackNumber(4),
            chunk_size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
            key: Hash::from([0xCD; 32]),
            chunks: vec![
                ChunkEntry {
                    track_number: TrackNumber(0),
                    offset: StorageUnits::zero(),
                    size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
                },
                ChunkEntry {
                    track_number: TrackNumber(1),
                    offset: StorageUnits::from_bytes(CHUNK_SIZE as u64),
                    size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
                },
                ChunkEntry {
                    track_number: TrackNumber(2),
                    offset: StorageUnits::from_bytes(2 * CHUNK_SIZE as u64),
                    size: StorageUnits::from_bytes(CHUNK_SIZE as u64),
                },
                ChunkEntry {
                    track_number: TrackNumber(3),
                    offset: StorageUnits::from_bytes(3 * CHUNK_SIZE as u64),
                    size: StorageUnits::from_bytes(1000),
                },
            ],
        };

        let bytes = manifest.to_bytes().expect("serialize manifest");
        let recovered = ChunkManifest::from_bytes(&bytes).expect("deserialize manifest");
        assert_eq!(recovered.chunks[3].size, StorageUnits::from_bytes(1000));
    }
}

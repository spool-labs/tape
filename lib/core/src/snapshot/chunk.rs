use tape_crypto::Hash;
use tape_crypto::hash::hashv;

use crate::snapshot::error::SnapshotError;
use crate::spooler::GroupIndex;
use crate::types::{ChunkNumber, EpochNumber};

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";

/// Derives the track key for a snapshot chunk.
/// A single group may contribute multiple chunks per epoch.
#[inline]
pub fn snapshot_chunk_key(
    epoch: EpochNumber,
    group: GroupIndex,
    chunk: ChunkNumber,
) -> Hash {
    hashv(&[
        SNAPSHOT_KEY_V1,
        &epoch.pack(),
        &group.pack(),
        &chunk.pack(),
    ])
}

/// Wire payload fed into the inner Clay encoder for a single snapshot chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotChunkPayload {
    pub chunk: ChunkNumber,
    pub data: Vec<u8>,
}

impl SnapshotChunkPayload {
    pub const HEADER_SIZE: usize = 8;

    pub fn pack(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::HEADER_SIZE + self.data.len());
        out.extend_from_slice(&self.chunk.pack());
        out.extend_from_slice(&self.data);
        out
    }

    pub fn unpack(bytes: &[u8]) -> Result<Self, SnapshotError> {
        if bytes.len() < Self::HEADER_SIZE {
            return Err(SnapshotError::ChunkPayloadTooShort(bytes.len()));
        }

        let mut head = [0u8; 8];
        head.copy_from_slice(&bytes[..Self::HEADER_SIZE]);
        let chunk = ChunkNumber::unpack(head);

        Ok(Self {
            chunk,
            data: bytes[Self::HEADER_SIZE..].to_vec(),
        })
    }
}

/// Length-prefix size for a compressed segment passed to outer-RS encoding.
pub const SEGMENT_HEADER_SIZE: usize = 4;

/// Prepend a 4-byte little-endian length so the bootstrap decoder can strip
/// the outer-RS zero padding and recover the exact compressed bytes.
pub fn pack_segment(segment: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEGMENT_HEADER_SIZE + segment.len());
    out.extend_from_slice(&(segment.len() as u32).to_le_bytes());
    out.extend_from_slice(segment);
    out
}

/// Read the length prefix and return the segment slice without padding.
pub fn unpack_segment(packed: &[u8]) -> Result<&[u8], SnapshotError> {
    if packed.len() < SEGMENT_HEADER_SIZE {
        return Err(SnapshotError::ChunkPayloadTooShort(packed.len()));
    }
    let mut head = [0u8; SEGMENT_HEADER_SIZE];
    head.copy_from_slice(&packed[..SEGMENT_HEADER_SIZE]);
    let len = u32::from_le_bytes(head) as usize;
    let end = SEGMENT_HEADER_SIZE.saturating_add(len);
    if end > packed.len() {
        return Err(SnapshotError::ChunkPayloadTooShort(packed.len()));
    }
    Ok(&packed[SEGMENT_HEADER_SIZE..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_epoch_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(10), GroupIndex(3), ChunkNumber(0));
        assert_ne!(a, b);
    }

    #[test]
    fn distinguishes_group_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(9), GroupIndex(4), ChunkNumber(0));
        assert_ne!(a, b);
    }

    #[test]
    fn distinguishes_chunk_index() {
        let a = snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(1));
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_payload_round_trips() {
        let payload = SnapshotChunkPayload {
            chunk: ChunkNumber(7),
            data: vec![1, 2, 3, 4, 5],
        };
        let packed = payload.pack();
        assert_eq!(packed.len(), SnapshotChunkPayload::HEADER_SIZE + 5);
        let decoded = SnapshotChunkPayload::unpack(&packed).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn chunk_payload_rejects_short_input() {
        let short = [0u8; 4];
        let err = SnapshotChunkPayload::unpack(&short).unwrap_err();
        assert!(matches!(err, SnapshotError::ChunkPayloadTooShort(4)));
    }

    #[test]
    fn chunk_payload_distinguishes_chunks_with_same_data() {
        let a = SnapshotChunkPayload {
            chunk: ChunkNumber(0),
            data: vec![7u8; 32],
        };
        let b = SnapshotChunkPayload {
            chunk: ChunkNumber(1),
            data: vec![7u8; 32],
        };
        assert_ne!(a.pack(), b.pack());
    }

    #[test]
    fn segment_round_trips() {
        let segment = b"abcdefgh";
        let packed = pack_segment(segment);
        assert_eq!(packed.len(), SEGMENT_HEADER_SIZE + segment.len());
        assert_eq!(unpack_segment(&packed).unwrap(), segment);
    }

    #[test]
    fn segment_strips_trailing_padding() {
        let segment = b"hello";
        let mut padded = pack_segment(segment);
        padded.extend_from_slice(&[0u8; 12]);
        assert_eq!(unpack_segment(&padded).unwrap(), segment);
    }

    #[test]
    fn segment_rejects_short_or_truncated() {
        assert!(unpack_segment(&[0u8; 2]).is_err());
        // Length prefix claims 10 bytes but only 2 follow.
        let mut bad = (10u32).to_le_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 2]);
        assert!(unpack_segment(&bad).is_err());
    }
}

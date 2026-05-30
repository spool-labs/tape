//! Snapshot chunk format and outer erasure sizing.
//!
//! The inner Clay coder operates on a [`SnapshotChunkPayload`]; the outer RS
//! coder operates on length-prefixed segments. Both the chunk key derivation
//! and the segment/outer sizing live here so the format is owned in one place.

use tape_core::spooler::GroupIndex;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::hash::hashv;
use tape_crypto::Hash;
use tape_slicer::MAX_CHUNK_BYTES;

use crate::SnapshotError;

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";

/// Derive the track key for a snapshot chunk. A single group may contribute
/// multiple chunks per epoch.
#[inline]
pub fn snapshot_chunk_key(epoch: EpochNumber, group: GroupIndex, chunk: ChunkNumber) -> Hash {
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

/// Prepend a 4-byte little-endian length so the decoder can strip the outer-RS
/// zero padding and recover the exact compressed bytes.
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

/// Snapshot outer RS data threshold is one third of the active group count,
/// rounded up. At n=50 this preserves the old fixed threshold: ceil(50/3)=17.
pub const SNAPSHOT_OUTER_DATA_DENOMINATOR: usize = 3;

/// Derive the snapshot outer RS `k` from the active group count.
pub const fn snapshot_outer_k(total_groups: usize) -> usize {
    if total_groups == 0 {
        0
    } else {
        total_groups.div_ceil(SNAPSHOT_OUTER_DATA_DENOMINATOR)
    }
}

/// Maximum compressed bytes carried by one snapshot outer RS segment.
pub const fn snapshot_max_segment_bytes(total_groups: usize) -> usize {
    let k = snapshot_outer_k(total_groups);
    if k == 0 {
        0
    } else {
        k * MAX_CHUNK_BYTES - SEGMENT_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_distinguishes_epoch_group_chunk() {
        let base = snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(0));
        assert_ne!(base, snapshot_chunk_key(EpochNumber(10), GroupIndex(3), ChunkNumber(0)));
        assert_ne!(base, snapshot_chunk_key(EpochNumber(9), GroupIndex(4), ChunkNumber(0)));
        assert_ne!(base, snapshot_chunk_key(EpochNumber(9), GroupIndex(3), ChunkNumber(1)));
    }

    #[test]
    fn chunk_payload_round_trips() {
        let payload = SnapshotChunkPayload {
            chunk: ChunkNumber(7),
            data: vec![1, 2, 3, 4, 5],
        };
        let packed = payload.pack();
        assert_eq!(packed.len(), SnapshotChunkPayload::HEADER_SIZE + 5);
        assert_eq!(SnapshotChunkPayload::unpack(&packed).unwrap(), payload);
    }

    #[test]
    fn chunk_payload_rejects_short_input() {
        let err = SnapshotChunkPayload::unpack(&[0u8; 4]).unwrap_err();
        assert!(matches!(err, SnapshotError::ChunkPayloadTooShort(4)));
    }

    #[test]
    fn segment_round_trips_and_strips_padding() {
        let segment = b"hello";
        let mut padded = pack_segment(segment);
        assert_eq!(unpack_segment(&padded).unwrap(), segment);
        padded.extend_from_slice(&[0u8; 12]);
        assert_eq!(unpack_segment(&padded).unwrap(), segment);
    }

    #[test]
    fn segment_rejects_short_or_truncated() {
        assert!(unpack_segment(&[0u8; 2]).is_err());
        let mut bad = (10u32).to_le_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 2]);
        assert!(unpack_segment(&bad).is_err());
    }

    #[test]
    fn snapshot_outer_k_scales_with_group_count() {
        assert_eq!(snapshot_outer_k(0), 0);
        assert_eq!(snapshot_outer_k(1), 1);
        assert_eq!(snapshot_outer_k(3), 1);
        assert_eq!(snapshot_outer_k(20), 7);
        assert_eq!(snapshot_outer_k(50), 17);
        assert_eq!(snapshot_outer_k(100), 34);
    }
}

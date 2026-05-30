//! Decode fetched snapshot slices back into a `SnapshotLog`. Inverse of the
//! encode path in the node's snapshot builder; carries no transport.

use std::collections::BTreeMap;

use tape_core::snapshot::chunk::{unpack_segment, SnapshotChunkPayload};
use tape_core::snapshot::replay::SnapshotLog;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_slicer::{snapshot_outer_k, ErasureCoder, OuterCoder, Slicer};

use crate::SnapshotError;

/// Minimum verified inner slices needed to Clay-decode one chunk.
pub const K_INNER: usize = 7;

/// Reject a chunk-track list that is empty, references the wrong tape, or
/// carries a non-blob track. Structural checks only; the cryptographic anchor is
/// [`crate::verify_snapshot_track_set`].
pub fn validate_snapshot_track_list(
    epoch: EpochNumber,
    tape: Address,
    tracks: &[CompressedTrack],
) -> Result<(), SnapshotError> {
    if tracks.is_empty() {
        return Err(SnapshotError::EmptyTrackList { epoch: epoch.0 });
    }

    for track in tracks {
        if track.tape != tape {
            return Err(SnapshotError::WrongTape { epoch: epoch.0 });
        }
        if !track.is_blob() {
            return Err(SnapshotError::NonBlobTrack { epoch: epoch.0 });
        }
    }

    Ok(())
}

/// The number of spool groups this snapshot was encoded across, derived from the
/// highest group index present in the track list.
pub fn snapshot_track_group_count(
    epoch: EpochNumber,
    tracks: &[CompressedTrack],
) -> Result<usize, SnapshotError> {
    let total_groups = tracks
        .iter()
        .map(|track| track.group.0 as usize + 1)
        .max()
        .unwrap_or(0);

    if total_groups == 0 {
        return Err(SnapshotError::NoGroups { epoch: epoch.0 });
    }

    Ok(total_groups)
}

/// Clay-decode one chunk's verified inner slices to its `(chunk, outer-symbol)`
/// pair. `slices` must hold at least [`K_INNER`] `(leaf_index, bytes)` entries.
pub fn decode_chunk_payload(
    slices: &[(usize, &[u8])],
) -> Result<(ChunkNumber, Vec<u8>), SnapshotError> {
    let mut slicer = Slicer::clay_default();
    let plaintext = slicer
        .decode(slices)
        .map_err(|e| SnapshotError::ClayDecode(e.to_string()))?;

    let payload = SnapshotChunkPayload::unpack(&plaintext)
        .map_err(|e| SnapshotError::ChunkPayload(e.to_string()))?;

    Ok((payload.chunk, payload.data))
}

/// Outer-RS-decode each segment and reassemble the compressed log, then
/// decompress and deserialize into a [`SnapshotLog`].
pub fn assemble_snapshot_log(
    symbols_by_segment: &BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>>,
    epoch: EpochNumber,
    total_groups: usize,
) -> Result<SnapshotLog, SnapshotError> {
    let segments = outer_decode_segments(symbols_by_segment, epoch, total_groups)?;
    decode_snapshot_log(segments, epoch)
}

/// Outer RS decode each segment into packed (length-prefixed) compressed bytes,
/// ordered by chunk. Chunks must be a contiguous `0..segment_count` range.
fn outer_decode_segments(
    symbols_by_segment: &BTreeMap<ChunkNumber, Vec<(usize, Vec<u8>)>>,
    epoch: EpochNumber,
    total_groups: usize,
) -> Result<Vec<Vec<u8>>, SnapshotError> {
    if symbols_by_segment.is_empty() {
        return Err(SnapshotError::NoChunks { epoch: epoch.0 });
    }

    let outer_k = snapshot_outer_k(total_groups);
    if outer_k == 0 {
        return Err(SnapshotError::NoGroups { epoch: epoch.0 });
    }

    let segment_count = symbols_by_segment
        .keys()
        .last()
        .map(|c| c.0 as usize + 1)
        .unwrap_or(0);
    for i in 0..segment_count {
        if !symbols_by_segment.contains_key(&ChunkNumber(i as u64)) {
            return Err(SnapshotError::MissingChunk {
                epoch: epoch.0,
                chunk: i,
            });
        }
    }

    let mut segments = Vec::with_capacity(segment_count);
    for (chunk, symbols) in symbols_by_segment {
        if symbols.len() < outer_k {
            return Err(SnapshotError::InsufficientGroups {
                epoch: epoch.0,
                chunk: chunk.0,
                got: symbols.len(),
                need: outer_k,
            });
        }
        let mut coder = OuterCoder::new(outer_k, total_groups);
        let refs: Vec<(usize, &[u8])> = symbols.iter().map(|(i, d)| (*i, d.as_slice())).collect();
        let packed = coder
            .decode(&refs)
            .map_err(|e| SnapshotError::OuterDecode(e.to_string()))?;
        segments.push(packed);
    }

    Ok(segments)
}

/// Strip each segment's length prefix, decompress via lz4, and deserialize.
fn decode_snapshot_log(
    segments: Vec<Vec<u8>>,
    epoch: EpochNumber,
) -> Result<SnapshotLog, SnapshotError> {
    let mut compressed = Vec::new();
    for packed in &segments {
        let segment =
            unpack_segment(packed).map_err(|e| SnapshotError::ChunkPayload(e.to_string()))?;
        compressed.extend_from_slice(segment);
    }

    let decompressed = lz4_flex::decompress_size_prepended(&compressed)
        .map_err(|e| SnapshotError::Decompress(e.to_string()))?;

    let log = SnapshotLog::from_bytes(&decompressed)
        .map_err(|e| SnapshotError::Deserialize(e.to_string()))?;

    if log.epoch != epoch {
        return Err(SnapshotError::EpochMismatch {
            expected: epoch.0,
            got: log.epoch.0,
        });
    }

    Ok(log)
}

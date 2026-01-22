//! NestedSlicer: two-layer erasure coding for fast single-slice repair.
//!
//! Outer layer (primaries): RotatedSlicer (striped RS + rotation for fairness)
//! Inner layer (recovery): RecoveryCodec per primary slice, producing a 1024×1024 matrix:
//!   rec[row_i][col_j] is the j-th recovery shard of primary slice i.
//!
//! Storage format: spool j stores packed recovery column:
//!   col_j = rec[0][j] || rec[1][j] || ... || rec[1023][j]
//!
//! Repair: to rebuild missing primary k, fetch rec[k][j] for many j (>= 683),
//! decode via RecoveryCodec, yielding primary_k bytes.

use crate::api::Slicer;
use crate::consts::{DATA_SLICES, MERKLE_HEIGHT, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::recovery::{RecoveryCodec, RecoveryError, RecoveryLayout, RowEncodeError};
use crate::rotated::RotatedSlicer;
use crate::types::{Blob, Slice};

use std::io::Write;
use tape_crypto::merkle::MerkleTree;
use tape_crypto::Hash;
use thiserror::Error;

/// Minimum segment size for nested encoding (32 MiB).
pub const MIN_SEGMENT_SIZE: usize = 32 << 20;

/// Merkle tree for one recovery column (1024 leaves).
type ColumnMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;

/// Result from encoding + streaming recovery columns.
#[derive(Clone, Debug)]
pub struct EncodeResult {
    /// Primary slice size in bytes (as produced by RotatedSlicer; includes its metadata suffix).
    pub primary_size: usize,
    /// Inner recovery shard size (bytes).
    pub shard_len: usize,
    /// Merkle root per recovery column (1024 roots).
    pub column_roots: [Hash; SLICE_COUNT],
}

/// Errors for nested upload/encode flows.
#[derive(Debug, Error)]
pub enum NestedEncodeError {
    #[error("primary encode error: {0}")]
    Primary(#[from] EncodeError),

    #[error("recovery encode error: {0}")]
    Recovery(#[from] RecoveryError),

    #[error("io error while writing columns: {0}")]
    Io(#[from] std::io::Error),

    #[error("expected {expected} column writers, got {actual}")]
    BadWritersLen { expected: usize, actual: usize },
}

/// Errors for nested repair/decode flows.
#[derive(Debug, Error)]
pub enum NestedDecodeError {
    #[error("primary decode error: {0}")]
    Primary(#[from] DecodeError),

    #[error("recovery decode error: {0}")]
    Recovery(#[from] RecoveryError),

    #[error("row index out of range: {row} (max {max})")]
    RowOutOfRange { row: usize, max: usize },
}

/// Two-layer erasure encoder for fast repair.
pub struct NestedSlicer {
    primary: RotatedSlicer,
    recovery: RecoveryCodec,
}

impl Default for NestedSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl NestedSlicer {
    pub fn new() -> Self {
        Self {
            primary: RotatedSlicer::new(),
            recovery: RecoveryCodec::new(),
        }
    }

    /// Encode segment into primary slices (outer layer only).
    pub fn encode(&mut self, segment: &[u8]) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        self.primary.encode(Blob::from(segment.to_vec()))
    }

    /// Decode segment from primary slices (outer layer only).
    pub fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Vec<u8>, DecodeError> {
        let available = slices.iter().filter(|s| s.is_some()).count();
        if available < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }
        Ok(self.primary.decode(slices)?.data)
    }

    /// Encode recovery columns to in-memory buffers.
    ///
    /// Convenience wrapper around `stream()` that returns owned column buffers.
    /// Each buffer contains SLICE_COUNT * shard_len bytes.
    pub fn stream_to_vecs(
        &mut self,
        primary_slices: &[Slice; SLICE_COUNT],
    ) -> Result<(EncodeResult, Vec<Vec<u8>>), NestedEncodeError> {
        let primary_size = primary_slices[0].data.len();
        let shard_len = RecoveryLayout::new(primary_size).shard_len;
        let column_size = SLICE_COUNT * shard_len;

        let mut columns: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(column_size))
            .collect();

        let result = self.stream(primary_slices, &mut columns)?;
        Ok((result, columns))
    }

    /// Stream packed recovery columns to writers (UPLOAD PATH), while also computing
    /// a merkle root per column.
    ///
    /// Writers receive exactly SLICE_COUNT * shard_len bytes each.
    ///
    /// This does *not* materialize the n×n recovery matrix; it processes one primary at a time.
    pub fn stream<W: Write>(
        &mut self,
        primary_slices: &[Slice; SLICE_COUNT],
        column_writers: &mut [W],
    ) -> Result<EncodeResult, NestedEncodeError> {
        if column_writers.len() != SLICE_COUNT {
            return Err(NestedEncodeError::BadWritersLen {
                expected: SLICE_COUNT,
                actual: column_writers.len(),
            });
        }

        let primary_size = primary_slices[0].data.len();
        let shard_len = RecoveryLayout::new(primary_size).shard_len;

        // One merkle tree per column
        let mut trees: Vec<ColumnMerkleTree> =
            (0..SLICE_COUNT).map(|_| ColumnMerkleTree::new()).collect();

        // For each primary (row), stream its shards into all columns
        for slice in primary_slices.iter() {
            self.recovery
                .encode_row_into(&slice.data, |j, shard| {
                    // write shard into packed column j
                    column_writers[j].write_all(shard)?;
                    // also add leaf to column merkle
                    trees[j].add_leaf(shard).expect("tree capacity");
                    Ok::<(), std::io::Error>(())
                })
                .map_err(|e| match e {
                    RowEncodeError::Callback(io_err) => NestedEncodeError::Io(io_err),
                    RowEncodeError::Recovery(rec_err) => NestedEncodeError::Recovery(rec_err),
                })?;
        }

        let column_roots: [Hash; SLICE_COUNT] = std::array::from_fn(|j| trees[j].root());

        Ok(EncodeResult {
            primary_size,
            shard_len,
            column_roots,
        })
    }

    /// Extract the shard for `row` out of a packed column buffer.
    #[inline]
    pub fn shard(packed_column: &[u8], row: usize, shard_len: usize) -> Option<&[u8]> {
        let start = row.checked_mul(shard_len)?;
        let end = start.checked_add(shard_len)?;
        packed_column.get(start..end)
    }

    /// Given packed columns (one per j), build the recovery shard row `[Option<&[u8]>; 1024]`.
    pub fn row<'a>(
        packed_columns: &'a [Option<&'a [u8]>; SLICE_COUNT],
        idx: usize,
        shard_len: usize,
    ) -> Result<[Option<&'a [u8]>; SLICE_COUNT], NestedDecodeError> {
        if idx >= SLICE_COUNT {
            return Err(NestedDecodeError::RowOutOfRange {
                row: idx,
                max: SLICE_COUNT - 1,
            });
        }

        Ok(std::array::from_fn(|j| {
            packed_columns[j].and_then(|col| Self::shard(col, idx, shard_len))
        }))
    }

    /// Fast-repair a missing primary slice from recovery shards (REPAIR PATH).
    ///
    /// `recovery_row[j]` is rec[row][j] for each column j (or None if missing).
    pub fn repair(
        &mut self,
        recovery_row: &[Option<&[u8]>; SLICE_COUNT],
        primary_size: usize,
    ) -> Result<Vec<u8>, NestedDecodeError> {
        // decode() calls reconfigure internally
        Ok(self.recovery.decode(recovery_row, primary_size)?)
    }

    /// Convenience: repair primary at `row` directly from packed columns.
    pub fn repair_packed(
        &mut self,
        packed_columns: &[Option<&[u8]>; SLICE_COUNT],
        row: usize,
        primary_size: usize,
        shard_len: usize,
    ) -> Result<Vec<u8>, NestedDecodeError> {
        let recovery_row = Self::row(packed_columns, row, shard_len)?;
        self.repair(&recovery_row, primary_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{MERKLE_HEIGHT, SLICE_COUNT};
    use tape_crypto::merkle::MerkleTree;

    type ColumnMerkleTree = MerkleTree<{ MERKLE_HEIGHT }>;

    fn mk_segment(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn test_nested_stream_and_repair_one_primary() {
        // Use a small segment so the test is quick.
        // RotatedSlicer will choose a 16KB stripe; primary slices end up small.
        let segment = mk_segment(10_000);

        let mut nested = NestedSlicer::new();

        // 1) Outer encode
        let primary_slices = nested.encode(&segment).unwrap();
        let primary_size = primary_slices[0].data.len();
        assert!(primary_size > 0);

        // 2) Stream packed columns to in-memory buffers
        let (result, columns) = nested.stream_to_vecs(&primary_slices).unwrap();

        assert_eq!(result.primary_size, primary_size);
        assert!(result.shard_len > 0);

        // Each packed column must be exactly 1024 * shard_len bytes
        for col in &columns {
            assert_eq!(col.len(), SLICE_COUNT * result.shard_len);
        }

        // 3) Repair a chosen primary row k from packed columns
        let k = 5usize;

        let packed_cols: [Option<&[u8]>; SLICE_COUNT] =
            std::array::from_fn(|j| Some(columns[j].as_slice()));

        let repaired = nested
            .repair_packed(&packed_cols, k, result.primary_size, result.shard_len)
            .unwrap();

        assert_eq!(repaired, primary_slices[k].data);
    }

    #[test]
    fn test_column_roots_match_recomputed_merkle() {
        let segment = mk_segment(25_000);

        let mut nested = NestedSlicer::new();
        let primary_slices = nested.encode(&segment).unwrap();

        let (result, columns) = nested.stream_to_vecs(&primary_slices).unwrap();

        // Recompute roots for a few columns to keep test runtime reasonable
        for &j in &[0usize, 1, 500, 1023] {
            let col = &columns[j];

            let mut tree = ColumnMerkleTree::new();
            for row in 0..SLICE_COUNT {
                let shard = NestedSlicer::shard(col, row, result.shard_len).expect("shard slice");
                tree.add_leaf(shard).expect("tree capacity");
            }

            assert_eq!(tree.root(), result.column_roots[j]);
        }
    }

    #[test]
    fn test_column_root_changes_on_tamper() {
        let segment = mk_segment(12_345);

        let mut nested = NestedSlicer::new();
        let primary_slices = nested.encode(&segment).unwrap();

        let (result, mut columns) = nested.stream_to_vecs(&primary_slices).unwrap();

        // Pick a column and flip one byte
        let j = 3usize;
        assert!(!columns[j].is_empty());
        columns[j][0] ^= 0x01;

        // Recompute root and ensure it differs
        let mut tree = ColumnMerkleTree::new();
        for row in 0..SLICE_COUNT {
            let shard =
                NestedSlicer::shard(&columns[j], row, result.shard_len).expect("shard slice");
            tree.add_leaf(shard).expect("tree capacity");
        }

        assert_ne!(tree.root(), result.column_roots[j]);
    }

}

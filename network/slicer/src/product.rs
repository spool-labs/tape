//! ProductSlicer: 2D Reed-Solomon codes for ultra-efficient single-slice repair.
//!
//! Arranges data into a 32×32 grid and applies RS encoding along both rows and columns.
//! This provides ~3% repair bandwidth (vs 12.5% for LRC, 67% for standard RS).
//!
//! Layout (32×32 = 1024 cells):
//! - Data: 26×26 = 676 cells (rows 0-25, cols 0-25)
//! - Row parity: 26×6 = 156 cells (rows 0-25, cols 26-31)
//! - Column parity: 6×26 = 156 cells (rows 26-31, cols 0-25)
//! - Cross parity: 6×6 = 36 cells (rows 26-31, cols 26-31)
//!
//! Single-cell repair: fetch 31 cells from same row OR column (~3% bandwidth).
//!
//! Trade-off: Not MDS - guaranteed tolerance is ~6-36 arbitrary failures
//! (vs 335 for LRC, 341 for standard RS).

use crate::api::Slicer;
use crate::codec::round_up_to;
use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::slice_index::SliceIndex;
use crate::types::{Blob, Slice};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use thiserror::Error;

/// Grid dimension (32×32 = 1024 cells = SLICE_COUNT).
pub const GRID_SIZE: usize = 32;

/// Number of data rows/columns (26×26 = 676 data cells).
pub const DATA_DIM: usize = 26;

/// Number of parity rows/columns (6).
pub const PARITY_DIM: usize = GRID_SIZE - DATA_DIM; // 6

/// Total data cells (676).
pub const DATA_CELLS: usize = DATA_DIM * DATA_DIM; // 676

/// Row parity cells (26 rows × 6 parity cols = 156).
pub const ROW_PARITY_CELLS: usize = DATA_DIM * PARITY_DIM; // 156

/// Column parity cells (6 parity rows × 26 cols = 156).
pub const COL_PARITY_CELLS: usize = PARITY_DIM * DATA_DIM; // 156

/// Cross parity cells (6×6 = 36).
pub const CROSS_PARITY_CELLS: usize = PARITY_DIM * PARITY_DIM; // 36

/// Metadata suffix appended to each slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProductMetadata {
    /// Format version (currently 0).
    pub version: u64,
    /// Original blob length in bytes.
    pub blob_len: u64,
    /// Reserved for future use.
    pub reserved: u64,
}

impl ProductMetadata {
    pub const VERSION: u64 = 0;
    pub const SIZE: usize = 24; // 3 × 8 bytes

    pub fn new(blob_len: usize) -> Self {
        Self {
            version: Self::VERSION,
            blob_len: blob_len as u64,
            reserved: 0,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.blob_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.reserved.to_le_bytes());
        buf
    }

    pub fn from_slice(slice_data: &[u8]) -> Result<Self, DecodeError> {
        if slice_data.len() < Self::SIZE {
            return Err(DecodeError::InvalidLayout);
        }
        let suffix = &slice_data[slice_data.len() - Self::SIZE..];
        let version = u64::from_le_bytes(suffix[0..8].try_into().unwrap());
        let blob_len = u64::from_le_bytes(suffix[8..16].try_into().unwrap());
        let reserved = u64::from_le_bytes(suffix[16..24].try_into().unwrap());
        Ok(Self { version, blob_len, reserved })
    }

    pub fn blob_len(&self) -> usize {
        self.blob_len as usize
    }
}

/// Error type for Product code repair operations.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ProductRepairError {
    #[error("not enough cells in row {row} for repair (need {need}, have {have})")]
    NotEnoughInRow { row: usize, need: usize, have: usize },

    #[error("not enough cells in column {col} for repair (need {need}, have {have})")]
    NotEnoughInCol { col: usize, need: usize, have: usize },

    #[error("cell ({row}, {col}) out of range")]
    CellOutOfRange { row: usize, col: usize },

    #[error("reed-solomon decode failed")]
    RsDecodeFailed,
}

/// Convert linear slice index to (row, col) coordinates.
#[inline]
pub fn to_coords(slice_idx: usize) -> (usize, usize) {
    (slice_idx / GRID_SIZE, slice_idx % GRID_SIZE)
}

/// Convert (row, col) coordinates to linear slice index.
#[inline]
pub fn to_linear(row: usize, col: usize) -> usize {
    row * GRID_SIZE + col
}

/// Check if a cell is a data cell.
#[inline]
pub fn is_data_cell(row: usize, col: usize) -> bool {
    row < DATA_DIM && col < DATA_DIM
}

/// Check if a cell is a row parity cell.
#[inline]
pub fn is_row_parity_cell(row: usize, col: usize) -> bool {
    row < DATA_DIM && col >= DATA_DIM
}

/// Check if a cell is a column parity cell.
#[inline]
pub fn is_col_parity_cell(row: usize, col: usize) -> bool {
    row >= DATA_DIM && col < DATA_DIM
}

/// Check if a cell is a cross parity cell.
#[inline]
pub fn is_cross_parity_cell(row: usize, col: usize) -> bool {
    row >= DATA_DIM && col >= DATA_DIM
}

/// Product code slicer using 2D Reed-Solomon.
pub struct ProductSlicer {
    row_encoder: Option<ReedSolomonEncoder>,
    row_decoder: Option<ReedSolomonDecoder>,
    col_encoder: Option<ReedSolomonEncoder>,
    col_decoder: Option<ReedSolomonDecoder>,
}

impl Default for ProductSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl ProductSlicer {
    pub fn new() -> Self {
        Self {
            row_encoder: None,
            row_decoder: None,
            col_encoder: None,
            col_decoder: None,
        }
    }

    /// Get row RS encoder (RS(32, 26)).
    fn row_encoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonEncoder, EncodeError> {
        if self.row_encoder.is_none() {
            self.row_encoder = Some(
                ReedSolomonEncoder::new(DATA_DIM, PARITY_DIM, chunk_size)
                    .map_err(|_| EncodeError::TooMuchData)?,
            );
        }
        Ok(self.row_encoder.as_mut().unwrap())
    }

    /// Get row RS decoder.
    fn row_decoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonDecoder, DecodeError> {
        if self.row_decoder.is_none() {
            self.row_decoder = Some(
                ReedSolomonDecoder::new(DATA_DIM, PARITY_DIM, chunk_size)
                    .map_err(|_| DecodeError::TooMuchData)?,
            );
        }
        Ok(self.row_decoder.as_mut().unwrap())
    }

    /// Get column RS encoder (RS(32, 26)).
    fn col_encoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonEncoder, EncodeError> {
        if self.col_encoder.is_none() {
            self.col_encoder = Some(
                ReedSolomonEncoder::new(DATA_DIM, PARITY_DIM, chunk_size)
                    .map_err(|_| EncodeError::TooMuchData)?,
            );
        }
        Ok(self.col_encoder.as_mut().unwrap())
    }

    /// Get column RS decoder.
    fn col_decoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonDecoder, DecodeError> {
        if self.col_decoder.is_none() {
            self.col_decoder = Some(
                ReedSolomonDecoder::new(DATA_DIM, PARITY_DIM, chunk_size)
                    .map_err(|_| DecodeError::TooMuchData)?,
            );
        }
        Ok(self.col_decoder.as_mut().unwrap())
    }

    /// Calculate chunk size for a given blob size.
    fn chunk_size(blob_len: usize) -> usize {
        if blob_len == 0 {
            return 2; // Minimum for RS
        }
        let raw = (blob_len + DATA_CELLS - 1) / DATA_CELLS;
        round_up_to(raw.max(2), 2)
    }

    /// Repair a single cell using row-based RS decoding.
    pub fn repair_via_row(
        &mut self,
        target_row: usize,
        target_col: usize,
        slices: &[Option<Slice>; SLICE_COUNT],
        chunk_size: usize,
    ) -> Result<Vec<u8>, ProductRepairError> {
        if target_row >= GRID_SIZE || target_col >= GRID_SIZE {
            return Err(ProductRepairError::CellOutOfRange {
                row: target_row,
                col: target_col,
            });
        }

        // Count available cells in this row
        let mut available = 0;
        for c in 0..GRID_SIZE {
            if c != target_col && slices[to_linear(target_row, c)].is_some() {
                available += 1;
            }
        }

        if available < DATA_DIM {
            return Err(ProductRepairError::NotEnoughInRow {
                row: target_row,
                need: DATA_DIM,
                have: available,
            });
        }

        let decoder = self.row_decoder(chunk_size)
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;
        decoder
            .reset(DATA_DIM, PARITY_DIM, chunk_size)
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;

        // Add available cells from this row
        for c in 0..GRID_SIZE {
            if let Some(slice) = &slices[to_linear(target_row, c)] {
                let data = &slice.data[..chunk_size];
                if c < DATA_DIM {
                    decoder.add_original_shard(c, data)
                        .map_err(|_| ProductRepairError::RsDecodeFailed)?;
                } else {
                    decoder.add_recovery_shard(c - DATA_DIM, data)
                        .map_err(|_| ProductRepairError::RsDecodeFailed)?;
                }
            }
        }

        let result = decoder.decode()
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;

        // Get the repaired cell
        if target_col < DATA_DIM {
            // Data cell - get from restored originals
            let restored = result.restored_original(target_col)
                .ok_or(ProductRepairError::RsDecodeFailed)?;
            Ok(restored.to_vec())
        } else {
            // Parity cell - need to re-encode to get it
            // Collect row data first, then drop result, then encode
            let mut row_data = Vec::with_capacity(DATA_DIM);
            for c in 0..DATA_DIM {
                let cell_data = if let Some(slice) = &slices[to_linear(target_row, c)] {
                    slice.data[..chunk_size].to_vec()
                } else {
                    result.restored_original(c)
                        .ok_or(ProductRepairError::RsDecodeFailed)?
                        .to_vec()
                };
                row_data.push(cell_data);
            }

            // Drop result to release borrow
            drop(result);

            // Re-encode to get parity
            let encoder = self.row_encoder(chunk_size)
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;
            encoder.reset(DATA_DIM, PARITY_DIM, chunk_size)
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;

            for data in &row_data {
                encoder.add_original_shard(data)
                    .map_err(|_| ProductRepairError::RsDecodeFailed)?;
            }

            let enc_result = encoder.encode()
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;

            let parity_idx = target_col - DATA_DIM;
            let parity = enc_result.recovery_iter()
                .nth(parity_idx)
                .ok_or(ProductRepairError::RsDecodeFailed)?;

            Ok(parity.to_vec())
        }
    }

    /// Repair a single cell using column-based RS decoding.
    pub fn repair_via_col(
        &mut self,
        target_row: usize,
        target_col: usize,
        slices: &[Option<Slice>; SLICE_COUNT],
        chunk_size: usize,
    ) -> Result<Vec<u8>, ProductRepairError> {
        if target_row >= GRID_SIZE || target_col >= GRID_SIZE {
            return Err(ProductRepairError::CellOutOfRange {
                row: target_row,
                col: target_col,
            });
        }

        // Count available cells in this column
        let mut available = 0;
        for r in 0..GRID_SIZE {
            if r != target_row && slices[to_linear(r, target_col)].is_some() {
                available += 1;
            }
        }

        if available < DATA_DIM {
            return Err(ProductRepairError::NotEnoughInCol {
                col: target_col,
                need: DATA_DIM,
                have: available,
            });
        }

        let decoder = self.col_decoder(chunk_size)
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;
        decoder
            .reset(DATA_DIM, PARITY_DIM, chunk_size)
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;

        // Add available cells from this column
        for r in 0..GRID_SIZE {
            if let Some(slice) = &slices[to_linear(r, target_col)] {
                let data = &slice.data[..chunk_size];
                if r < DATA_DIM {
                    decoder.add_original_shard(r, data)
                        .map_err(|_| ProductRepairError::RsDecodeFailed)?;
                } else {
                    decoder.add_recovery_shard(r - DATA_DIM, data)
                        .map_err(|_| ProductRepairError::RsDecodeFailed)?;
                }
            }
        }

        let result = decoder.decode()
            .map_err(|_| ProductRepairError::RsDecodeFailed)?;

        // Get the repaired cell
        if target_row < DATA_DIM {
            let restored = result.restored_original(target_row)
                .ok_or(ProductRepairError::RsDecodeFailed)?;
            Ok(restored.to_vec())
        } else {
            // Parity cell - re-encode column
            let mut col_data = Vec::with_capacity(DATA_DIM);
            for r in 0..DATA_DIM {
                let cell_data = if let Some(slice) = &slices[to_linear(r, target_col)] {
                    slice.data[..chunk_size].to_vec()
                } else {
                    result.restored_original(r)
                        .ok_or(ProductRepairError::RsDecodeFailed)?
                        .to_vec()
                };
                col_data.push(cell_data);
            }

            // Drop result to release borrow
            drop(result);

            let encoder = self.col_encoder(chunk_size)
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;
            encoder.reset(DATA_DIM, PARITY_DIM, chunk_size)
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;

            for data in &col_data {
                encoder.add_original_shard(data)
                    .map_err(|_| ProductRepairError::RsDecodeFailed)?;
            }

            let enc_result = encoder.encode()
                .map_err(|_| ProductRepairError::RsDecodeFailed)?;

            let parity_idx = target_row - DATA_DIM;
            let parity = enc_result.recovery_iter()
                .nth(parity_idx)
                .ok_or(ProductRepairError::RsDecodeFailed)?;

            Ok(parity.to_vec())
        }
    }

    /// Get statistics about this encoding scheme.
    pub fn stats() -> ProductStats {
        ProductStats {
            grid_size: GRID_SIZE,
            data_dim: DATA_DIM,
            parity_dim: PARITY_DIM,
            data_cells: DATA_CELLS,
            total_cells: SLICE_COUNT,
            replication_factor: SLICE_COUNT as f64 / DATA_CELLS as f64,
            repair_bandwidth: (GRID_SIZE - 1) as f64 / SLICE_COUNT as f64,
        }
    }
}

/// Statistics about the Product code encoding scheme.
#[derive(Clone, Debug)]
pub struct ProductStats {
    pub grid_size: usize,
    pub data_dim: usize,
    pub parity_dim: usize,
    pub data_cells: usize,
    pub total_cells: usize,
    pub replication_factor: f64,
    pub repair_bandwidth: f64,
}

impl Slicer for ProductSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();
        let chunk_size = Self::chunk_size(blob_len);

        // Pad data to chunk_size * DATA_CELLS
        let padded_len = chunk_size * DATA_CELLS;
        let mut padded = Vec::with_capacity(padded_len);
        padded.extend_from_slice(data);
        padded.resize(padded_len, 0);

        // Split into data chunks (676 chunks for the 26×26 data region)
        let data_chunks: Vec<&[u8]> = padded.chunks(chunk_size).collect();
        assert_eq!(data_chunks.len(), DATA_CELLS);

        // Initialize grid (32×32)
        let metadata = ProductMetadata::new(blob_len);
        let slice_size = chunk_size + ProductMetadata::SIZE;
        let mut grid: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(slice_size))
            .collect();

        // Fill data cells (rows 0-25, cols 0-25)
        for r in 0..DATA_DIM {
            for c in 0..DATA_DIM {
                let data_idx = r * DATA_DIM + c;
                let linear_idx = to_linear(r, c);
                grid[linear_idx].extend_from_slice(data_chunks[data_idx]);
            }
        }

        // Compute row parities (for rows 0-25)
        let row_encoder = self.row_encoder(chunk_size)?;
        for r in 0..DATA_DIM {
            row_encoder.reset(DATA_DIM, PARITY_DIM, chunk_size)
                .map_err(|_| EncodeError::TooMuchData)?;

            // Add data cells from this row
            for c in 0..DATA_DIM {
                let linear_idx = to_linear(r, c);
                row_encoder.add_original_shard(&grid[linear_idx])
                    .map_err(|_| EncodeError::TooMuchData)?;
            }

            let result = row_encoder.encode()
                .map_err(|_| EncodeError::TooMuchData)?;

            // Store row parities (cols 26-31)
            for (p_idx, parity) in result.recovery_iter().enumerate() {
                let linear_idx = to_linear(r, DATA_DIM + p_idx);
                grid[linear_idx].extend_from_slice(parity);
            }
        }

        // Compute column parities (for cols 0-25, including data and row parity columns)
        // This also computes the cross parities (rows 26-31, cols 26-31)
        let col_encoder = self.col_encoder(chunk_size)?;
        for c in 0..GRID_SIZE {
            col_encoder.reset(DATA_DIM, PARITY_DIM, chunk_size)
                .map_err(|_| EncodeError::TooMuchData)?;

            // Add data cells from this column (rows 0-25)
            for r in 0..DATA_DIM {
                let linear_idx = to_linear(r, c);
                col_encoder.add_original_shard(&grid[linear_idx])
                    .map_err(|_| EncodeError::TooMuchData)?;
            }

            let result = col_encoder.encode()
                .map_err(|_| EncodeError::TooMuchData)?;

            // Store column parities (rows 26-31)
            for (p_idx, parity) in result.recovery_iter().enumerate() {
                let linear_idx = to_linear(DATA_DIM + p_idx, c);
                grid[linear_idx].extend_from_slice(parity);
            }
        }

        // Append metadata to all cells
        for cell in &mut grid {
            cell.extend_from_slice(&metadata.to_bytes());
        }

        // Convert to Slice array
        let output: Vec<Slice> = grid
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < DATA_CELLS {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Get metadata from any available slice
        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;
        let metadata = ProductMetadata::from_slice(&sample.data)?;
        let blob_len = metadata.blob_len();
        let chunk_size = sample.data.len() - ProductMetadata::SIZE;

        // Check if we have all data cells
        let mut all_data_present = true;
        for r in 0..DATA_DIM {
            for c in 0..DATA_DIM {
                if slices[to_linear(r, c)].is_none() {
                    all_data_present = false;
                    break;
                }
            }
            if !all_data_present {
                break;
            }
        }

        if all_data_present {
            // Fast path: just concatenate data cells
            let mut result = Vec::with_capacity(blob_len);
            for r in 0..DATA_DIM {
                for c in 0..DATA_DIM {
                    let slice = slices[to_linear(r, c)].as_ref().unwrap();
                    let data = &slice.data[..chunk_size];
                    result.extend_from_slice(data);
                }
            }
            result.truncate(blob_len);
            return Ok(Blob::from(result));
        }

        // Need to repair missing data cells
        // Collect what we have and what's missing
        let mut data_grid: Vec<Vec<Option<Vec<u8>>>> = vec![vec![None; GRID_SIZE]; GRID_SIZE];

        for r in 0..GRID_SIZE {
            for c in 0..GRID_SIZE {
                if let Some(slice) = &slices[to_linear(r, c)] {
                    data_grid[r][c] = Some(slice.data[..chunk_size].to_vec());
                }
            }
        }

        // Try to repair missing data cells using row or column decoding
        // We may need multiple passes for complex patterns
        let mut changed = true;
        let max_iterations = 10;
        let mut iteration = 0;

        while changed && iteration < max_iterations {
            changed = false;
            iteration += 1;

            for r in 0..DATA_DIM {
                for c in 0..DATA_DIM {
                    if data_grid[r][c].is_none() {
                        // Try row repair first
                        if let Ok(repaired) = self.repair_via_row(r, c, slices, chunk_size) {
                            data_grid[r][c] = Some(repaired);
                            changed = true;
                        } else if let Ok(repaired) = self.repair_via_col(r, c, slices, chunk_size) {
                            data_grid[r][c] = Some(repaired);
                            changed = true;
                        }
                    }
                }
            }
        }

        // Check if all data cells are now available
        for r in 0..DATA_DIM {
            for c in 0..DATA_DIM {
                if data_grid[r][c].is_none() {
                    return Err(DecodeError::NotEnoughSlices);
                }
            }
        }

        // Concatenate data cells
        let mut result = Vec::with_capacity(blob_len);
        for r in 0..DATA_DIM {
            for c in 0..DATA_DIM {
                result.extend_from_slice(data_grid[r][c].as_ref().unwrap());
            }
        }
        result.truncate(blob_len);
        Ok(Blob::from(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        std::array::from_fn(|i| Some(slices[i].clone()))
    }

    #[test]
    fn test_constants() {
        assert_eq!(GRID_SIZE, 32);
        assert_eq!(DATA_DIM, 26);
        assert_eq!(PARITY_DIM, 6);
        assert_eq!(DATA_CELLS, 676);
        assert_eq!(DATA_CELLS + ROW_PARITY_CELLS + COL_PARITY_CELLS + CROSS_PARITY_CELLS, SLICE_COUNT);
    }

    #[test]
    fn test_coordinates() {
        assert_eq!(to_coords(0), (0, 0));
        assert_eq!(to_coords(31), (0, 31));
        assert_eq!(to_coords(32), (1, 0));
        assert_eq!(to_coords(1023), (31, 31));

        assert_eq!(to_linear(0, 0), 0);
        assert_eq!(to_linear(0, 31), 31);
        assert_eq!(to_linear(1, 0), 32);
        assert_eq!(to_linear(31, 31), 1023);
    }

    #[test]
    fn test_cell_types() {
        // Data cell
        assert!(is_data_cell(0, 0));
        assert!(is_data_cell(25, 25));
        assert!(!is_data_cell(26, 0));
        assert!(!is_data_cell(0, 26));

        // Row parity
        assert!(is_row_parity_cell(0, 26));
        assert!(is_row_parity_cell(25, 31));
        assert!(!is_row_parity_cell(26, 26));

        // Column parity
        assert!(is_col_parity_cell(26, 0));
        assert!(is_col_parity_cell(31, 25));
        assert!(!is_col_parity_cell(26, 26));

        // Cross parity
        assert!(is_cross_parity_cell(26, 26));
        assert!(is_cross_parity_cell(31, 31));
        assert!(!is_cross_parity_cell(25, 26));
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_medium() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(100_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = ProductSlicer::new();
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_repair_single_data_cell_via_row() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove one data cell
        let target = to_linear(5, 10);
        opt[target] = None;

        // Should recover via row decoding
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_repair_single_data_cell_via_col() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove data cell and its row parities (force column repair)
        let target_row = 5;
        let target_col = 10;
        opt[to_linear(target_row, target_col)] = None;
        // Remove all cells in the same row except the target cell's column
        for c in 0..GRID_SIZE {
            if c != target_col {
                opt[to_linear(target_row, c)] = None;
            }
        }

        // Should recover via column decoding
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_repair_multiple_cells_different_rows_cols() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove cells from different rows/cols (each can be repaired independently)
        opt[to_linear(0, 0)] = None;
        opt[to_linear(5, 10)] = None;
        opt[to_linear(20, 5)] = None;
        opt[to_linear(15, 20)] = None;

        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_replication_factor() {
        let mut slicer = ProductSlicer::new();
        let payload = mk(10_000_000); // 10 MB
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();

        let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();
        let ratio = total_encoded as f64 / payload.len() as f64;

        // Should be approximately 1024/676 ≈ 1.515x
        assert!(ratio > 1.50 && ratio < 1.55, "ratio = {}", ratio);
    }

    #[test]
    fn test_stats() {
        let stats = ProductSlicer::stats();
        assert_eq!(stats.grid_size, 32);
        assert_eq!(stats.data_dim, 26);
        assert_eq!(stats.data_cells, 676);
        assert_eq!(stats.total_cells, 1024);
        assert!((stats.replication_factor - 1.515).abs() < 0.01);
        assert!((stats.repair_bandwidth - 0.0303).abs() < 0.01);
    }
}

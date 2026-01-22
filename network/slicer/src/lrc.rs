//! LrcSlicer: Local Reconstruction Codes for efficient single-slice repair.
//!
//! This implements an Azure-style LRC that partitions data slices into local groups,
//! each with a local parity slice for efficient single-failure repair.
//!
//! Layout:
//! - Slices [0..DATA_SLICES): Data slices
//! - Slices [DATA_SLICES..DATA_SLICES+LOCAL_PARITIES): Local parity slices
//! - Slices [DATA_SLICES+LOCAL_PARITIES..SLICE_COUNT): Global parity slices
//!
//! Repair:
//! - Single failure in a group: XOR local group members (128 slices = 12.5% bandwidth)
//! - Multiple failures or global parity: Fall back to global RS (683 slices = 67%)

use crate::api::Slicer;
use crate::codec::round_up_to;
use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use crate::errors::{DecodeError, EncodeError};
use crate::slice_index::SliceIndex;
use crate::types::{Blob, Slice};
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use thiserror::Error;

/// Local group size for LRC.
/// Each group of LOCAL_GROUP_SIZE data slices shares one local parity.
/// 128 is chosen to balance repair bandwidth (~12.5%) with group count.
pub const LOCAL_GROUP_SIZE: usize = 128;

/// Number of local parity slices = ceil(DATA_SLICES / LOCAL_GROUP_SIZE).
pub const LOCAL_PARITIES: usize = (DATA_SLICES + LOCAL_GROUP_SIZE - 1) / LOCAL_GROUP_SIZE; // 6

/// Number of global parity slices = CODING_SLICES - LOCAL_PARITIES.
pub const GLOBAL_PARITIES: usize = CODING_SLICES - LOCAL_PARITIES; // 335

/// First index of local parity slices.
pub const LOCAL_PARITY_START: usize = DATA_SLICES; // 683

/// First index of global parity slices.
pub const GLOBAL_PARITY_START: usize = DATA_SLICES + LOCAL_PARITIES; // 689

/// Metadata suffix appended to each slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LrcMetadata {
    /// Format version (currently 0).
    pub version: u64,
    /// Original blob length in bytes.
    pub blob_len: u64,
    /// Reserved for future use.
    pub reserved: u64,
}

impl LrcMetadata {
    pub const VERSION: u64 = 0;
    pub const SIZE: usize = 24; // 3 * 8 bytes

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

/// Error type for LRC repair operations.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum LrcRepairError {
    #[error("not enough slices for local repair (need {need}, have {have})")]
    NotEnoughLocal { need: usize, have: usize },

    #[error("not enough slices for global repair (need {DATA_SLICES}, have {0})")]
    NotEnoughGlobal(usize),

    #[error("slice index out of range: {0}")]
    IndexOutOfRange(usize),

    #[error("inconsistent slice sizes")]
    InconsistentSizes,

    #[error("reed-solomon decode failed")]
    RsDecodeFailed,
}

/// Information about a local group.
#[derive(Clone, Copy, Debug)]
pub struct LocalGroup {
    /// Group index (0 to LOCAL_PARITIES-1).
    pub index: usize,
    /// First data slice index in this group.
    pub data_start: usize,
    /// Number of data slices in this group (usually LOCAL_GROUP_SIZE, last may be smaller).
    pub data_count: usize,
    /// Index of the local parity slice for this group.
    pub parity_index: usize,
}

impl LocalGroup {
    /// Get the local group for a given data slice index.
    pub fn for_data_slice(data_idx: usize) -> Option<Self> {
        if data_idx >= DATA_SLICES {
            return None;
        }
        let group_idx = data_idx / LOCAL_GROUP_SIZE;
        Some(Self::from_index(group_idx))
    }

    /// Get the local group by group index.
    pub fn from_index(group_idx: usize) -> Self {
        let data_start = group_idx * LOCAL_GROUP_SIZE;
        let data_end = ((group_idx + 1) * LOCAL_GROUP_SIZE).min(DATA_SLICES);
        let data_count = data_end - data_start;
        let parity_index = LOCAL_PARITY_START + group_idx;

        Self {
            index: group_idx,
            data_start,
            data_count,
            parity_index,
        }
    }

    /// Iterate over all data slice indices in this group.
    pub fn data_indices(&self) -> impl Iterator<Item = usize> {
        self.data_start..(self.data_start + self.data_count)
    }
}

/// LRC Slicer with local groups for efficient single-slice repair.
pub struct LrcSlicer {
    encoder: Option<ReedSolomonEncoder>,
    decoder: Option<ReedSolomonDecoder>,
}

impl Default for LrcSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl LrcSlicer {
    pub fn new() -> Self {
        Self {
            encoder: None,
            decoder: None,
        }
    }

    /// Get RS encoder, lazily initialized.
    fn encoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonEncoder, EncodeError> {
        if self.encoder.is_none() {
            self.encoder = Some(
                ReedSolomonEncoder::new(DATA_SLICES, GLOBAL_PARITIES, chunk_size)
                    .map_err(|_| EncodeError::TooMuchData)?,
            );
        }
        Ok(self.encoder.as_mut().unwrap())
    }

    /// Get RS decoder, lazily initialized.
    fn decoder(&mut self, chunk_size: usize) -> Result<&mut ReedSolomonDecoder, DecodeError> {
        if self.decoder.is_none() {
            self.decoder = Some(
                ReedSolomonDecoder::new(DATA_SLICES, GLOBAL_PARITIES, chunk_size)
                    .map_err(|_| DecodeError::TooMuchData)?,
            );
        }
        Ok(self.decoder.as_mut().unwrap())
    }

    /// Compute local parity for a group by XORing all data chunks.
    fn compute_local_parity(chunks: &[&[u8]]) -> Vec<u8> {
        if chunks.is_empty() {
            return Vec::new();
        }
        let chunk_size = chunks[0].len();
        let mut parity = vec![0u8; chunk_size];

        for chunk in chunks {
            for (p, &b) in parity.iter_mut().zip(chunk.iter()) {
                *p ^= b;
            }
        }
        parity
    }

    /// Try to repair a single data slice using local parity.
    ///
    /// Returns Ok(repaired_data) if local repair succeeds.
    /// Returns Err if not enough slices in the group are available.
    pub fn local_repair(
        &self,
        target_idx: usize,
        slices: &[Option<Slice>; SLICE_COUNT],
        chunk_size: usize,
    ) -> Result<Vec<u8>, LrcRepairError> {
        if target_idx >= DATA_SLICES {
            return Err(LrcRepairError::IndexOutOfRange(target_idx));
        }

        let group = LocalGroup::for_data_slice(target_idx).unwrap();

        // Check we have all other members of the group + local parity
        let mut available_count = 0;
        for i in group.data_indices() {
            if i != target_idx && slices[i].is_some() {
                available_count += 1;
            }
        }
        if slices[group.parity_index].is_some() {
            available_count += 1;
        }

        // Need all other data slices + local parity (total = group.data_count)
        if available_count < group.data_count {
            return Err(LrcRepairError::NotEnoughLocal {
                need: group.data_count,
                have: available_count,
            });
        }

        // XOR all available members to recover target
        let mut result = vec![0u8; chunk_size];

        // XOR local parity
        if let Some(parity_slice) = &slices[group.parity_index] {
            let data = &parity_slice.data[..parity_slice.data.len() - LrcMetadata::SIZE];
            for (r, &b) in result.iter_mut().zip(data.iter()) {
                *r ^= b;
            }
        }

        // XOR other data slices in the group
        for i in group.data_indices() {
            if i != target_idx {
                if let Some(slice) = &slices[i] {
                    let data = &slice.data[..slice.data.len() - LrcMetadata::SIZE];
                    for (r, &b) in result.iter_mut().zip(data.iter()) {
                        *r ^= b;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Repair using global RS coding (fallback when local repair fails).
    pub fn global_repair(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Vec<Vec<u8>>, DecodeError> {
        // Count available slices (data + global parity)
        let mut available = 0;
        for i in 0..DATA_SLICES {
            if slices[i].is_some() {
                available += 1;
            }
        }
        for i in GLOBAL_PARITY_START..SLICE_COUNT {
            if slices[i].is_some() {
                available += 1;
            }
        }

        if available < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Get chunk size from any available slice
        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;
        let chunk_size = sample.data.len() - LrcMetadata::SIZE;

        let decoder = self.decoder(chunk_size)?;
        decoder
            .reset(DATA_SLICES, GLOBAL_PARITIES, chunk_size)
            .map_err(|_| DecodeError::TooMuchData)?;

        // Add data slices
        for i in 0..DATA_SLICES {
            if let Some(slice) = &slices[i] {
                let data = &slice.data[..chunk_size];
                decoder
                    .add_original_shard(i, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            }
        }

        // Add global parity slices
        for i in GLOBAL_PARITY_START..SLICE_COUNT {
            if let Some(slice) = &slices[i] {
                let data = &slice.data[..chunk_size];
                let parity_idx = i - GLOBAL_PARITY_START;
                decoder
                    .add_recovery_shard(parity_idx, data)
                    .map_err(|_| DecodeError::InvalidLayout)?;
            }
        }

        let result = decoder.decode().map_err(|_| DecodeError::BadEncoding)?;

        // Collect all data chunks (restored or original)
        let mut data_chunks = Vec::with_capacity(DATA_SLICES);
        for i in 0..DATA_SLICES {
            let chunk = match &slices[i] {
                Some(slice) => slice.data[..chunk_size].to_vec(),
                None => result
                    .restored_original(i)
                    .ok_or(DecodeError::InvalidLayout)?
                    .to_vec(),
            };
            data_chunks.push(chunk);
        }

        Ok(data_chunks)
    }

    /// Calculate chunk size for a given blob size.
    fn chunk_size(blob_len: usize) -> usize {
        if blob_len == 0 {
            // Minimum chunk size for RS (must be even and >= 2)
            return 2;
        }
        // Round up to ensure chunk_size is even (RS requirement)
        let raw = (blob_len + DATA_SLICES - 1) / DATA_SLICES;
        round_up_to(raw.max(2), 2)
    }

    /// Get statistics about this encoding scheme.
    pub fn stats() -> LrcStats {
        LrcStats {
            data_slices: DATA_SLICES,
            local_parities: LOCAL_PARITIES,
            global_parities: GLOBAL_PARITIES,
            total_slices: SLICE_COUNT,
            local_group_size: LOCAL_GROUP_SIZE,
            replication_factor: SLICE_COUNT as f64 / DATA_SLICES as f64,
            local_repair_bandwidth: LOCAL_GROUP_SIZE as f64 / SLICE_COUNT as f64,
            global_repair_bandwidth: DATA_SLICES as f64 / SLICE_COUNT as f64,
        }
    }
}

/// Statistics about the LRC encoding scheme.
#[derive(Clone, Debug)]
pub struct LrcStats {
    pub data_slices: usize,
    pub local_parities: usize,
    pub global_parities: usize,
    pub total_slices: usize,
    pub local_group_size: usize,
    pub replication_factor: f64,
    pub local_repair_bandwidth: f64,
    pub global_repair_bandwidth: f64,
}

impl Slicer for LrcSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let data = blob.as_slice();
        let blob_len = data.len();
        let chunk_size = Self::chunk_size(blob_len);

        // Pad data to chunk_size * DATA_SLICES
        let padded_len = chunk_size * DATA_SLICES;
        let mut padded = Vec::with_capacity(padded_len);
        padded.extend_from_slice(data);
        padded.resize(padded_len, 0);

        // Split into data chunks
        let data_chunks: Vec<&[u8]> = padded.chunks(chunk_size).collect();
        assert_eq!(data_chunks.len(), DATA_SLICES);

        // Initialize output slices
        let metadata = LrcMetadata::new(blob_len);
        let slice_size = chunk_size + LrcMetadata::SIZE;
        let mut slices: Vec<Vec<u8>> = (0..SLICE_COUNT)
            .map(|_| Vec::with_capacity(slice_size))
            .collect();

        // Data slices
        for (i, chunk) in data_chunks.iter().enumerate() {
            slices[i].extend_from_slice(chunk);
            slices[i].extend_from_slice(&metadata.to_bytes());
        }

        // Local parity slices (one per group)
        for g in 0..LOCAL_PARITIES {
            let group = LocalGroup::from_index(g);
            let group_chunks: Vec<&[u8]> = group.data_indices().map(|i| data_chunks[i]).collect();
            let parity = Self::compute_local_parity(&group_chunks);
            slices[group.parity_index].extend_from_slice(&parity);
            slices[group.parity_index].extend_from_slice(&metadata.to_bytes());
        }

        // Global parity slices using RS encoding
        let encoder = self.encoder(chunk_size)?;
        encoder
            .reset(DATA_SLICES, GLOBAL_PARITIES, chunk_size)
            .map_err(|_| EncodeError::TooMuchData)?;

        for chunk in &data_chunks {
            encoder
                .add_original_shard(chunk)
                .map_err(|_| EncodeError::TooMuchData)?;
        }

        let rs_result = encoder.encode().map_err(|_| EncodeError::TooMuchData)?;

        for (p_idx, parity_shard) in rs_result.recovery_iter().enumerate() {
            let slice_idx = GLOBAL_PARITY_START + p_idx;
            slices[slice_idx].extend_from_slice(parity_shard);
            slices[slice_idx].extend_from_slice(&metadata.to_bytes());
        }

        // Convert to Slice array
        let output: Vec<Slice> = slices
            .into_iter()
            .enumerate()
            .map(|(i, data)| Slice::new(SliceIndex::new(i).unwrap(), data))
            .collect();

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    fn decode(&mut self, slices: &[Option<Slice>; SLICE_COUNT]) -> Result<Blob, DecodeError> {
        let present_count = slices.iter().filter(|s| s.is_some()).count();
        if present_count < DATA_SLICES {
            return Err(DecodeError::NotEnoughSlices);
        }

        // Get metadata from any available slice
        let sample = slices
            .iter()
            .flatten()
            .next()
            .ok_or(DecodeError::NotEnoughSlices)?;
        let metadata = LrcMetadata::from_slice(&sample.data)?;
        let blob_len = metadata.blob_len();
        let chunk_size = sample.data.len() - LrcMetadata::SIZE;

        // Check if we have all data slices
        let all_data_present = (0..DATA_SLICES).all(|i| slices[i].is_some());

        if all_data_present {
            // Fast path: just concatenate data slices
            let mut result = Vec::with_capacity(blob_len);
            for i in 0..DATA_SLICES {
                let slice = slices[i].as_ref().unwrap();
                let data = &slice.data[..chunk_size];
                result.extend_from_slice(data);
            }
            result.truncate(blob_len);
            return Ok(Blob::from(result));
        }

        // Need to repair missing data slices
        // First try local repair for each missing data slice
        let mut data_chunks: Vec<Option<Vec<u8>>> = (0..DATA_SLICES)
            .map(|i| {
                slices[i]
                    .as_ref()
                    .map(|s| s.data[..chunk_size].to_vec())
            })
            .collect();

        // Try local repair for missing slices
        for i in 0..DATA_SLICES {
            if data_chunks[i].is_none() {
                if let Ok(repaired) = self.local_repair(i, slices, chunk_size) {
                    data_chunks[i] = Some(repaired);
                }
            }
        }

        // Check if local repair was sufficient
        let still_missing: Vec<usize> = data_chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_none())
            .map(|(i, _)| i)
            .collect();

        if still_missing.is_empty() {
            // All repaired via local
            let mut result = Vec::with_capacity(blob_len);
            for chunk in data_chunks.into_iter().flatten() {
                result.extend_from_slice(&chunk);
            }
            result.truncate(blob_len);
            return Ok(Blob::from(result));
        }

        // Fall back to global RS repair
        let all_chunks = self.global_repair(slices)?;

        let mut result = Vec::with_capacity(blob_len);
        for chunk in all_chunks {
            result.extend_from_slice(&chunk);
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
        assert_eq!(LOCAL_PARITIES, 6);
        assert_eq!(GLOBAL_PARITIES, 335);
        assert_eq!(LOCAL_PARITY_START, DATA_SLICES);
        assert_eq!(GLOBAL_PARITY_START, DATA_SLICES + LOCAL_PARITIES);
        assert_eq!(
            DATA_SLICES + LOCAL_PARITIES + GLOBAL_PARITIES,
            SLICE_COUNT
        );
    }

    #[test]
    fn test_local_group() {
        // Group 0
        let g0 = LocalGroup::from_index(0);
        assert_eq!(g0.data_start, 0);
        assert_eq!(g0.data_count, 128);
        assert_eq!(g0.parity_index, 683);

        // Group 5 (last, smaller)
        let g5 = LocalGroup::from_index(5);
        assert_eq!(g5.data_start, 640);
        assert_eq!(g5.data_count, 43); // 683 - 640 = 43
        assert_eq!(g5.parity_index, 688);

        // From data slice
        let g = LocalGroup::for_data_slice(200).unwrap();
        assert_eq!(g.index, 1); // 200 / 128 = 1
    }

    #[test]
    fn test_roundtrip_small() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(1000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_medium() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(100_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_roundtrip_empty() {
        let mut slicer = LrcSlicer::new();
        let payload = Vec::new();
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_slice_count() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        assert_eq!(slices.len(), SLICE_COUNT);
    }

    #[test]
    fn test_all_slices_same_size() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let first_len = slices[0].data.len();
        for slice in &slices {
            assert_eq!(slice.data.len(), first_len);
        }
    }

    #[test]
    fn test_local_repair_single_loss() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove one data slice from group 0
        opt[50] = None;

        // Decode should recover via local repair
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_local_repair_multiple_single_losses() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove one data slice from each of multiple groups
        opt[10] = None; // Group 0
        opt[150] = None; // Group 1
        opt[300] = None; // Group 2
        opt[450] = None; // Group 3

        // Each should be locally repairable
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_global_repair_multiple_losses_same_group() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove two data slices from the same group (local repair fails)
        opt[10] = None;
        opt[20] = None;

        // Should fall back to global RS repair
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_decode_data_only() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();
        let mut opt = to_opt(&slices);

        // Remove all local and global parities, keep only data
        for i in DATA_SLICES..SLICE_COUNT {
            opt[i] = None;
        }

        // Should succeed (all data present)
        let restored = slicer.decode(&opt).unwrap();
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn test_not_enough_slices() {
        let mut slicer = LrcSlicer::new();
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).unwrap();
        let mut opt = to_opt(&slices);

        // Remove too many slices
        for i in 0..(SLICE_COUNT - DATA_SLICES + 1) {
            opt[i] = None;
        }

        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn test_replication_factor() {
        let mut slicer = LrcSlicer::new();
        // Use larger payload to minimize metadata overhead
        let payload = mk(10_000_000); // 10 MB
        let slices = slicer.encode(Blob::from(payload.clone())).unwrap();

        let total_encoded: usize = slices.iter().map(|s| s.data.len()).sum();
        let ratio = total_encoded as f64 / payload.len() as f64;

        // Should be approximately 1.5x (1024/683 ≈ 1.499)
        // With small metadata overhead, expect 1.5-1.52x for large files
        assert!(ratio > 1.49 && ratio < 1.55, "ratio = {}", ratio);
    }

    #[test]
    fn test_stats() {
        let stats = LrcSlicer::stats();
        assert_eq!(stats.data_slices, DATA_SLICES);
        assert_eq!(stats.local_parities, LOCAL_PARITIES);
        assert_eq!(stats.global_parities, GLOBAL_PARITIES);
        assert_eq!(stats.total_slices, SLICE_COUNT);
        assert!((stats.replication_factor - 1.499).abs() < 0.01);
    }
}

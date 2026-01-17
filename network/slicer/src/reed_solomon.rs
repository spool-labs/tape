use super::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::Slice;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use thiserror::Error;

/// Maximum slice size for BasicSlicer (used for testing/debugging only).
/// 4 KiB allows encoding blobs up to ~2.7 MB (DATA_SLICES * 4 KiB).
/// For production workloads, use StripedSlicer which handles large blobs efficiently.
pub const MAX_SLICE_BYTES: usize = 1 << 12; // 4 KiB

/// Errors that may be returned by ReedSolomonCoder::encode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonEncodeError {
    #[error("too much data to encode with current settings")]
    TooMuchData,
}

/// Errors that may be returned by ReedSolomonCoder::decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonDecodeError {
    #[error("not enough slices to reconstruct (need at least DATA_SLICES)")]
    NotEnoughSlices,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding detected")]
    InvalidPadding,
    #[error("invalid layout or inconsistent slice sizes/indices")]
    InvalidLayout,
}

/// The data and coding slices output by encode().
#[derive(Clone, Debug)]
pub struct RawSlices {
    pub data: Vec<Vec<u8>>,
    pub coding: Vec<Vec<u8>>,
}

/// Reed-Solomon coder for 3f+1 layout (k = data, r = coding).
/// This is a thin wrapper around reed_solomon_simd. It reuses working buffers across calls.
pub struct ReedSolomonCoder {
    k_data: usize,
    r_coding: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl ReedSolomonCoder {
    /// Create a new Reed-Solomon coder with default max slice size (4 KiB).
    /// This is suitable for testing/debugging. For larger blobs, use `with_max_slice_bytes`.
    pub fn new(k_data: usize, r_coding: usize) -> Self {
        Self::with_max_slice_bytes(k_data, r_coding, MAX_SLICE_BYTES)
    }

    /// Create a new Reed-Solomon coder with a custom max slice size.
    ///
    /// The max_slice_bytes determines the maximum size of each slice,
    /// which affects memory allocation in the encoder/decoder.
    /// Use larger values for benchmarks or when encoding large blobs.
    pub fn with_max_slice_bytes(k_data: usize, r_coding: usize, max_slice_bytes: usize) -> Self {
        assert!(u16::MAX as usize >= 65535);
        assert!(k_data > 0, "k_data must be > 0");
        assert!(r_coding > 0, "r_coding must be > 0");
        assert!(max_slice_bytes > 0, "max_slice_bytes must be > 0");

        let n_total = k_data + r_coding;
        assert!(n_total <= 65536, "too many total slices for RS field");
        assert!(k_data == DATA_SLICES, "k_data must match DATA_SLICES");
        assert!(r_coding == CODING_SLICES, "r_coding must match CODING_SLICES");

        // Use a bounded max slice size the library accepts. Per-call reset() will set the actual slice size.
        let encoder = ReedSolomonEncoder::new(k_data, r_coding, max_slice_bytes)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(k_data, r_coding, max_slice_bytes)
            .expect("RS decoder init");

        Self {
            k_data,
            r_coding,
            encoder,
            decoder,
        }
    }

    /// Reed-Solomon encodes the payload into k data and r coding slices, returning RawSlices.
    /// Returns TooMuchData if payload cannot be encoded under the current encoder limits.
    pub fn encode(&mut self, payload: &[u8]) -> Result<RawSlices, ReedSolomonEncodeError> {
        // Compute padding: make total a multiple of 2 * k.
        let k = self.k_data;
        let two_k = 2 * k;

        // Avoid division by zero; guaranteed by constructor.
        debug_assert!(k > 0);

        // If payload is empty, we still add the 0x80 byte (minimum padding).
        let remainder = payload.len() % two_k;
        let padding_bytes = if remainder == 0 { two_k } else { two_k - remainder };
        let total_len = payload
            .len()
            .checked_add(padding_bytes)
            .ok_or(ReedSolomonEncodeError::TooMuchData)?;

        // slice_bytes = ceil(total_len / k)
        let slice_bytes = (total_len + k - 1) / k;

        // Ensure the encoder can handle this slice size.
        self.encoder
            .reset(self.k_data, self.r_coding, slice_bytes)
            .map_err(|_| ReedSolomonEncodeError::TooMuchData)?;

        // Place 0x80 and zeros at end of payload (bit padding).
        let last_group_bytes = (two_k + slice_bytes - 1) / slice_bytes * slice_bytes;
        let boundary = total_len
            .checked_sub(last_group_bytes)
            .ok_or(ReedSolomonEncodeError::TooMuchData)?;
        let mut tail = Vec::with_capacity(last_group_bytes);
        tail.extend_from_slice(&payload[boundary..payload.len()]);
        tail.push(0x80);
        tail.resize(last_group_bytes, 0x00);

        // Feed k original slices into the encoder.
        let mut data = Vec::with_capacity(self.k_data);
        payload[..boundary]
            .chunks(slice_bytes)
            .chain(tail.chunks(slice_bytes))
            .for_each(|chunk| {
                self.encoder
                    .add_original_shard(chunk)
                    .expect("adding slices of the configured size should succeed");
                data.push(chunk.to_vec());
            });

        // Create parity slices.
        let output = self
            .encoder
            .encode()
            .expect("should be able to encode after k data slices were added");
        let coding = output.recovery_iter().map(<[u8]>::to_vec).collect();

        Ok(RawSlices { data, coding })
    }

    /// Reconstructs the raw payload bytes from optional slices (data and coding).
    /// Layout: data slices are indices [0..k), coding slices are indices [k..k+r).
    /// At least k total slices (data+coding) are required.
    pub fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Vec<u8>, ReedSolomonDecodeError> {
        let present = slices.iter().flatten().count();
        if present < self.k_data {
            return Err(ReedSolomonDecodeError::NotEnoughSlices);
        }

        // Infer slice_bytes from any present slice.
        let slice_bytes = slices
            .iter()
            .flatten()
            .map(|s| s.data.len())
            .next()
            .ok_or(ReedSolomonDecodeError::InvalidLayout)?;

        // Ensure all present slices have the same size.
        if slices
            .iter()
            .flatten()
            .any(|s| s.data.len() != slice_bytes)
        {
            return Err(ReedSolomonDecodeError::InvalidLayout);
        }

        self.decoder
            .reset(self.k_data, self.r_coding, slice_bytes)
            .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;

        // Split into data and coding by index ranges.
        // Feed data slices (original) and coding slices (recovery) into decoder.
        for s in slices.iter().flatten() {
            let idx = *s.index;
            if idx < self.k_data {
                // data slice at index idx
                self.decoder
                    .add_original_shard(idx, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else if idx < self.k_data + self.r_coding {
                // coding slice at offset
                let offset = idx - self.k_data;
                self.decoder
                    .add_recovery_shard(offset, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else {
                return Err(ReedSolomonDecodeError::InvalidLayout);
            }
        }

        let restored = self.decoder.decode().map_err(|_| {
            // If the library returns an error here, it's likely because the slices were inconsistent.
            ReedSolomonDecodeError::InvalidLayout
        })?;

        // Reassemble the payload from data slices in order [0..k).
        // If a data slice was missing, pull the restored version.
        let mut payload = Vec::with_capacity(self.k_data * slice_bytes);
        for data_idx in 0..self.k_data {
            let slice_ref = match slices[data_idx].as_ref() {
                Some(s) => &s.data,
                None => restored
                    .restored_original(data_idx)
                    .ok_or(ReedSolomonDecodeError::InvalidLayout)?,
            };
            // Avoid expanding to impossible sizes.
            payload
                .try_reserve(slice_ref.len())
                .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;
            payload.extend_from_slice(slice_ref);
        }

        // Remove padding: scan backwards counting zeros, then require a single 0x80 preceding them.
        if payload.is_empty() {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        let zeros = payload.iter().rev().take_while(|b| **b == 0).count();
        let padding_total = zeros + 1;
        if padding_total > payload.len() {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        let marker_pos = payload.len() - padding_total;
        if payload[marker_pos] != 0x80 {
            return Err(ReedSolomonDecodeError::InvalidPadding);
        }
        payload.truncate(marker_pos);

        Ok(payload)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::{Slice, CODING_SLICES, DATA_SLICES, SLICE_COUNT};
    use crate::SliceIndex;

    /// Create a test coder with the default configuration.
    fn test_coder() -> ReedSolomonCoder {
        ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES)
    }

    fn make_payload(len: usize) -> Vec<u8> {
        // Deterministic, non-trivial pattern
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_full(raw: &RawSlices) -> [Option<Slice>; SLICE_COUNT] {
        let mut arr: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
        for (i, d) in raw.data.iter().enumerate() {
            arr[i] = Some(Slice {
                index: SliceIndex::new(i).unwrap(),
                data: d.clone(),
            });
        }
        for (j, c) in raw.coding.iter().enumerate() {
            let idx = DATA_SLICES + j;
            arr[idx] = Some(Slice {
                index: SliceIndex::new(idx).unwrap(),
                data: c.clone(),
            });
        }
        arr
    }

    fn keep_only(arr: &mut [Option<Slice>; SLICE_COUNT], keep: &[usize]) {
        let mut keep_set = vec![false; SLICE_COUNT];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    fn equal_sizes(arr: &[Option<Slice>; SLICE_COUNT]) -> Option<usize> {
        let mut size = None;
        for s in arr.iter().flatten() {
            match size {
                None => size = Some(s.data.len()),
                Some(expected) if expected != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    #[test]
    fn encode_counts() {
        let mut coder = test_coder();
        let payload = make_payload(42_000);
        let raw = coder.encode(&payload).expect("encode ok");

        assert_eq!(raw.data.len(), DATA_SLICES);
        assert_eq!(raw.coding.len(), CODING_SLICES);

        let slice_len = raw.data[0].len();
        assert!(raw.data.iter().all(|d| d.len() == slice_len));
        assert!(raw.coding.iter().all(|c| c.len() == slice_len));
    }

    #[test]
    fn roundtrip_sizes() {
        let mut coder = test_coder();

        let sizes = [
            0usize,
            1,
            DATA_SLICES - 1,
            DATA_SLICES,
            DATA_SLICES + 1,
            2 * DATA_SLICES - 1,
            2 * DATA_SLICES,
            5 * DATA_SLICES + 123,
            100_000,
        ];

        for &sz in &sizes {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");
            let full = to_full(&raw);

            // all slices
            let restored = coder.decode(&full).expect("decode ok");
            assert_eq!(restored, payload, "round-trip mismatch for size {}", sz);

            // only data slices (k)
            let mut only_data = full.clone();
            keep_only(&mut only_data, &(0..DATA_SLICES).collect::<Vec<_>>());
            let restored = coder.decode(&only_data).expect("decode ok with k data slices");
            assert_eq!(restored, payload, "round-trip data-only mismatch for size {}", sz);

            // mixed: ~k/2 data + all coding, then fill to k
            let half_data = DATA_SLICES / 2;
            let mut keep = Vec::with_capacity(DATA_SLICES);
            for i in (0..DATA_SLICES).step_by(2).take(half_data) {
                keep.push(i);
            }
            for j in 0..CODING_SLICES {
                keep.push(DATA_SLICES + j);
            }
            while keep.len() < DATA_SLICES {
                let mut added = false;
                for i in 0..DATA_SLICES {
                    if !keep.contains(&i) {
                        keep.push(i);
                        added = true;
                        break;
                    }
                }
                assert!(added);
            }

            let mut mixed = full.clone();
            keep_only(&mut mixed, &keep);
            assert_eq!(mixed.iter().flatten().count(), DATA_SLICES);
            let restored = coder.decode(&mixed).expect("decode ok with mixed slices");
            assert_eq!(restored, payload, "round-trip mixed mismatch size {}", sz);
        }
    }

    #[test]
    fn tiny() {
        let mut coder = test_coder();

        for sz in 0..4usize {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");
            let slices = to_full(&raw);
            let out = coder.decode(&slices).expect("decode ok");
            assert_eq!(out, payload, "tiny payload mismatch sz={}", sz);
        }
    }

    #[test]
    fn not_enough() {
        let mut coder = test_coder();
        let payload = make_payload(10_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut slices = to_full(&raw);

        // keep only k-1 data slices
        let keep: Vec<usize> = (0..(DATA_SLICES - 1)).collect();
        keep_only(&mut slices, &keep);

        let res = coder.decode(&slices);
        assert!(matches!(res, Err(ReedSolomonDecodeError::NotEnoughSlices)));
    }

    #[test]
    fn bad_size() {
        let mut coder = test_coder();
        let payload = make_payload(50_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut slices = to_full(&raw);

        // uniform to start
        let base_len = equal_sizes(&slices).expect("uniform sizes");

        // tamper: shrink one slice by 1 byte
        if let Some(Some(sh)) = slices.get_mut(0) {
            assert_eq!(sh.data.len(), base_len);
            sh.data.pop();
            assert_eq!(sh.data.len(), base_len - 1);
        } else {
            panic!("expected slice present");
        }

        let res = coder.decode(&slices);
        assert!(matches!(res, Err(ReedSolomonDecodeError::InvalidLayout)));
    }

    #[test]
    fn empty_rt() {
        let mut coder = test_coder();
        let payload = Vec::<u8>::new();
        let raw = coder.encode(&payload).expect("encode ok for empty payload");
        let slices = to_full(&raw);
        let out = coder.decode(&slices).expect("decode ok");
        assert!(out.is_empty(), "decoded payload should be empty");
    }


    #[test]
    fn size_table() {
        // Keep this short so it's readable on the terminal.

        let mut coder = test_coder();
        // Max payload with default 4 KiB slices: 4 KiB * 683 data slices = ~2.7 MB
        // Keep sizes modest for test speed
        let sizes = [
            0usize,
            1,
            DATA_SLICES / 2,
            DATA_SLICES - 1,
            DATA_SLICES,
            DATA_SLICES + 1,
            10_000,
            50_000,
            100_000,
        ];

        println!(
            "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
            "payload", "slice", "k", "r", "n", "total_bytes", "ratio", "ok"
        );
        println!(
            "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
            "(bytes)", "(bytes)", "", "", "", "(bytes)", "", ""
        );

        for &sz in &sizes {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");

            // All slices have equal length (by construction)
            let slice_len = raw.data[0].len();
            let n = DATA_SLICES + CODING_SLICES;
            let total_bytes = n * slice_len;
            let ratio_str = if sz > 0 {
                format!("{:.3}", total_bytes as f64 / sz as f64)
            } else {
                "-".to_string()
            };

            // Build full slice set and round trip
            let mut slices: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
            for (i, d) in raw.data.iter().enumerate() {
                slices[i] = Some(Slice {
                    index: SliceIndex::new(i).unwrap(),
                    data: d.clone(),
                });
            }
            for (j, c) in raw.coding.iter().enumerate() {
                let idx = DATA_SLICES + j;
                slices[idx] = Some(Slice {
                    index: SliceIndex::new(idx).unwrap(),
                    data: c.clone(),
                });
            }

            let out = coder.decode(&slices).expect("decode ok");
            let ok = out == payload;

            println!(
                "{:<10} {:<10} {:<5} {:<5} {:<5} {:<14} {:<8} {:<6}",
                sz,
                slice_len,
                DATA_SLICES,
                CODING_SLICES,
                n,
                total_bytes,
                ratio_str,
                if ok { "ok" } else { "FAIL" }
            );

            // Keep the test meaningful
            assert!(ok, "round-trip failed for size {}", sz);
        }
    }
}

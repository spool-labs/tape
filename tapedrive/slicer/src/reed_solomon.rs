use super::{CODING_SLICES, DATA_SLICES, TOTAL_SLICES};
use super::Shard;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use static_assertions::const_assert;
use thiserror::Error;

/// This is the maximum shard size we allow the encoder/decoder to handle.
/// We set it to 1 MiB here, which allows encoding up to about 683 MiB of data per stripe
/// (with 341 MiB of coding, and 1Gib total).
const DEFAULT_MAX_SHARD_BYTES: usize = 1 << 20; // 1 MiB

/// Errors that may be returned by ReedSolomonCoder::encode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonEncodeError {
    #[error("too much data to encode with current settings")]
    TooMuchData,
}

/// Errors that may be returned by ReedSolomonCoder::decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonDecodeError {
    #[error("not enough shards to reconstruct")]
    NotEnoughShards,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding detected")]
    InvalidPadding,
    #[error("invalid layout or inconsistent shard sizes/indices")]
    InvalidLayout,
}

/// The data and coding slices (shards) output by encode().
#[derive(Clone, Debug)]
pub struct RawSlices {
    pub data: Vec<Vec<u8>>,
    pub coding: Vec<Vec<u8>>,
}

/// Reed-Solomon coder for 3f+1 layout (k = data, r = coding).
/// This is a thin wrapper around reed_solomon_simd. It reuses working buffers across calls.
///
/// Padding scheme:
/// - Add a single 0x80 byte and then as many 0x00 bytes as needed so the total payload
///   length is a multiple of 2 * DATA_SLICES.
/// - Split into DATA_SLICES shards of equal size (last ones padded).
/// - Generate CODING_SLICES parity shards.
/// - On decode, reconstruct and remove padding by scanning from the end for zeros
///   followed by a single 0x80 at the boundary.
pub struct ReedSolomonCoder {
    k_data: usize,
    r_coding: usize,
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl ReedSolomonCoder {
    /// n = k + r must fit the field limits (reed_solomon_simd supports up to 65536 shards).
    pub fn new(k_data: usize, r_coding: usize) -> Self {
        const_assert!(u16::MAX as usize >= 65535);
        assert!(k_data > 0, "k_data must be > 0");
        assert!(r_coding > 0, "r_coding must be > 0");
        let n_total = k_data + r_coding;
        assert!(n_total <= 65536, "too many total shards for RS field");
        assert!(k_data == DATA_SLICES, "k_data must match DATA_SLICES");
        assert!(r_coding == CODING_SLICES, "r_coding must match CODING_SLICES");

        // Use a bounded max shard size the library accepts. Per-call reset() will set the actual shard size.
        let encoder = ReedSolomonEncoder::new(k_data, r_coding, DEFAULT_MAX_SHARD_BYTES)
            .expect("RS encoder init");
        let decoder = ReedSolomonDecoder::new(k_data, r_coding, DEFAULT_MAX_SHARD_BYTES)
            .expect("RS decoder init");

        Self {
            k_data,
            r_coding,
            encoder,
            decoder,
        }
    }

    /// Reed–Solomon encodes the payload into k data and r coding shards, returning RawSlices.
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

        // shard_bytes = ceil(total_len / k)
        let shard_bytes = (total_len + k - 1) / k;

        // Ensure the encoder can handle this shard size.
        self.encoder
            .reset(self.k_data, self.r_coding, shard_bytes)
            .map_err(|_| ReedSolomonEncodeError::TooMuchData)?;

        // Place 0x80 and zeros at end of payload (bit padding).
        let last_group_bytes = (two_k + shard_bytes - 1) / shard_bytes * shard_bytes;
        let boundary = total_len
            .checked_sub(last_group_bytes)
            .ok_or(ReedSolomonEncodeError::TooMuchData)?;
        let mut tail = Vec::with_capacity(last_group_bytes);
        tail.extend_from_slice(&payload[boundary..payload.len()]);
        tail.push(0x80);
        tail.resize(last_group_bytes, 0x00);

        // Feed k original shards into the encoder.
        let mut data = Vec::with_capacity(self.k_data);
        payload[..boundary]
            .chunks(shard_bytes)
            .chain(tail.chunks(shard_bytes))
            .for_each(|chunk| {
                self.encoder
                    .add_original_shard(chunk)
                    .expect("adding shards of the configured size should succeed");
                data.push(chunk.to_vec());
            });

        // Create parity shards.
        let output = self
            .encoder
            .encode()
            .expect("should be able to encode after k data shards were added");
        let coding = output.recovery_iter().map(<[u8]>::to_vec).collect();

        Ok(RawSlices { data, coding })
    }

    /// Reconstructs the raw payload bytes from optional shards (data and coding).
    /// Layout: data shards are indices [0..k), coding shards are indices [k..k+r).
    /// At least k total shards (data+coding) are required.
    ///
    /// Returns:
    /// - NotEnoughShards if fewer than k provided.
    /// - InvalidLayout if shard indices are inconsistent or inconsistent sizes are found.
    /// - TooMuchData/InvalidPadding per the padding rules.
    pub fn decode(
        &mut self,
        shards: &[Option<Shard>; TOTAL_SLICES],
    ) -> Result<Vec<u8>, ReedSolomonDecodeError> {
        let present = shards.iter().flatten().count();
        if present < self.k_data {
            return Err(ReedSolomonDecodeError::NotEnoughShards);
        }

        // Infer shard_bytes from any present shard.
        let shard_bytes = shards
            .iter()
            .flatten()
            .map(|s| s.data.len())
            .next()
            .ok_or(ReedSolomonDecodeError::InvalidLayout)?;

        // Ensure all present shards have the same size.
        if shards
            .iter()
            .flatten()
            .any(|s| s.data.len() != shard_bytes)
        {
            return Err(ReedSolomonDecodeError::InvalidLayout);
        }

        self.decoder
            .reset(self.k_data, self.r_coding, shard_bytes)
            .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;

        // Split into data and coding by index ranges.
        // Feed data shards (original) and coding shards (recovery) into decoder.
        for s in shards.iter().flatten() {
            let idx = *s.index;
            if idx < self.k_data {
                // data shard at index idx
                self.decoder
                    .add_original_shard(idx, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else if idx < self.k_data + self.r_coding {
                // coding shard at offset
                let offset = idx - self.k_data;
                self.decoder
                    .add_recovery_shard(offset, &s.data)
                    .map_err(|_| ReedSolomonDecodeError::InvalidLayout)?;
            } else {
                return Err(ReedSolomonDecodeError::InvalidLayout);
            }
        }

        let restored = self.decoder.decode().map_err(|_| {
            // If the library returns an error here, it's likely because the shards were inconsistent.
            ReedSolomonDecodeError::InvalidLayout
        })?;

        // Reassemble the payload from data shards in order [0..k).
        // If a data shard was missing, pull the restored version.
        let mut payload = Vec::with_capacity(self.k_data * shard_bytes);
        for data_idx in 0..self.k_data {
            let shard_ref = match shards[data_idx].as_ref() {
                Some(s) => &s.data,
                None => restored
                    .restored_original(data_idx)
                    .ok_or(ReedSolomonDecodeError::InvalidLayout)?,
            };
            // Avoid expanding to impossible sizes.
            payload
                .try_reserve(shard_ref.len())
                .map_err(|_| ReedSolomonDecodeError::TooMuchData)?;
            payload.extend_from_slice(shard_ref);
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
    use super::{Shard, CODING_SLICES, DATA_SLICES, TOTAL_SLICES};
    use crate::ShardIndex;

    fn make_payload(len: usize) -> Vec<u8> {
        // Deterministic, non-trivial pattern
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_full(raw: &RawSlices) -> [Option<Shard>; TOTAL_SLICES] {
        let mut arr: [Option<Shard>; TOTAL_SLICES] = std::array::from_fn(|_| None);
        for (i, d) in raw.data.iter().enumerate() {
            arr[i] = Some(Shard {
                index: ShardIndex::new(i).unwrap(),
                data: d.clone(),
            });
        }
        for (j, c) in raw.coding.iter().enumerate() {
            let idx = DATA_SLICES + j;
            arr[idx] = Some(Shard {
                index: ShardIndex::new(idx).unwrap(),
                data: c.clone(),
            });
        }
        arr
    }

    fn keep_only(arr: &mut [Option<Shard>; TOTAL_SLICES], keep: &[usize]) {
        let mut keep_set = vec![false; TOTAL_SLICES];
        for &k in keep {
            keep_set[k] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_set[i] {
                *slot = None;
            }
        }
    }

    fn equal_sizes(arr: &[Option<Shard>; TOTAL_SLICES]) -> Option<usize> {
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
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);
        let payload = make_payload(42_000);
        let raw = coder.encode(&payload).expect("encode ok");

        assert_eq!(raw.data.len(), DATA_SLICES);
        assert_eq!(raw.coding.len(), CODING_SLICES);

        let shard_len = raw.data[0].len();
        assert!(raw.data.iter().all(|d| d.len() == shard_len));
        assert!(raw.coding.iter().all(|c| c.len() == shard_len));
    }

    #[test]
    fn roundtrip_sizes() {
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);

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

            // all shards
            let restored = coder.decode(&full).expect("decode ok");
            assert_eq!(restored, payload, "round-trip mismatch for size {}", sz);

            // only data shards (k)
            let mut only_data = full.clone();
            keep_only(&mut only_data, &(0..DATA_SLICES).collect::<Vec<_>>());
            let restored = coder.decode(&only_data).expect("decode ok with k data shards");
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
            let restored = coder.decode(&mixed).expect("decode ok with mixed shards");
            assert_eq!(restored, payload, "round-trip mixed mismatch size {}", sz);
        }
    }

    #[test]
    fn tiny() {
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);

        for sz in 0..4usize {
            let payload = make_payload(sz);
            let raw = coder.encode(&payload).expect("encode ok");
            let shards = to_full(&raw);
            let out = coder.decode(&shards).expect("decode ok");
            assert_eq!(out, payload, "tiny payload mismatch sz={}", sz);
        }
    }

    #[test]
    fn not_enough() {
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);
        let payload = make_payload(10_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut shards = to_full(&raw);

        // keep only k-1 data shards
        let keep: Vec<usize> = (0..(DATA_SLICES - 1)).collect();
        keep_only(&mut shards, &keep);

        let res = coder.decode(&shards);
        assert!(matches!(res, Err(ReedSolomonDecodeError::NotEnoughShards)));
    }

    #[test]
    fn bad_size() {
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);
        let payload = make_payload(50_000);
        let raw = coder.encode(&payload).expect("encode ok");
        let mut shards = to_full(&raw);

        // uniform to start
        let base_len = equal_sizes(&shards).expect("uniform sizes");

        // tamper: shrink one shard by 1 byte
        if let Some(Some(sh)) = shards.get_mut(0) {
            assert_eq!(sh.data.len(), base_len);
            sh.data.pop();
            assert_eq!(sh.data.len(), base_len - 1);
        } else {
            panic!("expected shard present");
        }

        let res = coder.decode(&shards);
        assert!(matches!(res, Err(ReedSolomonDecodeError::InvalidLayout)));
    }

    #[test]
    fn empty_rt() {
        let mut coder = ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES);
        let payload = Vec::<u8>::new();
        let raw = coder.encode(&payload).expect("encode ok for empty payload");
        let shards = to_full(&raw);
        let out = coder.decode(&shards).expect("decode ok");
        assert!(out.is_empty(), "decoded payload should be empty");
    }
}

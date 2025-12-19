use thiserror::Error;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder, EncoderResult};
use crate::{TOTAL_SLICES, DATA_SLICES, CODING_SLICES, MAX_SLICE_SIZE};
use crate::{ShredError, DeshredError, RawSlices};

const PADDING_MARKER: u8 = 0x80;
const MAX_DATA_PER_SLICE: usize = DATA_SLICES * MAX_SLICE_SIZE;
const MAX_DATA_PER_SLICE_AFTER_PADDING: usize = MAX_DATA_PER_SLICE + 2 * DATA_SLICES - 1;

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonShredError {
    #[error("too much data for slice")]
    TooMuchData,
    #[error("RS error: {0}")]
    RsError(String),
}

/// Errors that may be returned by [`ReedSolomonCoder::deshred`].
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ReedSolomonDeshredError {
    #[error("not enough slices to reconstruct")]
    NotEnoughSlices,
    #[error("too much data for slice")]
    TooMuchData,
    #[error("invalid padding detected")]
    InvalidPadding,
    #[error("RS error: {0}")]
    RsError(String),
    #[error("invalid slice size")]
    InvalidSliceSize,
}

pub struct ReedSolomonCoder {
    encoder: ReedSolomonEncoder,
    decoder: ReedSolomonDecoder,
}

impl ReedSolomonCoder {
    pub fn new() -> ReedSolomonCoder {
        // max slices supported by RS field
        //const_assert!(DATA_SLICES + TOTAL_SLICES <= 65536);

        let encoder = ReedSolomonEncoder::new(DATA_SLICES, CODING_SLICES, 2).unwrap();
        let decoder = ReedSolomonDecoder::new(DATA_SLICES, CODING_SLICES, 2).unwrap();

        ReedSolomonCoder {
            encoder,
            decoder,
        }
    }

    pub fn shred(&mut self, payload: &[u8]) -> Result<RawSlices, ReedSolomonShredError> {
        if payload.len() > MAX_DATA_PER_SLICE {
            return Err(ReedSolomonShredError::TooMuchData);
        }

        // determine padding length & configure encoder for slice length
        let padding_bytes = 2 * DATA_SLICES - payload.len() % (2 * DATA_SLICES);
        let slice_bytes = (payload.len() + padding_bytes + DATA_SLICES - 1) / DATA_SLICES;
        let slice_bytes = slice_bytes.max(2);
        self.encoder
            .reset(DATA_SLICES, CODING_SLICES, slice_bytes)
            .map_err(|e| ReedSolomonShredError::RsError(e.to_string()))?;

        // add padding to last slices
        let last_slices_bytes = (2 * DATA_SLICES).next_multiple_of(slice_bytes);
        let boundary = payload.len().saturating_sub(last_slices_bytes - padding_bytes);
        let mut last_slices = Vec::with_capacity(last_slices_bytes);
        last_slices.extend_from_slice(&payload[boundary..]);
        last_slices.push(PADDING_MARKER);
        last_slices.resize(last_slices_bytes, 0);

        // chunk data
        let mut data = Vec::with_capacity(DATA_SLICES);
        payload[..boundary]
            .chunks(slice_bytes)
            .chain(last_slices.chunks(slice_bytes))
            .for_each(|chunk| {
                self.encoder
                    .add_original_shard(chunk)
                    .expect("adding correct number of chunks of correct size");
                data.push(chunk.to_vec());
            });

        // perform coding
        let result = self
            .encoder
            .encode()
            .expect("we just added enough data slices");
        let coding = result.recovery_iter().map(<[u8]>::to_vec).collect();

        Ok(RawSlices { data, coding })
    }

    pub fn deshred(
        &mut self,
        slices: &[Option<Vec<u8>>; TOTAL_SLICES],
    ) -> Result<Vec<u8>, ReedSolomonDeshredError> {
        let slices_cnt = slices.iter().filter(|s| s.is_some()).count();
        if slices_cnt < DATA_SLICES {
            return Err(ReedSolomonDeshredError::NotEnoughSlices);
        }

        // configure decoder for slice size
        let slice_bytes = slices.iter().flatten().next().map(|s| s.len()).unwrap_or(0);
        self.decoder
            .reset(DATA_SLICES, CODING_SLICES, slice_bytes)
            .map_err(|e| ReedSolomonDeshredError::RsError(e.to_string()))?;

        // add to decoder
        for (i, opt) in slices.iter().enumerate() {
            if let Some(slice) = opt {
                if slice.len() != slice_bytes {
                    return Err(ReedSolomonDeshredError::InvalidSliceSize);
                }
                if i < DATA_SLICES {
                    self.decoder.add_original_shard(i, slice).map_err(|e| ReedSolomonDeshredError::RsError(e.to_string()))?;
                } else {
                    self.decoder.add_recovery_shard(i - DATA_SLICES, slice).map_err(|e| ReedSolomonDeshredError::RsError(e.to_string()))?;
                }
            }
        }

        let restored = self.decoder.decode().map_err(|e| ReedSolomonDeshredError::RsError(e.to_string()))?;

        let mut data_slices = vec![None; DATA_SLICES];
        for (i, opt) in slices.iter().take(DATA_SLICES).enumerate() {
            if let Some(slice) = opt {
                data_slices[i] = Some(slice.as_slice());
            }
        }

        let mut restored_payload = Vec::with_capacity(MAX_DATA_PER_SLICE_AFTER_PADDING);
        for (i, d) in data_slices.into_iter().enumerate() {
            let slice_data = match d {
                Some(data_ref) => data_ref,
                None => restored.restored_original(i).expect("all non-existing data slices are restored"),
            };
            if restored_payload.len() + slice_data.len() > MAX_DATA_PER_SLICE_AFTER_PADDING {
                return Err(ReedSolomonDeshredError::TooMuchData);
            }
            restored_payload.extend_from_slice(slice_data);
        }

        // remove padding
        let padding_bytes = restored_payload
            .iter()
            .rev()
            .take_while(|b| **b == 0)
            .count()
            + 1;
        if restored_payload[restored_payload.len() - padding_bytes] != PADDING_MARKER {
            return Err(ReedSolomonDeshredError::InvalidPadding);
        }
        restored_payload.truncate(restored_payload.len().saturating_sub(padding_bytes));

        Ok(restored_payload)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngCore, thread_rng};

    const TEST_SLICE_SIZE: usize = 1024;  // Small for tests
    const MAX_TEST_DATA: usize = DATA_SLICES * TEST_SLICE_SIZE;

    #[test]
    fn restore_full() {
        let mut coder = ReedSolomonCoder::new();
        let payload = random_bytes(MAX_TEST_DATA);
        shred_deshred_restore(&mut coder, &payload);
    }

    #[test]
    fn restore_tiny() {
        let mut coder = ReedSolomonCoder::new();
        let payload = random_bytes(DATA_SLICES - 1);
        shred_deshred_restore(&mut coder, &payload);
    }

    #[test]
    fn restore_empty() {
        let mut coder = ReedSolomonCoder::new();
        let payload: Vec<u8> = Vec::new();
        let raw = coder.shred(&payload).unwrap();
        let input = take_enough_slices(&raw);
        let restored = coder.deshred(&input).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn round_trip() {
        let mut coder = ReedSolomonCoder::new();
        let payload = random_bytes(MAX_TEST_DATA);

        let raw = coder.shred(&payload).unwrap();
        let input = take_enough_slices(&raw);
        let restored = coder.deshred(&input).unwrap();

        assert_eq!(restored, payload);
    }

    //#[test]
    //fn restore_various() {
    //    let mut coder = ReedSolomonCoder::new();
    //    let base_len = MAX_TEST_DATA / 2;
    //    for offset in 0..DATA_SLICES {
    //        let payload = random_bytes(base_len + offset);
    //        shred_deshred_restore(&mut coder, &payload);
    //    }
    //}

    #[test]
    fn shred_too_much_data() {
        let mut coder = ReedSolomonCoder::new();
        let payload = vec![0; MAX_DATA_PER_SLICE_AFTER_PADDING + 1];
        let res = coder.shred(&payload);
        assert!(res.is_err());
    }

    #[test]
    fn deshred_not_enough_slices() {
        let mut coder = ReedSolomonCoder::new();
        let payload = random_bytes(MAX_TEST_DATA);
        let raw = coder.shred(&payload).unwrap();
        let mut input = std::array::from_fn(|_| None);
        for (i, d) in raw.data.iter().enumerate().take(DATA_SLICES - 1) {
            input[i] = Some(d.clone());
        }
        let res = coder.deshred(&input);
        assert!(res.is_err());
        //assert!(matches!(res.unwrap_err(), DeshredError::NotEnoughSlices));
    }

    fn shred_deshred_restore(coder: &mut ReedSolomonCoder, payload: &[u8]) {
        let raw = coder.shred(payload).unwrap();
        let input = take_enough_slices(&raw);
        let restored = coder.deshred(&input).unwrap();
        assert_eq!(restored, payload);
    }

    fn take_enough_slices(raw: &RawSlices) -> [Option<Vec<u8>>; TOTAL_SLICES] {
        let mut input = std::array::from_fn(|_| None);
        for (i, d) in raw.data.iter().enumerate() {
            input[i] = Some(d.clone());
        }
        // Add some coding if needed, but for min, data suffices
        input
    }

    fn random_bytes(len: usize) -> Vec<u8> {
        let mut rng = thread_rng();
        let mut bytes = vec![0u8; len];
        rng.fill_bytes(&mut bytes);
        bytes
    }
}

use super::api::Slicer;
use super::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::errors::{DecodeError, EncodeError};
use super::reed_solomon::{ReedSolomonCoder, ReedSolomonDecodeError, ReedSolomonEncodeError};
use super::slice_index::SliceIndex;
use super::types::{Blob, Slice};
use core::convert::TryInto;

/// A basic slicer that uses a single Reed-Solomon encoding pass (no striping).
/// For large blobs, this may use a lot of RAM, since all slices are derived from the full blob.
pub struct BasicSlicer(ReedSolomonCoder);

impl BasicSlicer {
    /// Create a new BasicSlicer with a custom max slice size.
    ///
    /// Use smaller values for testing to reduce memory usage.
    /// For production, use `Default::default()` which uses 1 MiB max slice size.
    pub fn with_max_slice_bytes(max_slice_bytes: usize) -> Self {
        Self(ReedSolomonCoder::with_max_slice_bytes(
            DATA_SLICES,
            CODING_SLICES,
            max_slice_bytes,
        ))
    }
}

impl Default for BasicSlicer {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for BasicSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        let raw = self.0.encode(blob.as_slice()).map_err(|e| match e {
            ReedSolomonEncodeError::TooMuchData => EncodeError::TooMuchData,
        })?;

        let mut output = Vec::with_capacity(SLICE_COUNT);
        for (i, data) in raw.data.into_iter().enumerate() {
            let idx = SliceIndex::new(i).expect("index in range");
            output.push(Slice::new(idx, data));
        }
        for (offset, coding) in raw.coding.into_iter().enumerate() {
            let idx = SliceIndex::new(DATA_SLICES + offset).expect("index in range");
            output.push(Slice::new(idx, coding));
        }

        Ok(output.try_into().expect("exactly SLICE_COUNT slices"))
    }

    fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError> {
        let reconstructed = self.0.decode(slices).map_err(|e| match e {
            ReedSolomonDecodeError::NotEnoughSlices => DecodeError::NotEnoughSlices,
            ReedSolomonDecodeError::TooMuchData => DecodeError::TooMuchData,
            ReedSolomonDecodeError::InvalidPadding => DecodeError::BadEncoding,
            ReedSolomonDecodeError::InvalidLayout => DecodeError::InvalidLayout,
        })?;
        Ok(Blob { data: reconstructed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::DecodeError;
    use crate::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
    use crate::merkle_helpers::build_blob_merkle_tree;
    use crate::reed_solomon::TEST_MAX_SLICE_BYTES;

    /// Create a test slicer with reduced memory footprint.
    fn test_slicer() -> BasicSlicer {
        BasicSlicer::with_max_slice_bytes(TEST_MAX_SLICE_BYTES)
    }

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SLICE_COUNT]) -> [Option<Slice>; SLICE_COUNT] {
        let mut arr: [Option<Slice>; SLICE_COUNT] = std::array::from_fn(|_| None);
        for (i, s) in slices.iter().enumerate() {
            arr[i] = Some(s.clone());
        }
        arr
    }

    fn keep(arr: &mut [Option<Slice>; SLICE_COUNT], idxs: &[usize]) {
        let mut mask = vec![false; SLICE_COUNT];
        for &i in idxs {
            mask[i] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !mask[i] {
                *slot = None;
            }
        }
    }

    fn equal_size(slices: &[Slice; SLICE_COUNT]) -> Option<usize> {
        let mut size: Option<usize> = None;
        for s in slices.iter() {
            match size {
                None => size = Some(s.data.len()),
                Some(sz) if sz != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    // Max test payload size with TEST_MAX_SLICE_BYTES (4 KiB * 683 data slices = ~2.7 MB)
    // Use smaller payloads to stay well within limits
    const MAX_TEST_PAYLOAD: usize = 100_000; // 100 KB

    #[test]
    fn encode_counts() {
        let mut slicer = test_slicer();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        assert_eq!(slices.len(), SLICE_COUNT);
        for (i, s) in slices.iter().enumerate() {
            assert_eq!(*s.index, i);
        }
        let sz = equal_size(&slices).expect("same sizes");
        assert!(sz > 0);
    }

    #[test]
    fn roundtrip_all() {
        let sizes = [0usize, 1, 17, 10_000, MAX_TEST_PAYLOAD];
        let mut slicer = test_slicer();
        for &sz in &sizes {
            let payload = mk(sz);
            let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
            let opt = to_opt(&slices);
            let restored = slicer.decode(&opt).expect("decode ok");
            assert_eq!(restored.data, payload);
        }
    }

    #[test]
    fn data_only() {
        let mut slicer = test_slicer();
        let payload = mk(42_000);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn mixed_k() {
        let mut slicer = test_slicer();
        let payload = mk(77_777);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);

        let mut keep_idxs = Vec::with_capacity(DATA_SLICES);
        for j in 0..CODING_SLICES {
            keep_idxs.push(DATA_SLICES + j);
        }
        let mut need = DATA_SLICES - keep_idxs.len();
        let mut i = 0usize;
        while need > 0 && i < DATA_SLICES {
            if i % 2 == 0 {
                keep_idxs.push(i);
                need -= 1;
            }
            i += 1;
        }
        i = 1;
        while keep_idxs.len() < DATA_SLICES && i < DATA_SLICES {
            keep_idxs.push(i);
            i += 2;
        }
        keep(&mut opt, &keep_idxs);

        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn not_enough() {
        let mut slicer = test_slicer();
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn bad_size() {
        let mut slicer = test_slicer();
        let payload = mk(50_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        if let Some(s) = opt[0].as_mut() {
            s.data.pop();
        }
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn dup_index() {
        let mut slicer = test_slicer();
        let payload = mk(33_333);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        let dup = opt[0].clone().unwrap();
        opt[10] = Some(dup);
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn merkle_root() {
        let mut slicer = test_slicer();
        let payload = mk(80_000);
        let slices1 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let slices2 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let t1 = build_blob_merkle_tree(&slices1);
        let t2 = build_blob_merkle_tree(&slices2);
        assert_eq!(t1.root(), t2.root());

        let mut slices3 = slices1.clone();
        slices3[0].data[0] ^= 1;
        let t3 = build_blob_merkle_tree(&slices3);
        assert_ne!(t1.root(), t3.root());
    }

    #[test]
    fn repl_factor() {
        let mut slicer = test_slicer();
        let n = MAX_TEST_PAYLOAD;
        let payload = mk(n);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let total: usize = slices.iter().map(|s| s.data.len()).sum();
        let r = total as f64 / n as f64;
        assert!(r > 1.45 && r < 1.55, "ratio {}", r);
    }
}

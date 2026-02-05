use super::api::Slicer;
use super::consts::SPOOL_GROUP_SIZE;
use super::errors::{DecodeError, EncodeError};
use super::reed_solomon::{ReedSolomonCoder, ReedSolomonDecodeError, ReedSolomonEncodeError};
use super::slice_index::SliceIndex;
use super::types::{Blob, Slice};
use core::convert::TryInto;
use tape_core::encoding::{EncodingProfile, RSParams};

/// A basic slicer that uses a single Reed-Solomon encoding pass (no striping).
///
/// **For testing/debugging only.** Supports blobs up to ~40 KB (4 KiB × 10 data slices).
/// For production workloads, use `StripedSlicer` or `RotatedSlicer` instead.
pub struct BasicSlicer {
    coder: ReedSolomonCoder,
    profile: EncodingProfile,
}

impl BasicSlicer {
    /// Create a BasicSlicer with default RS profile (k=10, m=10).
    pub fn new() -> Self {
        Self::with_profile(EncodingProfile::basic_default())
    }

    /// Create a BasicSlicer with a specific encoding profile.
    pub fn with_profile(profile: EncodingProfile) -> Self {
        let params = profile.rs_params();
        Self {
            coder: ReedSolomonCoder::new(params.k() as usize, params.m() as usize),
            profile,
        }
    }

    /// Create a BasicSlicer with custom RS parameters.
    pub fn with_params(k: u8, m: u8) -> Self {
        Self::with_profile(EncodingProfile::basic(RSParams::new(k, m)))
    }

    /// Get the encoding profile.
    pub fn profile(&self) -> EncodingProfile {
        self.profile
    }
}

impl Default for BasicSlicer {
    fn default() -> Self {
        Self::new()
    }
}

impl Slicer for BasicSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;

    fn k(&self) -> usize {
        self.coder.k()
    }

    fn m(&self) -> usize {
        self.coder.m()
    }

    fn encode(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError> {
        let raw = self.coder.encode(blob.as_slice()).map_err(|e| match e {
            ReedSolomonEncodeError::TooMuchData => EncodeError::TooMuchData,
        })?;

        let k = self.k();
        let n = k + self.m();

        // BasicSlicer requires n == SPOOL_GROUP_SIZE for now
        // (custom k/m that produce n < SPOOL_GROUP_SIZE would need padding strategy)
        assert_eq!(n, SPOOL_GROUP_SIZE, "BasicSlicer requires k + m == SPOOL_GROUP_SIZE");

        let mut output = Vec::with_capacity(SPOOL_GROUP_SIZE);
        for (i, data) in raw.data.into_iter().enumerate() {
            let idx = SliceIndex::new(i).expect("index in range");
            output.push(Slice::new(idx, data));
        }
        for (offset, coding) in raw.coding.into_iter().enumerate() {
            let idx = SliceIndex::new(k + offset).expect("index in range");
            output.push(Slice::new(idx, coding));
        }

        Ok(output.try_into().expect("exactly SPOOL_GROUP_SIZE slices"))
    }

    fn decode(
        &mut self,
        slices: &[Option<Slice>; SPOOL_GROUP_SIZE],
    ) -> Result<Blob, DecodeError> {
        let reconstructed = self.coder.decode(slices).map_err(|e| match e {
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
    use crate::api::Slicer;
    use crate::errors::DecodeError;
    use crate::consts::SPOOL_GROUP_SIZE;
    use crate::merkle_helpers::build_blob_merkle_tree;

    // Test constants (k=10, m=10)
    const K: usize = 10;
    const M: usize = 10;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(slices: &[Slice; SPOOL_GROUP_SIZE]) -> [Option<Slice>; SPOOL_GROUP_SIZE] {
        let mut arr: [Option<Slice>; SPOOL_GROUP_SIZE] = std::array::from_fn(|_| None);
        for (i, s) in slices.iter().enumerate() {
            arr[i] = Some(s.clone());
        }
        arr
    }

    fn keep(arr: &mut [Option<Slice>; SPOOL_GROUP_SIZE], idxs: &[usize]) {
        let mut mask = vec![false; SPOOL_GROUP_SIZE];
        for &i in idxs {
            mask[i] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !mask[i] {
                *slot = None;
            }
        }
    }

    fn equal_size_nonempty(slices: &[Slice; SPOOL_GROUP_SIZE], n: usize) -> Option<usize> {
        let mut size: Option<usize> = None;
        for s in slices.iter().take(n) {
            match size {
                None => size = Some(s.data.len()),
                Some(sz) if sz != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    // Max payload with default 4 KiB slices: 4 KiB * 10 data slices = 40 KB
    // Use smaller payloads to stay well within limits
    const MAX_TEST_PAYLOAD: usize = 30_000; // 30 KB

    #[test]
    fn encode_counts() {
        let mut slicer = BasicSlicer::default();
        assert_eq!(slicer.k(), K);
        assert_eq!(slicer.m(), M);

        let payload = mk(20_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        assert_eq!(slices.len(), SPOOL_GROUP_SIZE);
        for (i, s) in slices.iter().enumerate() {
            assert_eq!(*s.index, i);
        }
        let sz = equal_size_nonempty(&slices, K + M).expect("same sizes");
        assert!(sz > 0);
    }

    #[test]
    fn roundtrip_all() {
        let sizes = [0usize, 1, 17, 5_000, MAX_TEST_PAYLOAD];
        let mut slicer = BasicSlicer::default();
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
        let mut slicer = BasicSlicer::default();
        let payload = mk(20_000);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..K).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn mixed_k() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(25_000);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&slices);

        let mut keep_idxs = Vec::with_capacity(K);
        for j in 0..M {
            keep_idxs.push(K + j);
        }
        let mut need = K - keep_idxs.len();
        let mut i = 0usize;
        while need > 0 && i < K {
            if i % 2 == 0 {
                keep_idxs.push(i);
                need -= 1;
            }
            i += 1;
        }
        i = 1;
        while keep_idxs.len() < K && i < K {
            keep_idxs.push(i);
            i += 2;
        }
        keep(&mut opt, &keep_idxs);

        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn not_enough() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        keep(&mut opt, &(0..(K - 1)).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughSlices)));
    }

    #[test]
    fn bad_size() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(20_000);
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
        let mut slicer = BasicSlicer::default();
        let payload = mk(15_000);
        let slices = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&slices);
        let dup = opt[0].clone().unwrap();
        opt[10] = Some(dup);
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn merkle_root() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(25_000);
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
        let mut slicer = BasicSlicer::default();
        let n = MAX_TEST_PAYLOAD;
        let payload = mk(n);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let total: usize = slices.iter().take(K + M).map(|s| s.data.len()).sum();
        let r = total as f64 / n as f64;
        assert!(r > 1.95 && r < 2.10, "ratio {}", r);
    }

    #[test]
    fn custom_params() {
        // Test with k=6, m=14 (n=20, same as SPOOL_GROUP_SIZE)
        let mut slicer = BasicSlicer::with_params(6, 14);
        assert_eq!(slicer.k(), 6);
        assert_eq!(slicer.m(), 14);

        let payload = mk(10_000);
        let slices = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let opt = to_opt(&slices);
        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }
}

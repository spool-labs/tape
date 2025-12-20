use super::api::Slicer;
use super::consts::{CODING_SLICES, DATA_SLICES, TOTAL_SLICES};
use super::errors::{DecodeError, EncodeError};
use super::reed_solomon::{ReedSolomonCoder, ReedSolomonDecodeError, ReedSolomonEncodeError};
use super::shard_index::ShardIndex;
use super::types::{Blob, Shard};
use core::convert::TryInto;

/// A basic slicer that uses a single Reed–Solomon encoding pass (no striping).
/// For large blobs, this may use a lot of RAM, since all shards are derived from the full blob.
pub struct BasicSlicer(ReedSolomonCoder);

impl Default for BasicSlicer {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for BasicSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SHREDS: usize = DATA_SLICES;
    const CODING_OUTPUT_SHREDS: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Shard; TOTAL_SLICES], EncodeError> {
        let raw = self.0.encode(blob.as_slice()).map_err(|e| match e {
            ReedSolomonEncodeError::TooMuchData => EncodeError::TooMuchData,
        })?;

        let mut output = Vec::with_capacity(TOTAL_SLICES);
        for (i, data) in raw.data.into_iter().enumerate() {
            let idx = ShardIndex::new(i).expect("index in range");
            output.push(Shard::new(idx, data));
        }
        for (offset, coding) in raw.coding.into_iter().enumerate() {
            let idx = ShardIndex::new(DATA_SLICES + offset).expect("index in range");
            output.push(Shard::new(idx, coding));
        }

        Ok(output.try_into().expect("exactly TOTAL_SLICES shards"))
    }

    fn decode(
        &mut self,
        shards: &[Option<Shard>; TOTAL_SLICES],
    ) -> Result<Blob, DecodeError> {
        let reconstructed = self.0.decode(shards).map_err(|e| match e {
            ReedSolomonDecodeError::NotEnoughShards => DecodeError::NotEnoughShards,
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
    use crate::consts::{CODING_SLICES, DATA_SLICES, TOTAL_SLICES};
    use crate::merkle_helpers::build_blob_merkle_tree;

    fn mk(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_opt(shards: &[Shard; TOTAL_SLICES]) -> [Option<Shard>; TOTAL_SLICES] {
        let mut arr: [Option<Shard>; TOTAL_SLICES] = std::array::from_fn(|_| None);
        for (i, s) in shards.iter().enumerate() {
            arr[i] = Some(s.clone());
        }
        arr
    }

    fn keep(arr: &mut [Option<Shard>; TOTAL_SLICES], idxs: &[usize]) {
        let mut mask = vec![false; TOTAL_SLICES];
        for &i in idxs {
            mask[i] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !mask[i] {
                *slot = None;
            }
        }
    }

    fn equal_size(shards: &[Shard; TOTAL_SLICES]) -> Option<usize> {
        let mut size: Option<usize> = None;
        for s in shards.iter() {
            match size {
                None => size = Some(s.data.len()),
                Some(sz) if sz != s.data.len() => return None,
                _ => {}
            }
        }
        size
    }

    #[test]
    fn encode_counts() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(123_456);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");
        assert_eq!(shards.len(), TOTAL_SLICES);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(*s.index, i);
        }
        let sz = equal_size(&shards).expect("same sizes");
        assert!(sz > 0);
    }

    #[test]
    fn roundtrip_all() {
        let sizes = [0usize, 1, 17, 10_000, 250_000];
        let mut slicer = BasicSlicer::default();
        for &sz in &sizes {
            let payload = mk(sz);
            let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
            let opt = to_opt(&shards);
            let restored = slicer.decode(&opt).expect("decode ok");
            assert_eq!(restored.data, payload);
        }
    }

    #[test]
    fn data_only() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(42_000);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&shards);
        keep(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());
        let restored = slicer.decode(&opt).expect("decode ok");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn mixed_k() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(77_777);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let mut opt = to_opt(&shards);

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
        let mut slicer = BasicSlicer::default();
        let payload = mk(10_000);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&shards);
        keep(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughShards)));
    }

    #[test]
    fn bad_size() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(50_000);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&shards);
        if let Some(s) = opt[0].as_mut() {
            s.data.pop();
        }
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn dup_index() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(33_333);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");
        let mut opt = to_opt(&shards);
        let dup = opt[0].clone().unwrap();
        opt[10] = Some(dup);
        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn merkle_root() {
        let mut slicer = BasicSlicer::default();
        let payload = mk(120_000);
        let shards1 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let shards2 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let t1 = build_blob_merkle_tree(&shards1);
        let t2 = build_blob_merkle_tree(&shards2);
        assert_eq!(t1.root(), t2.root());

        let mut shards3 = shards1.clone();
        shards3[0].data[0] ^= 1;
        let t3 = build_blob_merkle_tree(&shards3);
        assert_ne!(t1.root(), t3.root());
    }

    #[test]
    fn repl_factor() {
        let mut slicer = BasicSlicer::default();
        let n = 1_000_000;
        let payload = mk(n);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let total: usize = shards.iter().map(|s| s.data.len()).sum();
        let r = total as f64 / n as f64;
        assert!(r > 1.45 && r < 1.55, "ratio {}", r);
    }
}


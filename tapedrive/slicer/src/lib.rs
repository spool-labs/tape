#![allow(clippy::len_without_is_empty)]

pub mod reed_solomon;
pub mod shard_index;
pub mod slice_index;

use tape_crypto::Hash;
use tape_crypto::merkle::MerkleTree;
use reed_solomon::ReedSolomonCoder;
use shard_index::ShardIndex;
use std::convert::TryInto;
use thiserror::Error;

/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per shard.
pub const MERKLE_HEIGHT: usize = 10;

/// Total slices (data + coding) produced by Reed–Solomon per stripe.
/// Must match on-chain apportionment logic.
pub const TOTAL_SLICES: usize = 1 << MERKLE_HEIGHT; // 1024

/// Set f for the 3f+1 layout that tolerates up to f failures.
/// For TOTAL_SLICES = 3f + 1, f = (TOTAL_SLICES - 1)/3.
pub const F: usize = (TOTAL_SLICES - 1) / 3;

/// Number of coding slices (parity) per stripe.
/// With 3f+1 layout, coding = f and data = 2f + 1.
pub const CODING_SLICES: usize = F; // 341 for 1024 total
pub const DATA_SLICES: usize = TOTAL_SLICES - CODING_SLICES; // 683 for 1024 total

/// Alias for our fixed-size Merkle tree over shard hashes for a blob.
pub type BlobMerkleTree = MerkleTree<MERKLE_HEIGHT>;
pub type BlobMerkleRoot = Hash;

/// Smallest unit stored off-chain corresponding to one index over the whole blob.
/// For BasicSlicer (one stripe), each Shard contains exactly one slice (data or coding).
/// For StripedSlicer (multi-stripe), each Shard would append one slice per stripe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    pub index: ShardIndex,
    pub data: Vec<u8>,
}

impl Shard {
    pub fn new(index: ShardIndex, data: Vec<u8>) -> Self {
        Self { index, data }
    }
}

/// A blob of data to be encoded.
/// The CLI/app handles file access. We just operate on bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Blob {
    pub data: Vec<u8>,
}

impl From<Vec<u8>> for Blob {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl Blob {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

/// Error types for encoding and decoding at the slicer level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    #[error("too much data to encode in a single stripe/coder configuration")]
    TooMuchData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("not enough shards to reconstruct")]
    NotEnoughShards,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding in recovered data")]
    BadEncoding,
    #[error("invalid layout or inconsistent shards")]
    InvalidLayout,
}

/// Trait for encoding and decoding blobs into shards.
///
/// The intention:
/// - BasicSlicer encodes a single stripe into 1024 shards.
/// - StripedSlicer (future) splits the blob into multiple stripes to keep RAM usage bounded,
///   and still outputs 1024 shards total, each shard accumulating one slice per stripe.
pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;          // Maximum supported input size for this slicer implementation
    const DATA_OUTPUT_SHREDS: usize;     // number of data slices produced
    const CODING_OUTPUT_SHREDS: usize;   // number of coding slices produced

    fn encode(&mut self, blob: Blob) -> Result<[Shard; TOTAL_SLICES], EncodeError>;

    fn decode(
        &mut self,
        shards: &[Option<Shard>; TOTAL_SLICES],
    ) -> Result<Blob, DecodeError>;
}

/// A basic slicer that uses a single Reed–Solomon encoding pass (no striping).
/// For large blobs, this may use a lot of RAM, since all shards are derived from the full blob.
pub struct BasicSlicer(ReedSolomonCoder);

impl Default for BasicSlicer {
    fn default() -> Self {
        // One coder for the 3f+1 layout with fixed total and coding counts.
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for BasicSlicer {
    // For BasicSlicer we do not impose an artificial small max size;
    // in practice, memory usage will be the limiting factor.
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SHREDS: usize = DATA_SLICES;
    const CODING_OUTPUT_SHREDS: usize = CODING_SLICES;

    fn encode(&mut self, blob: Blob) -> Result<[Shard; TOTAL_SLICES], EncodeError> {
        let raw = self.0.encode(blob.as_slice()).map_err(|e| match e {
            reed_solomon::ReedSolomonEncodeError::TooMuchData => EncodeError::TooMuchData,
        })?;

        let mut output = Vec::with_capacity(TOTAL_SLICES);

        // Data slices go to indices [0..DATA_SLICES)
        for (i, data) in raw.data.into_iter().enumerate() {
            let idx = ShardIndex::new(i).expect("index in range");
            output.push(Shard::new(idx, data));
        }

        // Coding slices go to indices [DATA_SLICES..TOTAL_SLICES)
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
        // Basic format only: index < DATA_SLICES are data, others are coding.
        let reconstructed = self.0.decode(shards).map_err(|e| match e {
            reed_solomon::ReedSolomonDecodeError::NotEnoughShards => DecodeError::NotEnoughShards,
            reed_solomon::ReedSolomonDecodeError::TooMuchData => DecodeError::TooMuchData,
            reed_solomon::ReedSolomonDecodeError::InvalidPadding => DecodeError::BadEncoding,
            reed_solomon::ReedSolomonDecodeError::InvalidLayout => DecodeError::InvalidLayout,
        })?;
        Ok(Blob { data: reconstructed })
    }
}

/// A striped slicer (stub) that would split the blob into multiple stripes.
/// Each stripe is encoded into 1024 slices and appended to the corresponding 1024 shards.
/// This keeps per-stripe memory bounded at the cost of multiple RS passes.
/// Not implemented here, but the skeleton is provided.
pub struct StripedSlicer(ReedSolomonCoder);

impl Default for StripedSlicer {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for StripedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SHREDS: usize = DATA_SLICES;
    const CODING_OUTPUT_SHREDS: usize = CODING_SLICES;

    fn encode(&mut self, _blob: Blob) -> Result<[Shard; TOTAL_SLICES], EncodeError> {
        // TODO: Implement multi-stripe logic to handle huge blobs without large RAM usage.
        // For now, just error to make it explicit.
        todo!()
    }

    fn decode(
        &mut self,
        _shards: &[Option<Shard>; TOTAL_SLICES],
    ) -> Result<Blob, DecodeError> {
        // TODO: Implement the reverse: deaggregate shards into stripes and decode stripe-by-stripe.
        todo!()
    }
}

/// Computes the Merkle tree over shard bytes (one leaf per shard).
/// Each leaf is leaf(hashv(LEAF_LABEL || shard_bytes)) as in the provided Merkle tree.
pub fn build_blob_merkle_tree(shards: &[Shard; TOTAL_SLICES]) -> BlobMerkleTree {
    let mut tree = BlobMerkleTree::new();
    for s in shards.iter() {
        // This adds the leaf in index order from 0..TOTAL_SLICES.
        // For missing shards, pass empty (if building a partial tree).
        tree.add_leaf(&s.data).expect("tree capacity");
    }
    tree
}

/// Convenience to compute leaf hashes externally, if needed for proofs.
/// The Merkle implementation manages hashing internally, so usually not required.
pub fn blob_merkle_root(shards: &[Shard; TOTAL_SLICES]) -> BlobMerkleRoot {
    build_blob_merkle_tree(shards).root()
}


#[cfg(test)]
mod basic_slicer_tests {
    use super::*;

    fn make_payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn to_option_array(shards: &[Shard; TOTAL_SLICES]) -> [Option<Shard>; TOTAL_SLICES] {
        let mut arr: [Option<Shard>; TOTAL_SLICES] = std::array::from_fn(|_| None);
        for (i, s) in shards.iter().enumerate() {
            arr[i] = Some(s.clone());
        }
        arr
    }

    fn keep_only_indices(arr: &mut [Option<Shard>; TOTAL_SLICES], keep: &[usize]) {
        let mut keep_mask = vec![false; TOTAL_SLICES];
        for &i in keep {
            keep_mask[i] = true;
        }
        for (i, slot) in arr.iter_mut().enumerate() {
            if !keep_mask[i] {
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
        let payload = make_payload(123_456);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");
        assert_eq!(shards.len(), TOTAL_SLICES);

        for (i, s) in shards.iter().enumerate() {
            assert_eq!(*s.index, i, "shard index mismatch at {}", i);
        }

        let sz = equal_size(&shards).expect("all shard sizes equal");
        assert!(sz > 0, "shard size should be > 0");
    }

    #[test]
    fn roundtrip_all() {
        let sizes = [0usize, 1, 17, 10_000, 250_000];
        let mut slicer = BasicSlicer::default();

        for &sz in &sizes {
            let payload = make_payload(sz);
            let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
            let opt = to_option_array(&shards);
            let restored = slicer.decode(&opt).expect("decode ok");
            assert_eq!(restored.data, payload, "round-trip mismatch for size {}", sz);
        }
    }

    #[test]
    fn data_only() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(42_000);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");

        let mut opt = to_option_array(&shards);
        keep_only_indices(&mut opt, &(0..DATA_SLICES).collect::<Vec<_>>());

        let restored = slicer.decode(&opt).expect("decode ok with k data shards");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn mixed_k() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(77_777);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");

        let mut opt = to_option_array(&shards);

        // Keep all coding + some data until exactly k total.
        let mut keep = Vec::with_capacity(DATA_SLICES);
        for j in 0..CODING_SLICES {
            keep.push(DATA_SLICES + j);
        }
        let mut needed = DATA_SLICES - keep.len();
        let mut i = 0usize;
        while needed > 0 && i < DATA_SLICES {
            if i % 2 == 0 {
                keep.push(i);
                needed -= 1;
            }
            i += 1;
        }
        i = 1;
        while keep.len() < DATA_SLICES && i < DATA_SLICES {
            keep.push(i);
            i += 2;
        }
        assert_eq!(keep.len(), DATA_SLICES);

        keep_only_indices(&mut opt, &keep);

        let restored = slicer.decode(&opt).expect("decode ok with mixed shards");
        assert_eq!(restored.data, payload);
    }

    #[test]
    fn not_enough() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(10_000);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");

        let mut opt = to_option_array(&shards);
        keep_only_indices(&mut opt, &(0..DATA_SLICES - 1).collect::<Vec<_>>());

        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::NotEnoughShards)));
    }

    #[test]
    fn bad_size() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(50_000);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");

        let mut opt = to_option_array(&shards);

        // Shrink one shard by 1 byte
        if let Some(s) = opt[0].as_mut() {
            assert!(s.data.len() > 0);
            s.data.pop();
        }

        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn dup_index() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(33_333);
        let shards = slicer.encode(Blob::from(payload)).expect("encode ok");

        let mut opt = to_option_array(&shards);

        // Duplicate shard index 0 into a different position.
        let dup = opt[0].clone().expect("present");
        opt[10] = Some(dup);

        let res = slicer.decode(&opt);
        assert!(matches!(res, Err(DecodeError::InvalidLayout)));
    }

    #[test]
    fn merkle_root() {
        let mut slicer = BasicSlicer::default();
        let payload = make_payload(120_000);
        let shards1 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");
        let shards2 = slicer.encode(Blob::from(payload.clone())).expect("encode ok");

        let tree1 = build_blob_merkle_tree(&shards1);
        let tree2 = build_blob_merkle_tree(&shards2);
        assert_eq!(tree1.root(), tree2.root());

        let mut shards3 = shards1.clone();
        shards3[0].data[0] ^= 0x01;
        let tree3 = build_blob_merkle_tree(&shards3);
        assert_ne!(tree1.root(), tree3.root());
    }

    #[test]
    fn repl_factor() {
        let mut slicer = BasicSlicer::default();
        let payload_len = 1_000_000;
        let payload = make_payload(payload_len);
        let shards = slicer.encode(Blob::from(payload.clone())).expect("encode ok");

        let total_bytes: usize = shards.iter().map(|s| s.data.len()).sum();
        let ratio = total_bytes as f64 / payload_len as f64;

        assert!(ratio > 1.45 && ratio < 1.55, "replication ratio out of band: {}", ratio);
    }
}

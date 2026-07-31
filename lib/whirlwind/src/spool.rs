//! Builds a real coded spool with the workspace slicer and merkle code.
//!
//! Everything the harness measures runs against this: Clay-encoded slices, the
//! actual BlobMerkleTree commitment, and the real repair layout. Nothing here
//! approximates the protocol; it calls the same functions a node calls.

use anyhow::{anyhow, bail, Result};
use tape_core::encoding::ClayParams;
use tape_core::erasure::{slice_root, GROUP_SIZE, SLICE_TREE_HEIGHT};
use tape_core::track::blob::BlobEncoding;
use tape_core::types::{StorageUnits, StripeCount};
use tape_crypto::Hash;
use tape_slicer::{
    build_blob_merkle_tree, BlobMerkleTree, ClayCoder, ErasureCoder, MappingStrategy, Slicer,
};

/// A coded track laid out exactly as a node stores it: n slices, the merkle
/// tree over those slices, and the registered commitment root.
pub struct Spool {
    /// The n coded slices. Slice i is what spool owner i stores.
    pub slices: Vec<Vec<u8>>,
    /// The top tree over the slices, whose leaves are the per-slice sub-leaf roots.
    pub tree: BlobMerkleTree,
    /// The registered encoding: commitment, profile, stripe layout, slice roots.
    pub encoding: BlobEncoding,
    /// Registered track commitment C_x = tree root.
    pub commitment: Hash,
    /// Original payload length.
    pub blob_len: usize,
    /// Adaptive stripe size the commitment was built with.
    pub stripe_size: usize,
    /// Per-stripe coded chunk size in bytes.
    pub chunk_size: usize,
    /// Clay sub-packetization (sub-chunks per chunk).
    pub alpha: usize,
    /// Clay sub-chunks fetched per helper during repair.
    pub beta: usize,
    /// Number of stripes in the blob.
    pub num_stripes: usize,
    /// Shard-to-slice mapping strategy in force.
    pub strategy: MappingStrategy,
    /// Total positions in the group (n).
    pub group_size: usize,
    /// Reconstruction threshold (k).
    pub data_shards: usize,
    /// Repair helper count (d).
    pub helper_count: usize,
}

impl Spool {
    /// Encode a deterministic payload of blob_len bytes into a real coded
    /// spool using the production Clay defaults (n=20, k=7, d=16, rotated).
    pub fn build(blob_len: usize) -> Result<Self> {
        if blob_len == 0 {
            bail!("blob_len must be greater than zero");
        }

        let payload = deterministic_payload(blob_len, 0);

        let mut slicer = Slicer::clay_default();
        let slices = slicer
            .encode(&payload)
            .map_err(|error| anyhow!("clay encode failed: {error:?}"))?;

        // encode selects the adaptive stripe size and records it on the slicer,
        // so this is the exact size the commitment was built with.
        let stripe_size = slicer.stripe_size();
        let strategy = slicer.strategy();

        let tree = build_blob_merkle_tree(&slices);
        let commitment = tree.root();
        let encoding = blob_encoding(&slicer, blob_len, &slices)?;

        let coder = ClayCoder::from_params(ClayParams::default());
        let chunk_size = coder.track_chunk_size(stripe_size, blob_len);
        let num_stripes = blob_len.div_ceil(stripe_size);

        Ok(Self {
            slices,
            tree,
            encoding,
            commitment,
            blob_len,
            stripe_size,
            chunk_size,
            alpha: coder.alpha(),
            beta: coder.beta(),
            num_stripes,
            strategy,
            group_size: coder.n(),
            data_shards: coder.k(),
            helper_count: coder.d(),
        })
    }

    /// Byte length of one coded slice (the unit a merkle leaf covers).
    pub fn slice_len(&self) -> usize {
        self.slices.first().map(Vec::len).unwrap_or(0)
    }

    /// Sub-chunk size in bytes (chunk_size / alpha).
    pub fn sub_chunk_size(&self) -> usize {
        self.chunk_size / self.alpha
    }
}

/// Build the encoding a track registers on chain for an encoded payload.
///
/// This is the record every verifier holds: the commitment, the coding profile
/// and stripe layout the slicer chose, and the per-slice sub-leaf roots a
/// challenge proof anchors against.
pub fn blob_encoding<C: ErasureCoder>(
    slicer: &Slicer<C>,
    payload_len: usize,
    slices: &[Vec<u8>],
) -> Result<BlobEncoding> {
    if slices.len() != GROUP_SIZE {
        bail!("encoded {} slices, need {GROUP_SIZE}", slices.len());
    }

    let mut leaves = [Hash::default(); GROUP_SIZE];
    for (leaf, slice) in leaves.iter_mut().zip(slices) {
        *leaf = slice_root(slice)
            .ok_or_else(|| anyhow!("slice of {} bytes exceeds the sub-leaf tree", slice.len()))?;
    }

    let stripe_size = slicer.stripe_size();
    let mut encoding = BlobEncoding {
        size: StorageUnits::from_bytes(payload_len as u64),
        commitment: Hash::default(),
        profile: slicer.profile(),
        stripe_size: StorageUnits::from_bytes(stripe_size as u64),
        stripe_count: StripeCount(payload_len.div_ceil(stripe_size) as u64),
        leaves,
    };
    encoding.commitment = encoding.commitment_root();

    Ok(encoding)
}

/// Merkle tree height for the slice commitment, surfaced for reporting.
pub const TREE_HEIGHT: usize = SLICE_TREE_HEIGHT;

/// Group size (leaf count of the slice commitment), surfaced for reporting.
pub const LEAF_COUNT: usize = GROUP_SIZE;

/// Agreement threshold for one spool group, which pins the Byzantine bound at 6
pub const AGREEMENT_THRESHOLD: usize = 14;

/// Deterministic non-trivial payload via a salted xorshift, reproducible per
/// caller so a run is stable and the codec never sees an all-zero input.
pub fn deterministic_payload(len: usize, salt: u64) -> Vec<u8> {
    let mut data = vec![0u8; len];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ salt.wrapping_mul(0xD1B5_4A32_D192_ED03);
    for byte in data.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state >> 24) as u8;
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    // a built spool holds one slice per group position
    #[test]
    fn slice_count() {
        let spool = Spool::build(100_000).unwrap();
        assert_eq!(spool.slices.len(), LEAF_COUNT);
        assert_eq!(spool.group_size, LEAF_COUNT);
    }

    // every slice of a spool has the same length
    #[test]
    fn slice_layout() {
        let spool = Spool::build(100_000).unwrap();
        let expected = spool.num_stripes * spool.chunk_size + 48;
        for slice in &spool.slices {
            assert_eq!(slice.len(), expected);
        }
    }

    // the chunk size divides evenly into clay sub-chunks
    #[test]
    fn chunk_divides() {
        let spool = Spool::build(250_000).unwrap();
        assert_eq!(spool.chunk_size % spool.alpha, 0);
    }
}

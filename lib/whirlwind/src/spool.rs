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

    #[test]
    fn slice_count() {
        let spool = Spool::build(100_000).expect("build spool");
        assert_eq!(spool.slices.len(), LEAF_COUNT);
        assert_eq!(spool.group_size, LEAF_COUNT);
    }

    #[test]
    fn slice_layout() {
        let spool = Spool::build(100_000).expect("build spool");
        let expected = spool.num_stripes * spool.chunk_size + 48;
        for slice in &spool.slices {
            assert_eq!(slice.len(), expected);
        }
    }

    #[test]
    fn chunk_divides() {
        let spool = Spool::build(250_000).expect("build spool");
        assert_eq!(spool.chunk_size % spool.alpha, 0);
    }
}

#[cfg(test)]
mod leaf_sizing {
    //! Sizing evidence for the sub-leaf: 1 KiB at height 16 against 4 KiB at 14.
    //!
    //! Each height is the one its leaf size needs to cover a 64 MiB track held in a
    //! single slice, which is Reed-Solomon at k=1 and the case the on-chain height
    //! has to survive. The timings are taken on the Clay k=7 worst case instead,
    //! since that is the largest slice the default profile produces and where the
    //! per-track cost actually lands. Both candidates are asserted to cover it and to
    //! round-trip a proof, using the real merkle code and the shipped response shape.
    //! Run with nocapture to read the timings.

    use std::time::Instant;

    use tape_crypto::hash::{hashv, Hash};
    use tape_crypto::merkle::{compute_path, create_proof_from_leaf_hashes, hash_leaf, root_from_leaf_hashes};

    /// Sub-leaves a 64 MiB track puts in one Clay k=7 slice at 1 KiB, measured with
    /// the commitment probe at that payload size. It moved from 9,497 when the
    /// slicer started deriving stripe sizes instead of picking them off a ladder.
    const MAX_TRACK_SUB_LEAVES_1K: usize = 9364;
    const SLICE_BYTES: usize = MAX_TRACK_SUB_LEAVES_1K * 1024;
    const SLICES_PER_TRACK: usize = 20;
    const SLICE_TREE_HEIGHT: usize = 5;

    fn slice_bytes() -> Vec<u8> {
        let mut data = vec![0u8; SLICE_BYTES];
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        for byte in data.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = (state >> 24) as u8;
        }
        data
    }

    fn micros(start: Instant) -> f64 {
        start.elapsed().as_nanos() as f64 / 1_000.0
    }

    /// One configuration's timings, all on a single slice unless noted.
    fn measure<const HEIGHT: usize>(label: &str, slice: &[u8], leaf_bytes: usize) {
        let chunks: Vec<&[u8]> = slice.chunks(leaf_bytes).collect();
        assert!(
            chunks.len() <= 1 << HEIGHT,
            "{label}: {} leaves over capacity {}",
            chunks.len(),
            1 << HEIGHT
        );

        // Write side: hash every sub-leaf, then build the per-slice tree.
        let start = Instant::now();
        let hashes: Vec<Hash> = chunks.iter().map(|chunk| hash_leaf(chunk)).collect();
        let leaf_hash_us = micros(start);

        let start = Instant::now();
        let slice_root = root_from_leaf_hashes::<HEIGHT>(&hashes);
        let tree_us = micros(start);

        // Top tree over the slice roots, built once per track.
        let roots = vec![slice_root; SLICES_PER_TRACK];
        let start = Instant::now();
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&roots);
        let top_us = micros(start);

        // Round side: produce a proof for one sampled sub-leaf.
        let index = chunks.len() / 3;
        let start = Instant::now();
        let sub_proof = create_proof_from_leaf_hashes::<HEIGHT>(&hashes, index).expect("sub proof");
        let top_proof = create_proof_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&roots, 0).expect("top proof");
        let prove_us = micros(start);

        // Round side: verify it, the work every observer repeats.
        let start = Instant::now();
        let leaf = hash_leaf(chunks[index]);
        let reached_root = *compute_path(&sub_proof, leaf, index as u64, HEIGHT)
            .last()
            .expect("path");
        let reached_commitment = *compute_path(&top_proof, reached_root, 0, SLICE_TREE_HEIGHT)
            .last()
            .expect("path");
        let verify_us = micros(start);
        assert_eq!(reached_root, slice_root);
        assert_eq!(reached_commitment, commitment);

        // The shipped response is the leaf and its path to the slice root. It carries
        // neither the root nor the top path, because a verifier holds the encoding.
        let response = leaf_bytes + sub_proof.len() * Hash::LEN;
        let track_build_ms = (leaf_hash_us + tree_us) * SLICES_PER_TRACK as f64 / 1_000.0 + top_us / 1_000.0;

        println!("{label}");
        println!("  sub-leaves per slice   {}", chunks.len());
        println!("  leaf hashing           {leaf_hash_us:.0} us per slice");
        println!("  slice tree build       {tree_us:.0} us per slice");
        println!("  top tree build         {top_us:.1} us per track");
        println!("  whole track commitment {track_build_ms:.1} ms for {SLICES_PER_TRACK} slices");
        println!("  prove one sub-leaf     {prove_us:.1} us");
        println!("  verify one sub-leaf    {verify_us:.1} us");
        println!("  response on the wire   {response} B");
        println!();
    }

    #[test]
    fn leaf_sizes() {
        let slice = slice_bytes();
        println!();
        println!(
            "one slice of the largest legal track: {} B ({:.2} MiB), sha256 over {} slices per track",
            SLICE_BYTES,
            SLICE_BYTES as f64 / (1024.0 * 1024.0),
            SLICES_PER_TRACK,
        );
        println!();
        measure::<16>("1 KiB leaves, height 16", &slice, 1024);
        measure::<14>("4 KiB leaves, height 14", &slice, 4096);

        // A single node hash for reference, the unit both trees are built from.
        let a = hashv(&[b"a"]);
        let start = Instant::now();
        let rounds = 100_000;
        for _ in 0..rounds {
            std::hint::black_box(hashv(&[a.as_ref(), a.as_ref()]));
        }
        println!("one 64 B node hash     {:.3} us", micros(start) / rounds as f64);
    }
}

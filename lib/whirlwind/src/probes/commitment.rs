//! Probes what the registered commitment commits to at each of its two levels,
//! and what a challenge response against each level must carry.
//!
//! The top tree has one leaf per slice, and that leaf is the root of the slice's
//! sub-leaf tree. Reading the top level alone gives a response that is a hash and
//! a path, so an owner that cached those bytes answers forever without holding
//! the slice. Reading the slice bytes closes that hole but makes the response the
//! whole slice, which grows with the track. The sub-leaf level is the one the
//! challenge samples: the response carries real bytes and stays one fixed size no
//! matter how large the track is.

use anyhow::{anyhow, Result};
use tape_core::erasure::{slice_root, sub_leaf_count, SUB_TREE_HEIGHT};
use tape_core::types::SpoolIndex;
use tape_crypto::merkle::create_proof_from_leaf_hashes;
use tape_crypto::Hash;

use crate::spool::{Spool, LEAF_COUNT, TREE_HEIGHT};

/// Position whose slice the readings are taken against; a spool owner holds one.
const OWNER_INDEX: usize = 3;

/// One sampled sub-leaf of that slice, away from both ends.
const SAMPLED_SUB_LEAF: usize = 1;

/// The commitment's structure and the three readings a response can take.
pub struct CommitmentReport {
    /// Leaves under the registered commitment (GROUP_SIZE).
    pub leaf_count: usize,
    /// Height of the top tree over the slice roots.
    pub tree_height: usize,
    /// Bytes covered by one top-tree leaf, i.e. one whole slice.
    pub slice_bytes: usize,
    /// Sample leaves the slice is divided into beneath its root.
    pub sub_leaves_per_slice: usize,
    /// Height of the per-slice sub-leaf tree.
    pub sub_tree_height: usize,
    /// Bytes a top-level "proof only" response carries (slice root + top path).
    pub slice_root_bytes: usize,
    /// Whether that response, carrying no slice bytes, passes verification.
    pub slice_root_passes: bool,
    /// Bytes a whole-slice response carries (slice + top path).
    pub full_slice_bytes: usize,
    /// Whether verification that recomputes the slice root from the bytes passes.
    pub full_slice_verifies: bool,
    /// Bytes a sampled sub-leaf response carries (leaf + path to the slice root).
    pub sub_leaf_bytes: usize,
    /// Whether the sampled sub-leaf verifies against the registered slice root.
    pub sub_leaf_verifies: bool,
    /// Whether a sub-leaf response replayed at a neighbouring position verifies.
    pub sub_leaf_replays: bool,
}

impl CommitmentReport {
    pub fn measure(spool: &Spool) -> Result<Self> {
        let owner_slice = spool
            .slices
            .get(OWNER_INDEX)
            .ok_or_else(|| anyhow!("owner slice {OWNER_INDEX} missing"))?;
        let owner_position = SpoolIndex(OWNER_INDEX as u64);

        // The top tree's leaves are slice roots, so its path is built over those.
        let top_proof =
            create_proof_from_leaf_hashes::<TREE_HEIGHT>(&spool.encoding.leaves, OWNER_INDEX)
                .map_err(|error| anyhow!("top proof failed: {error:?}"))?;
        let registered_root = spool.encoding.leaves[OWNER_INDEX];

        // Reading A: the response is the slice root and its path, no slice bytes.
        // A free-rider that stored nothing but these 192 bytes still verifies.
        let slice_root_passes = spool
            .tree
            .verify_hash(OWNER_INDEX as u64, &top_proof, registered_root)
            .map_err(|error| anyhow!("verify_hash failed: {error:?}"))?;

        // Reading B: verification recomputes the root from the slice bytes, so
        // the response must carry the whole slice.
        let full_slice_verifies = slice_root(owner_slice) == Some(registered_root)
            && spool.encoding.verify_slice(owner_position, owner_slice);

        // Reading C: the sampled sub-leaf, which is what a round actually asks
        // for. The response carries bytes and stays one size as tracks grow.
        let proof = spool
            .encoding
            .prove_sub_leaf(owner_position, SAMPLED_SUB_LEAF, owner_slice)
            .ok_or_else(|| anyhow!("sub-leaf {SAMPLED_SUB_LEAF} not provable"))?;
        let sub_leaf_verifies =
            spool
                .encoding
                .verify_sub_leaf(owner_position, SAMPLED_SUB_LEAF, &proof);
        let sub_leaf_replays = spool.encoding.verify_sub_leaf(
            SpoolIndex((OWNER_INDEX + 1) as u64),
            SAMPLED_SUB_LEAF,
            &proof,
        );

        let hash_len = Hash::LEN;
        Ok(Self {
            leaf_count: LEAF_COUNT,
            tree_height: TREE_HEIGHT,
            slice_bytes: owner_slice.len(),
            sub_leaves_per_slice: sub_leaf_count(owner_slice.len()),
            sub_tree_height: SUB_TREE_HEIGHT,
            slice_root_bytes: hash_len + top_proof.len() * hash_len,
            slice_root_passes,
            full_slice_bytes: owner_slice.len() + top_proof.len() * hash_len,
            full_slice_verifies,
            sub_leaf_bytes: proof.sub_leaf.len() + proof.sub_proof.len() * hash_len,
            sub_leaf_verifies,
            sub_leaf_replays,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::SUB_LEAF_BYTES;

    // a top-level path verifies while carrying no slice bytes at all
    #[test]
    fn top_level_only() {
        // The finding that motivates the sub-leaf level: a path against the top
        // tree proves possession of a 32-byte root, not of the slice. An owner
        // that cached the root and path answers forever without storing bytes.
        let spool = Spool::build(1_000_000).unwrap();
        let report = CommitmentReport::measure(&spool).unwrap();
        assert!(report.slice_root_passes);
        assert!(report.full_slice_verifies);
        assert_eq!(report.slice_root_bytes, (TREE_HEIGHT + 1) * Hash::LEN);
    }

    // reading the top level with the bytes makes the response a whole slice
    #[test]
    fn whole_slice_reading() {
        // If the top level is read with the bytes, the "small proof" is a slice.
        let spool = Spool::build(4_000_000).unwrap();
        let report = CommitmentReport::measure(&spool).unwrap();
        assert!(report.full_slice_bytes > report.slice_root_bytes * 100);
    }

    // the sampled sub-leaf carries bytes and does not verify at a neighbour
    #[test]
    fn sampled_sub_leaf() {
        let spool = Spool::build(1_000_000).unwrap();
        let report = CommitmentReport::measure(&spool).unwrap();
        assert!(report.sub_leaf_verifies);
        // The path anchors at this slice's own root, so a neighbour's challenge
        // cannot be answered with it.
        assert!(!report.sub_leaf_replays);
        assert_eq!(
            report.sub_leaf_bytes,
            SUB_LEAF_BYTES + SUB_TREE_HEIGHT * Hash::LEN
        );
    }

    // the sampled response stays one size as the track grows
    #[test]
    fn bounded_response() {
        // The property the two-level commitment buys: the challenge response is
        // bounded, while a whole-slice response tracks the payload.
        let small = CommitmentReport::measure(&Spool::build(1_000_000).unwrap()).unwrap();
        let large = CommitmentReport::measure(&Spool::build(16_000_000).unwrap()).unwrap();
        assert!(large.slice_bytes > small.slice_bytes * 4);
        assert!(large.sub_leaves_per_slice > small.sub_leaves_per_slice * 4);
        assert_eq!(large.sub_leaf_bytes, small.sub_leaf_bytes);
    }
}

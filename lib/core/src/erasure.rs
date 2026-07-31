//! Erasure coding constants, and the commitment leaves derived from a slice.

use core::ops::Range;

use tape_crypto::Hash;
use tape_crypto::merkle::{create_proof_from_level, fold_level, hash_leaves, root_from_leaf_hashes};

use crate::types::{GroupIndex, SpoolIndex};

/// Number of slices per group (fixed network constant).
/// Individual encoding profiles may use n ≤ GROUP_SIZE.
pub const GROUP_SIZE: usize = 20;

/// Merkle tree height for slice commitments.
/// Derived from GROUP_SIZE: 2^5 = 32 >= 20 leaves.
pub const SLICE_TREE_HEIGHT: usize = 5;

/// Bytes covered by one sample leaf beneath a slice root.
/// Kept small because a challenge response carries one leaf and its path.
pub const SUB_LEAF_BYTES: usize = 1024;

/// Merkle tree height for the sub-leaf tree under a single slice.
/// 2^16 sample leaves of SUB_LEAF_BYTES is 64 MiB, which is where the max
/// track size came from. Reed-Solomon at k=1 puts a whole track in one slice,
/// so that is the case the height has to cover.
pub const SUB_TREE_HEIGHT: usize = 16;

/// Number of sample leaves a slice of this length is split into.
#[inline]
pub fn sub_leaf_count(slice_len: usize) -> usize {
    slice_len.div_ceil(SUB_LEAF_BYTES)
}

/// Hash every sample leaf of one coded slice, in order.
///
/// The leaves are independent, so the whole slice hashes in one multi-buffer
/// pass. A 64 MiB slice is 65,536 of them.
pub fn sub_leaf_hashes(slice: &[u8]) -> Vec<Hash> {
    hash_leaves(&slice.chunks(SUB_LEAF_BYTES).collect::<Vec<_>>())
}

/// Levels of the sub-leaf tree a challenge response rebuilds from slice bytes.
///
/// Everything above this comes from the sidecar an owner keeps beside the slice,
/// so answering reads one window rather than the whole slice. The paper's premise
/// is that reading retained data is fast; rebuilding a megabyte-scale tree per
/// response is not, and a deadline widened to cover it is a deadline a fetching
/// free-rider fits inside.
pub const SAMPLE_WINDOW_HEIGHT: usize = 8;

/// Sample leaves under one sidecar node.
pub const SAMPLE_WINDOW_LEAVES: usize = 1 << SAMPLE_WINDOW_HEIGHT;

/// Bytes of slice one window covers.
pub const SAMPLE_WINDOW_BYTES: usize = SAMPLE_WINDOW_LEAVES * SUB_LEAF_BYTES;

/// The nodes an owner keeps beside a slice so it can answer without rehashing it.
///
/// One entry per window of the slice, so the sidecar is `SAMPLE_WINDOW_LEAVES`
/// times smaller than the leaf hashes and a ten-thousandth of the slice itself.
/// None when the slice needs more leaves than the tree can hold.
pub fn slice_sidecar(slice: &[u8]) -> Option<Vec<Hash>> {
    if sub_leaf_count(slice.len()) > 1 << SUB_TREE_HEIGHT {
        return None;
    }

    Some(fold_level(&sub_leaf_hashes(slice), 0, SAMPLE_WINDOW_HEIGHT))
}

/// Slice root folded up from a sidecar rather than rehashed from the bytes.
///
/// A writer that built the sidecar already paid for every leaf hash, so this is
/// what saves it hashing the slice a second time to verify the root it just
/// committed to.
pub fn slice_root_from_sidecar(sidecar: &[Hash]) -> Hash {
    if sidecar.is_empty() {
        return root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&[]);
    }

    fold_level(sidecar, SAMPLE_WINDOW_HEIGHT, SUB_TREE_HEIGHT - SAMPLE_WINDOW_HEIGHT)[0]
}

/// Byte range of the slice a sample leaf's proof is built from.
pub fn sample_window(sub_leaf: usize, slice_len: usize) -> Range<usize> {
    let start = (sub_leaf / SAMPLE_WINDOW_LEAVES) * SAMPLE_WINDOW_BYTES;
    start..(start + SAMPLE_WINDOW_BYTES).min(slice_len)
}

/// Path from a sample leaf to its slice root, built from one window and the sidecar.
///
/// The lower `SAMPLE_WINDOW_HEIGHT` siblings come from rehashing the window the
/// leaf sits in; the rest come from the sidecar. Identical to the path a full
/// rebuild produces, which `sidecar_path_matches_a_full_rebuild` pins.
pub fn prove_sub_leaf_windowed(
    sidecar: &[Hash],
    window: &[u8],
    sub_leaf: usize,
) -> Option<Vec<Hash>> {
    let within = sub_leaf % SAMPLE_WINDOW_LEAVES;
    let node = sub_leaf / SAMPLE_WINDOW_LEAVES;

    let mut path =
        create_proof_from_level(&sub_leaf_hashes(window), within, 0, SAMPLE_WINDOW_HEIGHT).ok()?;
    path.extend(
        create_proof_from_level(
            sidecar,
            node,
            SAMPLE_WINDOW_HEIGHT,
            SUB_TREE_HEIGHT - SAMPLE_WINDOW_HEIGHT,
        )
        .ok()?,
    );

    Some(path)
}

/// Merkle root over the sample leaves of one coded slice.
/// None when the slice needs more leaves than the tree can hold.
pub fn slice_root(slice: &[u8]) -> Option<Hash> {
    // Reject on length first, so an oversized slice costs nothing to refuse.
    if sub_leaf_count(slice.len()) > 1 << SUB_TREE_HEIGHT {
        return None;
    }

    Some(root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&sub_leaf_hashes(slice)))
}

/// The slice position a spool holds within its group.
///
/// A spool index is network-wide while a commitment leaf index runs 0..GROUP_SIZE,
/// so anything indexing `leaves` has to convert first. Total, because every spool
/// index belongs to the group it divides into.
#[inline]
pub fn leaf_position(spool: SpoolIndex) -> SpoolIndex {
    SpoolIndex(spool.as_u64() % GROUP_SIZE as u64)
}

/// Get the group index for a given spool.
#[inline]
pub fn group_for_spool(spool: SpoolIndex) -> GroupIndex {
    GroupIndex::containing(spool)
}

/// Get the first spool index in a group.
#[inline]
pub fn group_start(group: GroupIndex) -> SpoolIndex {
    group.base_spool()
}

/// Get the global spool index for a slice within a group.
#[inline]
pub fn spool_for_slice(group: GroupIndex, slice_in_group: usize) -> SpoolIndex {
    group.spool_at(slice_in_group)
}

/// Get the position within a group for a spool, if the spool belongs to the group.
#[inline]
pub fn slice_for_spool(group: GroupIndex, spool: SpoolIndex) -> Option<usize> {
    group.position_of(spool)
}

/// Check if a spool belongs to a given group.
#[inline]
pub fn spool_in_group(spool: SpoolIndex, group: GroupIndex) -> bool {
    group.contains(spool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spool_group_size() {
        assert_eq!(GROUP_SIZE, 20);
    }

    #[test]
    fn a_leaf_position_wraps_within_its_group() {
        // The distinction that matters: a spool index runs network-wide while a
        // leaf index runs 0..GROUP_SIZE, so anything past the first group has to
        // wrap or it indexes off the end of the commitment.
        assert_eq!(leaf_position(SpoolIndex(0)), SpoolIndex(0));
        assert_eq!(leaf_position(SpoolIndex(19)), SpoolIndex(19));
        assert_eq!(leaf_position(SpoolIndex(20)), SpoolIndex(0));
        assert_eq!(leaf_position(SpoolIndex(137)), SpoolIndex(17));

        // It agrees with the group-derived form every other caller uses, and it
        // is total where that one is not.
        for spool in [0u64, 1, 20, 137, 999] {
            let spool = SpoolIndex(spool);
            let position = leaf_position(spool);
            assert!(position.as_usize() < GROUP_SIZE);
            assert_eq!(group_for_spool(spool).position_of(spool), Some(position.as_usize()));
        }
    }

    /// A slice whose leaves all differ, so a wrong path cannot pass by symmetry.
    fn slice_of(leaves: usize, tail: usize) -> Vec<u8> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..leaves * SUB_LEAF_BYTES + tail)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn a_sidecar_path_matches_a_full_rebuild() {
        // The whole point of the sidecar: serving from one window has to produce
        // byte-identical paths to hashing the entire slice, or an owner that took
        // the cheap path would fail every challenge.
        for (leaves, tail) in [(1usize, 0usize), (1, 5), (255, 0), (256, 0), (257, 0), (900, 33)] {
            let slice = slice_of(leaves, tail);
            let sidecar = slice_sidecar(&slice).expect("within capacity");
            let hashes = sub_leaf_hashes(&slice);
            let total = sub_leaf_count(slice.len());

            for sub_leaf in [0, total / 3, total.saturating_sub(1)] {
                let full = tape_crypto::merkle::create_proof_from_leaf_hashes::<SUB_TREE_HEIGHT>(
                    &hashes, sub_leaf,
                )
                .expect("full proof");
                let windowed = prove_sub_leaf_windowed(
                    &sidecar,
                    &slice[sample_window(sub_leaf, slice.len())],
                    sub_leaf,
                )
                .expect("windowed proof");

                assert_eq!(windowed, full, "leaves {leaves} tail {tail} leaf {sub_leaf}");
            }
        }
    }

    #[test]
    fn a_root_folds_out_of_the_sidecar() {
        // A writer builds the sidecar and needs the root to check what it is
        // about to store. Folding has to agree with hashing the slice, or the
        // write path would have to do both.
        for (leaves, tail) in [(0usize, 0usize), (0, 9), (1, 0), (255, 0), (256, 0), (900, 33)] {
            let slice = slice_of(leaves, tail);
            let sidecar = slice_sidecar(&slice).expect("within capacity");
            assert_eq!(
                slice_root_from_sidecar(&sidecar),
                slice_root(&slice).expect("within capacity"),
                "leaves {leaves} tail {tail}"
            );
        }
    }

    #[test]
    fn a_sidecar_is_a_rounding_of_the_slice() {
        // One node per window, so the sidecar is orders of magnitude smaller than
        // both the slice and its leaf hashes. This is what makes keeping it free.
        let slice = slice_of(900, 0);
        let sidecar = slice_sidecar(&slice).expect("within capacity");

        assert_eq!(sidecar.len(), 900usize.div_ceil(SAMPLE_WINDOW_LEAVES));
        assert!(sidecar.len() * Hash::LEN * 1_000 < slice.len());
    }

    #[test]
    fn a_window_covers_its_leaf_and_stops_at_the_slice() {
        let slice_len = 900 * SUB_LEAF_BYTES + 7;
        for sub_leaf in [0usize, 255, 256, 899] {
            let window = sample_window(sub_leaf, slice_len);
            let leaf_start = sub_leaf * SUB_LEAF_BYTES;

            assert!(window.contains(&leaf_start));
            assert!(window.end <= slice_len);
            assert_eq!(window.start % SAMPLE_WINDOW_BYTES, 0);
        }
    }

    #[test]
    fn test_sub_leaf_count_rounds_up() {
        assert_eq!(sub_leaf_count(0), 0);
        assert_eq!(sub_leaf_count(1), 1);
        assert_eq!(sub_leaf_count(SUB_LEAF_BYTES), 1);
        assert_eq!(sub_leaf_count(SUB_LEAF_BYTES + 1), 2);
    }

    #[test]
    fn test_slice_root_matches_leaf_hashes() {
        let slice: Vec<u8> = (0..SUB_LEAF_BYTES * 2 + 7).map(|b| b as u8).collect();
        let hashes = sub_leaf_hashes(&slice);

        assert_eq!(hashes.len(), sub_leaf_count(slice.len()));
        assert_eq!(
            slice_root(&slice),
            Some(tape_crypto::merkle::root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&hashes))
        );
    }

    #[test]
    fn test_slice_root_bounded_by_tree_capacity() {
        let capacity = SUB_LEAF_BYTES << SUB_TREE_HEIGHT;

        assert!(slice_root(&vec![0u8; capacity]).is_some());
        assert!(slice_root(&vec![0u8; capacity + 1]).is_none());
    }

    /// The tree must hold a slice that is a whole 64 MiB track, which is what
    /// Reed-Solomon at k=1 produces. This is exact, so a larger track size or
    /// any padding on the RS path needs the height raised with it.
    #[test]
    fn test_capacity_is_one_whole_track() {
        const MAX_TRACK_BYTES: usize = 64 * 1024 * 1024;

        assert_eq!(SUB_LEAF_BYTES << SUB_TREE_HEIGHT, MAX_TRACK_BYTES);
    }

    #[test]
    fn test_group_for_spool() {
        assert_eq!(group_for_spool(SpoolIndex(0)), GroupIndex(0));
        assert_eq!(group_for_spool(SpoolIndex(19)), GroupIndex(0));
        assert_eq!(group_for_spool(SpoolIndex(20)), GroupIndex(1));
        assert_eq!(group_for_spool(SpoolIndex(999)), GroupIndex(49));
    }

    #[test]
    fn test_group_start() {
        assert_eq!(group_start(GroupIndex(0)), SpoolIndex(0));
        assert_eq!(group_start(GroupIndex(1)), SpoolIndex(20));
        assert_eq!(group_start(GroupIndex(49)), SpoolIndex(980));
    }

    #[test]
    fn test_spool_for_slice() {
        assert_eq!(spool_for_slice(GroupIndex(0), 0), SpoolIndex(0));
        assert_eq!(spool_for_slice(GroupIndex(0), 19), SpoolIndex(19));
        assert_eq!(spool_for_slice(GroupIndex(1), 0), SpoolIndex(20));
        assert_eq!(spool_for_slice(GroupIndex(49), 19), SpoolIndex(999));
    }

    #[test]
    fn test_spool_in_group() {
        assert!(spool_in_group(SpoolIndex(0), GroupIndex(0)));
        assert!(spool_in_group(SpoolIndex(19), GroupIndex(0)));
        assert!(!spool_in_group(SpoolIndex(20), GroupIndex(0)));
        assert!(spool_in_group(SpoolIndex(20), GroupIndex(1)));
        assert!(spool_in_group(SpoolIndex(999), GroupIndex(49)));
    }

    #[test]
    fn test_slice_for_spool() {
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(0)), Some(0));
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(19)), Some(19));
        assert_eq!(slice_for_spool(GroupIndex(1), SpoolIndex(20)), Some(0));
        assert_eq!(slice_for_spool(GroupIndex(1), SpoolIndex(39)), Some(19));
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(20)), None);
    }
}

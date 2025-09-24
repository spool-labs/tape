use crate::hash::Hash;
use bytemuck::{Pod, Zeroable};
use super::{MerkleTree, Leaf, get_merkle_proof, utils};

/// Generates a Merkle proof using a cached layer and a closure to fetch only the required leaves
/// by index.
pub fn get_canopy_merkle_proof<const TREE_HEIGHT: usize>(
    tree: &MerkleTree<TREE_HEIGHT>,
    leaf_index: usize,
    cached_layer_number: usize,
    cached_layer_nodes: &[Hash],
    fetch_leaf: impl Fn(usize) -> Option<Leaf>,
) -> Vec<Hash> {

    assert!(cached_layer_number <= TREE_HEIGHT, "cached_layer_number exceeds tree height");
    assert!(leaf_index < (1usize << TREE_HEIGHT), "leaf_index out of capacity");
    assert!(!cached_layer_nodes.is_empty(), "cached_layer_nodes must not be empty");

    let subtree = Subtree::new(leaf_index, cached_layer_number, TREE_HEIGHT);

    let zero = tree.zero_values[0].as_leaf();
    let start = subtree.leaf_start;
    let end = subtree.leaf_start + subtree.leaf_count;

    // Should be way smaller than the total leaf count unless the root layer is chosen.
    let lower_leaves: Vec<Leaf> = (start..end)
        .map(|i| fetch_leaf(i).unwrap_or(zero))
        .collect();

    let target_relative = leaf_index - subtree.leaf_start;

    subtree.get_merkle_proof(
        tree,
        target_relative,
        &lower_leaves,
        cached_layer_nodes,
    )
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, Pod, Zeroable)]
pub struct Subtree {
    pub subtree_node_index: usize,
    pub pos_in_layer: usize,
    pub leaf_start: usize,
    pub leaf_count: usize,
    pub lower_height: usize,
    pub upper_height: usize,
}

impl Subtree {
    pub fn new(
        leaf_index: usize,
        layer_number: usize,
        height: usize,
    ) -> Self {
        compute_subtree_metadata(leaf_index, layer_number, height)
    }

    pub fn get_merkle_proof<const TREE_HEIGHT: usize>(
        &self,
        tree: &MerkleTree<TREE_HEIGHT>,
        target_leaf_relative: usize,
        lower_leaves: &[Leaf],
        cached_layer: &[Hash],
    ) -> Vec<Hash> {
        get_proof_with_metadata(
            self,
            tree,
            target_leaf_relative,
            lower_leaves,
            cached_layer,
        )
    }
}

/// Creates metadata for generating a split Merkle proof.
fn compute_subtree_metadata(
    leaf_index: usize,
    layer_number: usize,
    height: usize,
) -> Subtree {
    assert!(layer_number <= height, "layer_number > height");
    assert!(leaf_index < (1usize << height), "leaf_index out of capacity");

    // Ancestor at the split layer
    let subtree_node_index =
        utils::find_ancestor(layer_number, leaf_index, height);

    let pos_in_layer = subtree_node_index - 
        utils::first_index_in_layer(layer_number, height);

    // Range of leaves covered by that ancestor
    let (leaf_start, leaf_count) =
        utils::descendant_range(subtree_node_index, 0, height);

    Subtree {
        subtree_node_index,
        pos_in_layer,
        leaf_start,
        leaf_count,
        lower_height: layer_number,
        upper_height: height - layer_number,
    }
}

/// Generates a split Merkle proof for a specific layer into the Merkle tree using split metadata.
fn get_proof_with_metadata<const TREE_HEIGHT: usize>(
    meta: &Subtree,
    tree: &MerkleTree<TREE_HEIGHT>,
    target_leaf_relative: usize,
    lower_leaves: &[Leaf],
    cached_layer: &[Hash],
) -> Vec<Hash> {
    assert_eq!(
        lower_leaves.len(),
        meta.leaf_count,
        "lower_leaves length mismatch"
    );
    assert!(
        meta.pos_in_layer < cached_layer.len(),
        "cached layer missing target node"
    );

    let lower_proof = get_merkle_proof(
        lower_leaves,
        &tree.zero_values,
        target_leaf_relative,
        meta.lower_height,
    );

    let upper_leaves_as_leaf: Vec<Leaf> =
        cached_layer.iter().map(|h| h.as_leaf()).collect();

    let upper_proof = get_merkle_proof(
        &upper_leaves_as_leaf,
        &tree.zero_values[meta.lower_height..],
        meta.pos_in_layer,
        meta.upper_height,
    );

    let mut proof = Vec::with_capacity(TREE_HEIGHT);
    proof.extend(lower_proof);
    proof.extend(upper_proof);

    debug_assert_eq!(proof.len(), TREE_HEIGHT);

    proof
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{verify, MerkleTree};

    #[test]
    fn test_split_properties() {
        const TREE_HEIGHT: usize = 3;    // tree height (root at layer 3)

        let layer_number = 2;  // split layer
        let leaf_index   = 5;  // any leaf < 8

        let meta = compute_subtree_metadata(leaf_index, layer_number, TREE_HEIGHT);

        assert_eq!(meta.lower_height + meta.upper_height, TREE_HEIGHT);
        assert_eq!(meta.leaf_count, 1usize << layer_number);
        assert!(
            leaf_index >= meta.leaf_start &&
            leaf_index <  meta.leaf_start + meta.leaf_count
        );
        assert_eq!(
            meta.subtree_node_index,
            utils::first_index_in_layer(layer_number, TREE_HEIGHT) + meta.pos_in_layer
        );
    }

    #[test]
    fn test_split_baseline() {
        const TREE_HEIGHT: usize = 5;
        const FILLED: usize      = 4; // fill 4 leaves (out of 32)

        let layer_number = 2;
        let leaf_index   = 4usize;

        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = MerkleTree::<TREE_HEIGHT>::new(seeds);

        for i in 0..=FILLED {
            tree.try_add(&[format!("val_{i}").as_bytes()]).unwrap();
        }

        let mut leaves_by_index = std::collections::HashMap::new();
        for i in 0..=FILLED {
            leaves_by_index.insert(i, Leaf::new(&[format!("val_{i}").as_bytes()]));
        }
        let fetch = |idx: usize| leaves_by_index.get(&idx).copied();
        let zero  = tree.zero_values[0].as_leaf();

        let leaves: Vec<Leaf> = (0..=FILLED).map(|i| fetch(i).unwrap()).collect();
        let cached_layer = tree.get_layer_nodes(&leaves, layer_number);

        println!("cached_layer len: {:?}", cached_layer.len());

        let meta = compute_subtree_metadata(leaf_index, layer_number, TREE_HEIGHT);
        let lower: Vec<Leaf> = (meta.leaf_start .. meta.leaf_start + meta.leaf_count)
            .map(|g| fetch(g).unwrap_or(zero))
            .collect();

        let proof_split = get_proof_with_metadata(
            &meta,
            &tree,
            leaf_index - meta.leaf_start, // correct relative index
            &lower,
            &cached_layer,
        );

        let baseline = get_merkle_proof(&leaves, &tree.zero_values, leaf_index, TREE_HEIGHT);

        assert_eq!(proof_split, baseline);
        assert!(verify(tree.get_root(), &proof_split, leaves_by_index[&leaf_index]));
    }

    #[test]
    fn test_split_large_tree() {
        const TREE_HEIGHT: usize = 18; // 262 144-leaf capacity
        const FILLED: usize = 1 << 12; // fill 4096 leaves

        let layer_number = 10;
        let seeds: &[&[u8]] = &[b"large_tree"];
        let mut tree = MerkleTree::<TREE_HEIGHT>::new(seeds);
        let mut leaves_by_index = std::collections::HashMap::new();

        for i in 0..FILLED {
            let bytes = (i as u64).to_le_bytes();
            let leaf  = Leaf::new(&[&bytes]);
            leaves_by_index.insert(i, leaf);
            tree.try_add_leaf(leaf).unwrap();
        }

        let fetch = |idx: usize| leaves_by_index.get(&idx).copied();
        let zero  = tree.zero_values[0].as_leaf();

        let leaves: Vec<Leaf> = (0..FILLED).map(|i| fetch(i).unwrap()).collect();
        let cached_layer = tree.get_layer_nodes(&leaves, layer_number);

        // only 4 nodes in layer 10 are actually non-zero; a fully filled tree would have 256
        assert_eq!(cached_layer.len(), 4); 

        let leaf_index = 1234usize;
        let proof_split = get_canopy_merkle_proof(
            &tree,
            leaf_index,
            layer_number,
            &cached_layer,
            |i| fetch(i).or_else(|| Some(zero)),
        );

        let baseline = get_merkle_proof(&leaves, &tree.zero_values, leaf_index, TREE_HEIGHT);

        assert_eq!(proof_split, baseline);
        assert!(verify(tree.get_root(), &proof_split, fetch(leaf_index).unwrap()));
    }
}

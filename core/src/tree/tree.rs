use steel::*;
use super::Leaf;
use crate::hash::{hashv, Hash};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TreeError {
    #[error("Invalid Argument")]
    InvalidArgument,
    #[error("Tree is full")]
    TreeFull,
    #[error("Invalid Proof")]
    InvalidProof,
    #[error("Invalid Proof Length")]
    ProofLength,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct MerkleTree<const N: usize> {
    pub root: Hash,
    pub filled_subtrees: [Hash; N],
    pub zero_values: [Hash; N],
    pub next_index: u64,
}

unsafe impl<const N: usize> Zeroable for MerkleTree<N> {}
unsafe impl<const N: usize> Pod for MerkleTree<N> {}

impl<const N: usize> MerkleTree<N> {

    pub fn new(seeds: &[&[u8]]) -> Self {
        let zeros = Self::calc_zeros(seeds);
        Self {
            next_index: 0,
            root: zeros[N - 1],
            filled_subtrees: zeros,
            zero_values: zeros,
        }
    }

    pub fn from_zeros(zeros: [Hash; N]) -> Self {
        Self {
            next_index: 0,
            root: zeros[N - 1],
            filled_subtrees: zeros,
            zero_values: zeros,
        }
    }

    pub const fn get_depth(&self) -> u8 {
        N as u8
    }

    pub const fn get_size() -> usize {
        core::mem::size_of::<Self>()
    }

    pub fn get_root(&self) -> Hash {
        self.root
    }

    pub fn get_empty_leaf(&self) -> Leaf {
        self.zero_values[0].as_leaf()
    }

    pub fn init(&mut self, seeds: &[&[u8]]) {
        let zeros = Self::calc_zeros(seeds);
        self.next_index = 0;
        self.root = zeros[N - 1];
        self.filled_subtrees = zeros;
        self.zero_values = zeros;
    }

    /// Returns the number of leaves currently in the Merkle tree.
    pub fn get_leaf_count(&self) -> u64 {
        self.next_index
    }

    /// Returns the maximum capacity of the Merkle tree.
    pub fn get_capacity(&self) -> u64 {
        1u64 << N
    }

    /// Calculates the zero values for the Merkle tree based on the provided seeds.
    fn calc_zeros(seeds: &[&[u8]]) -> [Hash; N] {
        let mut zeros: [Hash; N] = [Hash::default(); N];
        let mut current = hashv(seeds);

        for i in 0..N {
            zeros[i] = current;
            current = hashv(&[b"NODE".as_ref(), current.as_ref(), current.as_ref()]);
        }

        zeros
    }

    /// Adds a data to the tree, creating a new leaf.
    pub fn try_add(&mut self, data: &[&[u8]]) -> Result<(), TreeError> {
        let leaf = Leaf::new(data);
        self.try_add_leaf(leaf)
    }

    /// Adds a leaf to the tree.
    pub fn try_add_leaf(&mut self, leaf: Leaf) -> Result<(), TreeError> {
        if self.next_index >= (1u64 << N) {
            return Err(TreeError::TreeFull.into());
        }

        let mut current_index = self.next_index;
        let mut current_hash = Hash::from(leaf);
        let mut left;
        let mut right;

        for i in 0..N {
            if current_index % 2 == 0 {
                left = current_hash;
                right = self.zero_values[i];
                self.filled_subtrees[i] = current_hash;
            } else {
                left = self.filled_subtrees[i];
                right = current_hash;
            }

            current_hash = hash_left_right(left, right);
            current_index /= 2;
        }

        self.root = current_hash;
        self.next_index += 1;

        Ok(())
    }

    /// Removes a leaf from the tree using the provided proof.
    pub fn try_remove<P>(&mut self, proof: &[P], data: &[&[u8]]) -> Result<(), TreeError>
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        let original_leaf = Leaf::new(data);
        self.try_remove_leaf(&proof_hashes, original_leaf)
    }

    /// Removes a leaf from the tree using the provided proof.
    pub fn try_remove_leaf<P>(&mut self, proof: &[P], leaf: Leaf) -> Result<(), TreeError>
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        self.check_length(&proof_hashes)?;
        self.try_replace_leaf(&proof_hashes, leaf, self.get_empty_leaf())
    }

    /// Replaces a leaf in the tree with new data using the provided proof.
    pub fn try_replace<P>(
        &mut self,
        proof: &[P],
        original_data: &[&[u8]],
        new_data: &[&[u8]],
    ) -> Result<(), TreeError>
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        let original_leaf = Leaf::new(original_data);
        let new_leaf = Leaf::new(new_data);
        self.try_replace_leaf(&proof_hashes, original_leaf, new_leaf)
    }

    /// Replaces a leaf in the tree with a new leaf using the provided proof.
    pub fn try_replace_leaf<P>(
        &mut self,
        proof: &[P],
        original_leaf: Leaf,
        new_leaf: Leaf,
    ) -> Result<(), TreeError>
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        self.check_length(&proof_hashes)?;
        let original_path = compute_path(&proof_hashes, original_leaf);
        let new_path = compute_path(&proof_hashes, new_leaf);

        if !is_valid_path(&original_path, self.root) {
            return Err(TreeError::InvalidProof);
        }

        for i in 0..N {
            if original_path[i] == self.filled_subtrees[i] {
                self.filled_subtrees[i] = new_path[i];
            }
        }
        self.root = *new_path.last().unwrap();
        Ok(())
    }

    /// Checks if the proof contains the specified data.
    pub fn contains<P>(&self, proof: &[P], data: &[&[u8]]) -> bool
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        let leaf = Leaf::new(data);
        self.contains_leaf(&proof_hashes, leaf)
    }

    /// Checks if the proof contains the specified leaf.
    pub fn contains_leaf<P>(&self, proof: &[P], leaf: Leaf) -> bool
    where
        P: Into<Hash> + Copy,
    {
        let proof_hashes: Vec<Hash> = proof.iter().map(|p| (*p).into()).collect();
        if self.check_length(&proof_hashes).is_err() {
            return false;
        }
        is_valid_leaf(&proof_hashes, self.root, leaf)
    }

    /// Checks if the proof length matches the expected depth of the tree.
    fn check_length(&self, proof: &[Hash]) -> Result<(), TreeError> {
        if proof.len() == N {
            Ok(())
        } else {
            Err(TreeError::ProofLength.into())
        }
    }

    /// Returns a Merkle proof for a specific leaf in the tree.
    pub fn get_proof(&self, leaves: &[Leaf], leaf_index: usize) -> Vec<Hash> {
        get_merkle_proof(leaves, &self.zero_values, leaf_index, N)
    }

    /// Hashes up to `layer_number` and returns only the non-empty nodes
    /// on that layer.
    pub fn get_layer_nodes(&self, leaves: &[Leaf], layer_number: usize) -> Vec<Hash> {
        if layer_number > N {
            return vec![];
        }

        let valid_leaves = leaves
            .iter()
            .take(self.next_index as usize)
            .copied()
            .collect::<Vec<Leaf>>();

        let mut current_layer: Vec<Hash> =
            valid_leaves.iter().map(|leaf| Hash::from(*leaf)).collect();

        if current_layer.is_empty() || layer_number == 0 {
            return current_layer;
        }

        let mut current_level: usize = 0;
        loop {
            if current_layer.is_empty() {
                break;
            }
            let mut next_layer = Vec::with_capacity(current_layer.len().div_ceil(2));
            let mut i = 0;
            while i < current_layer.len() {
                if i + 1 < current_layer.len() {
                    let val = hash_left_right(current_layer[i], current_layer[i + 1]);
                    next_layer.push(val);
                    i += 2;
                } else {
                    let val = hash_left_right(current_layer[i], self.zero_values[current_level]);
                    next_layer.push(val);
                    i += 1;
                }
            }
            current_level += 1;
            if current_level == layer_number {
                return next_layer;
            }
            current_layer = next_layer;
        }
        vec![]
    }
}

/// Returns a Merkle proof for a specific leaf in the tree.
pub fn get_merkle_proof(
    leaves: &[Leaf],
    zero_values: &[Hash],
    leaf_index: usize,
    height: usize,
) -> Vec<Hash> {
    let mut layers = Vec::with_capacity(height);
    let mut current_layer: Vec<Hash> = leaves.iter().map(|leaf| Hash::from(*leaf)).collect();

    for i in 0..height {
        if current_layer.len() % 2 != 0 {
            current_layer.push(zero_values[i]);
        }

        layers.push(current_layer.clone());
        current_layer = hash_pairs(current_layer);
    }

    let mut proof = Vec::with_capacity(height);
    let mut current_index = leaf_index;
    let mut layer_index = 0;

    for _ in 0..height {
        let sibling = if current_index % 2 == 0 {
            layers[layer_index][current_index + 1]
        } else {
            layers[layer_index][current_index - 1]
        };

        proof.push(sibling);

        current_index /= 2;
        layer_index += 1;
    }

    proof
}

/// Hashes pairs of hashes together, returning a new vector of hashes.
pub fn hash_pairs(pairs: Vec<Hash>) -> Vec<Hash> {
    let mut res = Vec::with_capacity(pairs.len() / 2);

    for i in (0..pairs.len()).step_by(2) {
        let left = pairs[i];
        let right = pairs[i + 1];

        let hashed = hash_left_right(left, right);
        res.push(hashed);
    }

    res
}

/// Hashes two hashes together, ensuring a consistent order.
pub fn hash_left_right(left: Hash, right: Hash) -> Hash {
    let combined;
    if left.to_bytes() <= right.to_bytes() {
        combined = [b"NODE".as_ref(), left.as_ref(), right.as_ref()];
    } else {
        combined = [b"NODE".as_ref(), right.as_ref(), left.as_ref()];
    }

    hashv(&combined)
}

/// Computes the path from the leaf to the root using the provided proof.
pub fn compute_path(proof: &[Hash], leaf: Leaf) -> Vec<Hash> {
    let mut computed_path = Vec::with_capacity(proof.len() + 1);
    let mut computed_hash = Hash::from(leaf);

    computed_path.push(computed_hash);

    for proof_element in proof.iter() {
        computed_hash = hash_left_right(computed_hash, *proof_element);
        computed_path.push(computed_hash);
    }

    computed_path
}

fn is_valid_leaf(proof: &[Hash], root: Hash, leaf: Leaf) -> bool {
    let computed_path = compute_path(proof, leaf);
    is_valid_path(&computed_path, root)
}

fn is_valid_path(path: &[Hash], root: Hash) -> bool {
    if path.is_empty() {
        return false;
    }

    *path.last().unwrap() == root
}

/// Verifies that a given merkle root contains the leaf using the provided proof.
pub fn verify<Root, Item, L>(root: Root, proof: &[Item], leaf: L) -> bool
where
    Root: Into<Hash>,
    Item: Into<Hash> + Copy,
    L: Into<Leaf>,
{
    let root_h: Hash = root.into();
    let proof_hashes: Vec<Hash> = proof.iter().map(|&x| x.into()).collect();

    let leaf_h: Leaf = leaf.into();
    let path = compute_path(&proof_hashes, leaf_h);
    is_valid_path(&path, root_h)
}


#[cfg(test)]
mod tests {
    use super::*;

    type TestTree = MerkleTree<3>;

    #[test]
    fn test_create_tree() {
        let seeds: &[&[u8]] = &[b"test"];
        let tree = TestTree::new(seeds);

        assert_eq!(tree.get_depth(), 3);
        assert_eq!(tree.get_root(), tree.zero_values.last().unwrap().clone());
    }

    #[test]
    fn test_insert_and_remove() {
        let seeds: &[&[u8]] = &[b"test"];

        let mut tree = TestTree::new(seeds);
        let empty = *tree.zero_values.first().unwrap();
        let empty_leaf = empty.as_leaf();

        // Tree structure:
        //
        //              root
        //            /     \
        //         m           n
        //       /   \       /   \
        //      i     j     k     l
        //     / \   / \   / \   / \
        //    a  b  c  d  e  f  g  h

        let a = Hash::from(Leaf::new(&[b"val_1"]));
        let b = Hash::from(Leaf::new(&[b"val_2"]));
        let c = Hash::from(Leaf::new(&[b"val_3"]));

        let d = empty;
        let e = empty;
        let f = empty;
        let g = empty;
        let h = empty;

        let i = hash_left_right(a, b);
        let j: Hash = hash_left_right(c, d);
        let k: Hash = hash_left_right(e, f);
        let l: Hash = hash_left_right(g, h);
        let m: Hash = hash_left_right(i, j);
        let n: Hash = hash_left_right(k, l);
        let root = hash_left_right(m, n);

        assert!(tree.try_add(&[b"val_1"]).is_ok());
        assert!(tree.filled_subtrees[0].eq(&a));

        assert!(tree.try_add(&[b"val_2"]).is_ok());
        assert!(tree.filled_subtrees[0].eq(&a)); // Not a typo

        assert!(tree.try_add(&[b"val_3"]).is_ok());
        assert!(tree.filled_subtrees[0].eq(&c)); // Not a typo

        assert_eq!(tree.filled_subtrees[0], c);
        assert_eq!(tree.filled_subtrees[1], i);
        assert_eq!(tree.filled_subtrees[2], m);
        assert_eq!(root, tree.get_root());

        let val1_proof = vec![b, j, n];
        let val2_proof = vec![a, j, n];
        let val3_proof = vec![d, i, n];

        // Check filled leaves
        assert!(tree.contains(&val1_proof, &[b"val_1"]));
        assert!(tree.contains(&val2_proof, &[b"val_2"]));
        assert!(tree.contains(&val3_proof, &[b"val_3"]));

        // Check empty leaves
        assert!(tree.contains_leaf(&[c, i, n], empty_leaf));
        assert!(tree.contains_leaf(&[f, l, m], empty_leaf));
        assert!(tree.contains_leaf(&[e, l, m], empty_leaf));
        assert!(tree.contains_leaf(&[h, k, m], empty_leaf));
        assert!(tree.contains_leaf(&[g, k, m], empty_leaf));

        // Remove val2 from the tree
        assert!(tree.try_remove(&val2_proof, &[b"val_2"]).is_ok());

        // Update the expected tree structure
        let i = hash_left_right(a, empty);
        let m: Hash = hash_left_right(i, j);
        let root = hash_left_right(m, n);

        assert_eq!(root, tree.get_root());

        let val1_proof = vec![empty, j, n];
        let val3_proof = vec![d, i, n];

        assert!(tree.contains_leaf(&val1_proof, Leaf::new(&[b"val_1"])));
        assert!(tree.contains_leaf(&val2_proof, empty_leaf));
        assert!(tree.contains_leaf(&val3_proof, Leaf::new(&[b"val_3"])));

        // Check that val2 is no longer in the tree
        assert!(!tree.contains_leaf(&val2_proof, Leaf::new(&[b"val_2"])));

        // Insert val4 into the tree
        assert!(tree.try_add(&[b"val_4"]).is_ok());
        assert!(tree.filled_subtrees[0].eq(&c)); // Not a typo

        // Update the expected tree structure
        let d = Hash::from(Leaf::new(&[b"val_4"]));
        let j = hash_left_right(c, d);
        let m = hash_left_right(i, j);
        let root = hash_left_right(m, n);

        assert_eq!(root, tree.get_root());
    }

    #[test]
    fn test_proof() {
        let seeds: &[&[u8]] = &[b"test"];

        let mut tree = TestTree::new(seeds);

        let leaves = [
            Leaf::new(&[b"val_1"]),
            Leaf::new(&[b"val_2"]),
            Leaf::new(&[b"val_3"]),
        ];

        assert!(tree.try_add(&[b"val_1"]).is_ok());
        assert!(tree.try_add(&[b"val_2"]).is_ok());
        assert!(tree.try_add(&[b"val_3"]).is_ok());

        let val1_proof = tree.get_proof(&leaves, 0);
        let val2_proof = tree.get_proof(&leaves, 1);
        let val3_proof = tree.get_proof(&leaves, 2);

        assert!(tree.contains(&val1_proof, &[b"val_1"]));
        assert!(tree.contains(&val2_proof, &[b"val_2"]));
        assert!(tree.contains(&val3_proof, &[b"val_3"]));

        // Invalid Proof Length
        let invalid_proof_short = &val1_proof[..2]; // Shorter than depth
        let invalid_proof_long = [&val1_proof[..], &val1_proof[..]].concat(); // Longer than depth

        assert!(!tree.contains(invalid_proof_short, &[b"val_1"]));
        assert!(!tree.contains(&invalid_proof_long, &[b"val_1"]));

        // Empty Proof
        let empty_proof: Vec<Hash> = Vec::new();
        assert!(!tree.contains(&empty_proof, &[b"val_1"]));
    }

    #[test]
    fn test_init_and_reinit() {
        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = TestTree::new(seeds);

        // Store initial state
        let initial_root = tree.get_root();
        let initial_zeros = tree.zero_values;
        let initial_filled = tree.filled_subtrees;
        let initial_index = tree.next_index;

        // Add a leaf to modify the tree
        assert!(tree.try_add(&[b"val_1"]).is_ok());

        // Reinitialize
        tree.init(seeds);

        // Verify tree is reset to initial state
        assert_eq!(tree.get_root(), initial_root);
        assert_eq!(tree.zero_values, initial_zeros);
        assert_eq!(tree.filled_subtrees, initial_filled);
        assert_eq!(tree.next_index, initial_index);
    }

    #[test]
    fn test_tree_full() {
        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = TestTree::new(seeds);

        // Fill the tree (2^3 = 8 leaves)
        for i in 0u8..8 {
            assert!(tree.try_add(&[&[i]]).is_ok());
        }

        // Try to add one more leaf
        let result = tree.try_add(&[b"extra"]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), TreeError::TreeFull);
    }

    #[test]
    fn test_replace_leaf() {
        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = TestTree::new(seeds);

        // Add initial leaves
        assert!(tree.try_add(&[b"val_1"]).is_ok());
        assert!(tree.try_add(&[b"val_2"]).is_ok());

        // Get proof for val_1
        let leaves = [Leaf::new(&[b"val_1"]), Leaf::new(&[b"val_2"])];
        let proof = tree.get_proof(&leaves, 0);

        // Replace val_1 with new_val
        assert!(tree.try_replace(&proof, &[b"val_1"], &[b"new_val"]).is_ok());

        // Verify new leaf is present
        assert!(tree.contains(&proof, &[b"new_val"]));
        assert!(!tree.contains(&proof, &[b"val_1"]));

        // Verify val_2 is still present
        let proof_val2 = tree.get_proof(&[Leaf::new(&[b"new_val"]), leaves[1]], 1);
        assert!(tree.contains(&proof_val2, &[b"val_2"]));
    }

    #[test]
    fn test_verify() {
        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = TestTree::new(seeds);

        // Add initial leaves
        assert!(tree.try_add(&[b"val_1"]).is_ok());
        assert!(tree.try_add(&[b"val_2"]).is_ok());

        // Get proof for val_1
        let leaves = [Leaf::new(&[b"val_1"]), Leaf::new(&[b"val_2"])];
        let proof = tree.get_proof(&leaves, 0);

        // Verify proof (typed)
        assert!(verify(tree.get_root(), &proof, Leaf::new(&[b"val_1"])));

        let a: [u8; 32] = tree.get_root().to_bytes();
        let b: [[u8; 32]; 3] = [
            proof[0].to_bytes(),
            proof[1].to_bytes(),
            proof[2].to_bytes(),
        ];
        let c: [u8; 32] = Leaf::new(&[b"val_1"]).to_bytes();

        // Verify proof (generic)
        assert!(verify(a, &b, c));
    }

    #[test]
    fn test_get_layer_nodes() {
        let seeds: &[&[u8]] = &[b"test"];
        let mut tree = TestTree::new(seeds);
        let empty = tree.zero_values[0];

        // Define leaves
        let leaves = [
            Leaf::new(&[b"val_1"]),
            Leaf::new(&[b"val_2"]),
            Leaf::new(&[b"val_3"]),
            Leaf::new(&[b"val_4"]),
        ];

        // Test empty tree
        assert_eq!(tree.get_layer_nodes(&leaves, 0), vec![]);
        assert_eq!(tree.get_layer_nodes(&leaves, 1), vec![]);

        // Add 3 leaves
        assert!(tree.try_add(&[b"val_1"]).is_ok());
        assert!(tree.try_add(&[b"val_2"]).is_ok());
        assert!(tree.try_add(&[b"val_3"]).is_ok());

        // Expected tree structure:
        //       root
        //      /    \
        //     m      0
        //    / \    / \
        //   i   j  0   0
        //  / \ / \ / \/ \
        // a  b c d 0 0 0 0

        let a = Hash::from(leaves[0]);
        let b = Hash::from(leaves[1]);
        let c = Hash::from(leaves[2]);
        let d = empty;
        let i = hash_left_right(a, b);
        let j = hash_left_right(c, d);

        // Test layer 0 (leaf layer)
        let layer_0 = tree.get_layer_nodes(&leaves, 0);
        assert_eq!(layer_0, vec![a, b, c]);

        // Test layer 1
        let layer_1 = tree.get_layer_nodes(&leaves, 1);
        assert_eq!(layer_1, vec![i, j]);

        // Test layer 2
        let layer_2 = tree.get_layer_nodes(&leaves, 2);
        let m = hash_left_right(i, j);
        assert_eq!(layer_2, vec![m]);

        // Test layer 3 (root)
        let layer_3 = tree.get_layer_nodes(&leaves, 3);
        assert_eq!(layer_3, vec![tree.get_root()]);

        // Test invalid layer
        let layer_4 = tree.get_layer_nodes(&leaves, 4);
        assert_eq!(layer_4, vec![]);

        // Add one more leaf to fill a node pair
        assert!(tree.try_add(&[b"val_4"]).is_ok());
        let d = Hash::from(leaves[3]);
        let j = hash_left_right(c, d);

        // Test layer 0 with 4 leaves
        let layer_0 = tree.get_layer_nodes(&leaves, 0);
        assert_eq!(layer_0, vec![a, b, c, d]);

        // Test layer 1 with updated j
        let layer_1 = tree.get_layer_nodes(&leaves, 1);
        assert_eq!(layer_1, vec![i, j]);
    }
}

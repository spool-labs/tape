use crate::hash::{hashv, Hash};
use hex_literal::hex;
use bytemuck::{Pod, Zeroable};

// Maximum height of Merkle trees supported.
pub const MAX_MERKLE_TREE_HEIGHT: usize = 32;

// Labels to domain-separate leaf and inner node hashing.
const LEFT_LABEL:  &[u8] = b"LEFT";
const RIGHT_LABEL: &[u8] = b"RIGHT";
const LEAF_LABEL:  &[u8] = b"LEAF";

/// Pre-calculated empty nodes for heights 0..MAX_MERKLE_TREE_HEIGHT-1.
/// (See empty_roots() in tests below)
const EMPTY_ROOTS: [[u8; 32]; MAX_MERKLE_TREE_HEIGHT] = [
    hex!("6b1b9cbf7c90b58807fce04f9a575fea9d831620e1729c55de743f8326fdd258"),
    hex!("3e38eb68ebf4aebe156616ca9423cf29754fbd6ac53f6ddd95c4a31810e06bd5"),
    hex!("ed7e22e570f3f7f0238afc180d66feaccb04df2f5dfc454deb810236abc5dd99"),
    hex!("976bd601e017a9692b0c14b7cd585723d4f5e86a81dc39f53aacc0bddc116a86"),
    hex!("fefc68d5a6caa871b9bf749d4dabff3f417a11f9ad83ab89f750cbd89887a4ab"),
    hex!("fda26bf4f73d15d2e2cc132e47c23c515f0dabc6f1fe4fd9eb06ad6bc87ee086"),
    hex!("de603d263fd11c40e23c62069c46e2fcf1e89bfdbec90bb265fa9150f5cce0a9"),
    hex!("41b1951f7b8dd7c798498edd64b7cafa8e3c5c07c78a3f41d5df643f477e01a0"),
    hex!("7b5ce3269ded7037bd102b95601dc18f5c68a27cb913eea438d32b955a4d6250"),
    hex!("153d3e2481c219daa791968e0ee0e87aa58d1d5b83e0833c3bc0d50c26ce6b5d"),
    hex!("56d538d2081f455953a1b3bc884c55eff7614c9b5b792fc2f02c11198d7e6bf7"),
    hex!("ea33cb6e02bee727a0a600acaccb8a2858e5a8fbfada702ec175108aff1d5d99"),
    hex!("5844bf474ab44d48f9d11350c3c72366217f949d088cb7b11352544b15d5044c"),
    hex!("9335a6033bef9da028aa0f6ff82f872169b57805d4738189030fc7bdf83c5e9e"),
    hex!("c96ff1e4e8677569e528565a6a90bbd33972ded9585b711f8e0a57f7e2d1de38"),
    hex!("3af1a6ae02df7f15b5226b78b7da2ff94ff4ea62b7fc1cb8f56481bde60ca78e"),
    hex!("d30833269685e1a2d515399a51e253d726a2cf448d76c18c24a0f4483d3805e3"),
    hex!("2b8dbcd919b629f134552971077529977d4e399aa44baff7e80951876cd872ee"),
    hex!("52d64df325830774bb229e8b5d9e38415b19bd2504c30a0c45c5a67383e81964"),
    hex!("b3b955ed5e811de3effed1cb103c2259e8d2cef961de446c5b934d35ae2d7467"),
    hex!("e99883fb1a77b287d38d3a6e16e55049e32037e414a1473a645a49f6d874e603"),
    hex!("96968c71edbaf2819f4679238d7c950bfbe4726873e990f2331e303e50562c37"),
    hex!("a2e8d2b0d53ca3233331e47d172fe414aa947219e27f0051165e9ad4274ee8a5"),
    hex!("ade3dcfa7b2e63fb2c0f73c396a5ce06ad41644702e5a9e9a28432f97409ad80"),
    hex!("a13673f9a682068f0acc58a6584992b064ea4dd17334fbd6f7a37dc43355c72b"),
    hex!("152025d2341fc9e602ab7e39d460347fb0c90c4b825c20b274c49129a04e6c88"),
    hex!("1e8568d4abf810de4e097dab7b359b36625cd8422e44129a0e640e5a177f3bef"),
    hex!("a0d5ab29ac43cc87a37e4d14b66e401dc54c2e273f343a62e1fb08b1280ebc06"),
    hex!("8f09d2f02d52aa9ad43d83728d5517afac171899eb3b2c303debbb5d9d9bee52"),
    hex!("adee18eedb1b71f983cb44bd0ef4312d6ca8d36ff6eda39d0457b649debe273e"),
    hex!("3a5d9438b80ff4439c2d572ac85524cc9da0c93f7ee94f6d335c9df6c3e9640d"),
    hex!("80f9b85d3ded25f980daf5996d3d934f1637b078f9e4ba12d5c69a4d44a56277"),
];

/// Hash a leaf node for merkle tree construction.
/// Uses domain separation with "LEAF" prefix.
#[inline]
pub fn hash_leaf(data: &[u8]) -> Hash {
    hashv(&[LEAF_LABEL, data])
}

#[inline]
fn hash_pair(left: Hash, right: Hash) -> Hash {
    hashv(&[LEFT_LABEL, left.as_ref(), RIGHT_LABEL, right.as_ref()])
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct MerkleTree<const N: usize> {
    pub root: Hash,
    pub filled_subtrees: [Hash; N],
    pub next_index: u64, // number of leaves inserted so far
}

unsafe impl<const N: usize> Zeroable for MerkleTree<N> {}
unsafe impl<const N: usize> Pod for MerkleTree<N> {}

impl<const N: usize> Default for MerkleTree<N> {
    fn default() -> Self {
        assert!(N > 0 && N <= MAX_MERKLE_TREE_HEIGHT);

        let first: Hash = EMPTY_ROOTS[0].into();
        let mut filled: [Hash; N] = [first; N];
        for i in 1..N {
            filled[i] = EMPTY_ROOTS[i].into();
        }

        let root = EMPTY_ROOTS[N - 1].into();
        Self {
            root,
            filled_subtrees: filled,
            next_index: 0,
        }
    }
}

impl<const N: usize> MerkleTree<N> {
    #[inline]
    pub const fn capacity() -> u64 {
        1u64 << N
    }

    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn is_zeroed(&self) -> bool {
        let zero = Hash::default();
        self.next_index == 0
            && self.root == zero
            && self.filled_subtrees.iter().all(|hash| *hash == zero)
    }

    #[inline]
    pub fn ensure_initialized(&mut self) {
        if self.is_zeroed() {
            *self = Self::default();
        }
    }

    #[inline]
    pub fn root(&self) -> Hash {
        self.root
    }

    /// Insert a pre-hashed leaf into the tree.
    pub fn add_leaf_hash(&mut self, leaf_hash: Hash) -> Result<u64, MerkleError> {
        if self.next_index >= Self::capacity() {
            return Err(MerkleError::TreeFull);
        }

        let inserted_index = self.next_index;
        let mut cur = leaf_hash;
        let mut idx = inserted_index;

        for level in 0..N {
            if (idx & 1) == 0 {
                self.filled_subtrees[level] = cur;
                cur = hash_pair(cur, EMPTY_ROOTS[level].into());
            } else {
                cur = hash_pair(self.filled_subtrees[level], cur);
            }
            idx >>= 1;
        }

        self.root = cur;
        self.next_index += 1;
        Ok(inserted_index)
    }

    /// Adds a new leaf to the tree.
    pub fn add_leaf(&mut self, leaf: &[u8]) -> Result<u64, MerkleError> {
        self.add_leaf_hash(hash_leaf(leaf))
    }

    /// Replaces a leaf in the tree with a new leaf using the provided proof.
    pub fn update_leaf<ProofElement>(
        &mut self,
        index: u64,
        proof: &[ProofElement],
        old_leaf: &[u8],
        new_leaf: &[u8],
    ) -> Result<(), MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        self.update_leaf_hash(index, proof, hash_leaf(old_leaf), hash_leaf(new_leaf))
    }

    /// Replaces a pre-hashed leaf in the tree with a new pre-hashed leaf using the provided proof.
    pub fn update_leaf_hash<ProofElement>(
        &mut self,
        index: u64,
        proof: &[ProofElement],
        old_leaf_hash: Hash,
        new_leaf_hash: Hash,
    ) -> Result<(), MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        if index >= self.next_index {
            return Err(MerkleError::InvalidProof);
        }
        if proof.len() != N {
            return Err(MerkleError::InvalidProof);
        }

        let proof: Vec<Hash> = proof
            .iter()
            .map(|p| (*p).into())
            .collect();

        self.check_length(&proof)?;

        let original_path = compute_path(&proof, old_leaf_hash, index, N);
        let new_path = compute_path(&proof, new_leaf_hash, index, N);

        let original_root = original_path
            .last()
            .ok_or(MerkleError::InvalidProof)?;
        if *original_root != self.root {
            return Err(MerkleError::InvalidProof);
        }

        for i in 0..N {
            if original_path[i] == self.filled_subtrees[i] {
                self.filled_subtrees[i] = new_path[i];
            }
        }
        self.root = *new_path
            .last()
            .ok_or(MerkleError::InvalidProof)?;
        Ok(())
    }

    /// Removes a leaf by replacing it with an empty leaf.
    pub fn remove_leaf<ProofElement>(
        &mut self,
        index: u64,
        proof: &[ProofElement],
        old_leaf: &[u8],
    ) -> Result<(), MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        self.remove_leaf_hash(index, proof, hash_leaf(old_leaf))
    }

    /// Removes a pre-hashed leaf by replacing it with an empty leaf.
    pub fn remove_leaf_hash<ProofElement>(
        &mut self,
        index: u64,
        proof: &[ProofElement],
        old_leaf_hash: Hash,
    ) -> Result<(), MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        self.update_leaf_hash(index, proof, old_leaf_hash, hash_leaf(&[]))
    }

    /// Verifies that a leaf is contained in the tree using the provided proof.
    pub fn contains<ProofElement>(
        &self,
        index: u64,
        proof: &[ProofElement],
        leaf: &[u8],
    ) -> bool
    where
        ProofElement: Into<Hash> + Copy,
    {

        let proof: Vec<Hash> = proof
            .iter()
            .map(|p| (*p).into())
            .collect();

        if self.check_length(&proof).is_err() {
            return false;
        }

        verify_proof(leaf, &self.root, &proof, index, N)
    }

    /// Verifies that a leaf is contained in the tree using the provided proof.
    pub fn verify<ProofElement>(
        &self,
        index: u64,
        proof: &[ProofElement],
        leaf: &[u8],
    ) -> Result<bool, MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        self.verify_leaf_hash(index, proof, hash_leaf(leaf))
    }

    /// Verifies that a pre-hashed leaf is contained in the tree using the provided proof.
    pub fn verify_hash<ProofElement>(
        &self,
        index: u64,
        proof: &[ProofElement],
        leaf_hash: Hash,
    ) -> Result<bool, MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        self.verify_leaf_hash(index, proof, leaf_hash)
    }

    /// Verifies that a pre-hashed leaf is contained in the tree using the provided proof.
    fn verify_leaf_hash<ProofElement>(
        &self,
        index: u64,
        proof: &[ProofElement],
        leaf_hash: Hash,
    ) -> Result<bool, MerkleError>
    where
        ProofElement: Into<Hash> + Copy,
    {
        if index >= self.next_index {
            return Err(MerkleError::InvalidProof);
        }

        let proof: Vec<Hash> = proof
            .iter()
            .map(|p| (*p).into())
            .collect();

        self.check_length(&proof)?;

        let path = compute_path(&proof, leaf_hash, index, N);
        let root = *path.last().ok_or(MerkleError::InvalidProof)?;
        Ok(root == self.root)
    }

    /// Returns a Merkle proof for a specific leaf in the tree.
    pub fn create_proof<T: AsRef<[u8]>>(
        &self, leaves: &[T], index: usize
    ) -> Result<Vec<Hash>, MerkleError> {

        if index as u64 >= self.next_index {
            return Err(MerkleError::InvalidIndex);
        }

        create_merkle_proof(leaves, index, N)
    }

    fn check_length(&self, proof: &[Hash]) -> Result<(), MerkleError> {
        if proof.len() == N {
            Ok(())
        } else {
            Err(MerkleError::ProofLength.into())
        }
    }

}

/// Compute a Merkle root from pre-hashed leaf values.
pub fn root_from_leaf_hashes<const N: usize>(hashes: &[Hash]) -> Hash {
    let mut tree = MerkleTree::<N>::new();
    for h in hashes {
        tree.add_leaf_hash(*h).expect("tree capacity");
    }
    tree.root()
}

/// Create a Merkle proof from pre-hashed leaf values.
pub fn create_proof_from_leaf_hashes<const N: usize>(
    hashes: &[Hash],
    index: usize,
) -> Result<Vec<Hash>, MerkleError> {
    create_merkle_proof_hashes(hashes, index, N)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MerkleError {
    TreeFull,
    InvalidProof,
    InvalidIndex,
    ProofLength,
}

/// Computes the path from the leaf to the root using the provided proof.
pub fn compute_path(proof: &[Hash], leaf: Hash, index: u64, height: usize) -> Vec<Hash> {
    let mut computed_path = Vec::with_capacity(height + 1);
    let mut computed_hash = leaf;
    let mut idx = index;
    let mut proof_idx = 0;

    computed_path.push(computed_hash);

    for _ in 0..height {
        let sibling = proof[proof_idx];
        proof_idx += 1;

        if (idx & 1) == 0 {
            computed_hash = hash_pair(computed_hash, sibling);
        } else {
            computed_hash = hash_pair(sibling, computed_hash);
        }
        computed_path.push(computed_hash);
        idx >>= 1;
    }

    computed_path
}

pub fn create_merkle_proof<T: AsRef<[u8]>>(
    leaves: &[T],
    index: usize, 
    height: usize,
) -> Result<Vec<Hash>, MerkleError> {
    let hashes: Vec<Hash> = leaves
        .iter()
        .map(|leaf| hash_leaf(leaf.as_ref()))
        .collect();

    create_merkle_proof_hashes(&hashes, index, height)
}

fn create_merkle_proof_hashes(
    hashes: &[Hash],
    index: usize,
    height: usize,
) -> Result<Vec<Hash>, MerkleError> {
    if hashes.is_empty() {
        return Err(MerkleError::InvalidProof);
    }
    if index >= hashes.len() {
        return Err(MerkleError::InvalidProof);
    }
    if hashes.len() > (1usize << height) {
        return Err(MerkleError::InvalidProof);
    }
    if height > MAX_MERKLE_TREE_HEIGHT {
        return Err(MerkleError::InvalidProof);
    }

    let empty: Vec<Hash> = (0..height)
        .map(|i| EMPTY_ROOTS[i].into())
        .collect();

    let mut layers = Vec::with_capacity(height);
    let mut current_layer: Vec<Hash> = hashes.to_vec();

    for i in 0..height {
        if current_layer.len() % 2 != 0 {
            current_layer.push(empty[i]);
        }

        layers.push(current_layer.clone());
        current_layer = (0..current_layer.len() / 2)
            .map(|j| hash_pair(current_layer[2 * j], current_layer[2 * j + 1]))
            .collect();
    }

    let mut proof = Vec::with_capacity(height);
    let mut current_index = index;
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

    Ok(proof)
}

pub fn verify_proof(
    data: &[u8],
    root: &Hash,
    proof: &[Hash],
    index: u64,
    height: usize
) -> bool {
    if proof.len() != height {
        return false;
    }

    let mut node = hash_leaf(data);
    let mut idx = index;

    for &sibling in proof.iter() {
        if (idx & 1) == 0 {
            node = hash_pair(node, sibling);
        } else {
            node = hash_pair(sibling, node);
        }
        idx >>= 1;
    }
    node == *root
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        // Two leaves -> height 1 tree.
        let mut tree = MerkleTree::<1>::new();
        let a = b"hello";
        let b = b"world";
        tree.add_leaf(a).unwrap();
        tree.add_leaf(b).unwrap();
        assert_eq!(tree.next_index, 2);
    }

    #[test]
    fn two_leaves() {
        // Two leaves -> height 1
        let mut tree = MerkleTree::<1>::new();

        let data = [b"hello".to_vec(), b"world".to_vec()];
        tree.add_leaf(&data[0]).unwrap();
        tree.add_leaf(&data[1]).unwrap();

        // calculate expected root hash manually
        let leaf1 = hash_leaf(&data[0]);
        let leaf2 = hash_leaf(&data[1]);
        let expected_root = hash_pair(leaf1, leaf2);

        assert_eq!(tree.root, expected_root);
    }

    #[test]
    fn proofs() {
        // 4 leaves -> height 2
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];

        let mut tree = MerkleTree::<2>::new();
        for d in &data {
            tree.add_leaf(d).unwrap();
        }

        // proof and verify all leaves
        let proof = tree.create_proof(&data, 0).expect("valid proof");
        assert!(tree.verify(0, &proof, &data[0]).unwrap());
        
        let proof = tree.create_proof(&data, 1).expect("valid proof");
        assert!(tree.verify(1, &proof, &data[1]).unwrap());
        
        let proof = tree.create_proof(&data, 2).expect("valid proof");
        assert!(tree.verify(2, &proof, &data[2]).unwrap());
        
        let proof = tree.create_proof(&data, 3).expect("valid proof");
        assert!(tree.verify(3, &proof, &data[3]).unwrap());
    }

    #[test]
    fn three_leaves() {
        // Use height 2 (capacity 4)
        let data1 = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let mut t1 = MerkleTree::<2>::new();
        for d in &data1 {
            t1.add_leaf(d).unwrap();
        }

        let data2 = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), vec![]];
        let mut t2 = MerkleTree::<2>::new();
        for d in &data2 {
            t2.add_leaf(d).unwrap();
        }

        // missing leaves should be equivalent to empty leaves
        assert_eq!(t1.root, t2.root);
    }

    #[test]
    fn non_power_of_two() {
        // 33 leaves -> height 6 (capacity 64)
        let data1 = vec![b"hello".to_vec(); 33];
        let mut t1 = MerkleTree::<6>::new();
        for d in &data1 {
            t1.add_leaf(d).unwrap();
        }

        let mut data2 = vec![b"hello".to_vec(); 33];
        let empty = vec![];
        data2.extend_from_slice(vec![empty; 31].as_slice());

        let mut t2 = MerkleTree::<6>::new();
        for d in &data2 {
            t2.add_leaf(d).unwrap();
        }

        // missing leaves should be equivalent to empty leaves
        assert_eq!(t1.root, t2.root);
    }

    #[test]
    fn add_and_replace() {
        let mut tree = MerkleTree::<3>::new();
        let empty_value: &[u8] = &[];
        let empty: Hash = EMPTY_ROOTS[0].into();

        // Tree structure:
        //
        //              root
        //            /     \
        //         m           n
        //       /   \       /   \
        //      i     j     k     l
        //     / \   / \   / \   / \
        //    a  b  c  d  e  f  g  h

        let val1 = b"val1";
        let val2 = b"val2";
        let val3 = b"val3";

        let a = hash_leaf(val1);
        let b = hash_leaf(val2);
        let c = hash_leaf(val3);

        let d = empty;
        let e = empty;
        let f = empty;
        let g = empty;
        let h = empty;

        let i = hash_pair(a, b);
        let j = hash_pair(c, d);
        let k = hash_pair(e, f);
        let l = hash_pair(g, h);
        let m = hash_pair(i, j);
        let n = hash_pair(k, l);
        let root = hash_pair(m, n);

        tree.add_leaf(val1).unwrap();
        assert_eq!(tree.filled_subtrees[0], a);

        tree.add_leaf(val2).unwrap();
        assert_eq!(tree.filled_subtrees[0], a);

        tree.add_leaf(val3).unwrap();
        assert_eq!(tree.filled_subtrees[0], c);

        assert_eq!(tree.filled_subtrees[1], i);
        assert_eq!(tree.filled_subtrees[2], m);
        assert_eq!(tree.root, root);

        let leaf1_proof = vec![b, j, n];
        let leaf2_proof = vec![a, j, n];
        let leaf3_proof = vec![d, i, n];

        // Check filled leaves
        assert!(tree.contains(0, &leaf1_proof, val1));
        assert!(tree.contains(1, &leaf2_proof, val2));
        assert!(tree.contains(2, &leaf3_proof, val3));

        // Check empty leaves
        assert!(tree.contains(3, &[c, i, n], empty_value));
        assert!(tree.contains(4, &[f, l, m], empty_value));
        assert!(tree.contains(5, &[e, l, m], empty_value));
        assert!(tree.contains(6, &[g, k, m], empty_value));
        assert!(tree.contains(7, &[h, k, m], empty_value));

        // Replace leaf2 with empty (simulate remove)
        tree.update_leaf(1, &leaf2_proof, val2, empty_value).unwrap();

        // Update the expected tree structure
        let i_new = hash_pair(a, empty);
        let m_new = hash_pair(i_new, j);
        let root_new = hash_pair(m_new, n);

        assert_eq!(tree.root, root_new);

        let leaf1_proof_new = vec![empty, j, n];
        let leaf3_proof_new = vec![d, i_new, n];

        assert!(tree.contains(0, &leaf1_proof_new, val1));
        assert!(tree.contains(1, &leaf2_proof, empty_value));
        assert!(tree.contains(2, &leaf3_proof_new, val3));

        // Check that leaf2 is no longer in the tree
        assert!(!tree.contains(1, &leaf2_proof, val2));

        // Insert leaf4 into the tree
        let leaf4 = b"leaf4";
        tree.add_leaf(leaf4).unwrap();
        assert_eq!(tree.filled_subtrees[0], c);

        // Update the expected tree structure
        let d_new = hash_leaf(leaf4);
        let j_new = hash_pair(c, d_new);
        let m_new2 = hash_pair(i_new, j_new);
        let root_new2 = hash_pair(m_new2, n);

        assert_eq!(tree.root, root_new2);
    }

    #[test]
    fn root_from_leaf_hashes_matches_add_leaf() {
        let data: Vec<Vec<u8>> = (0..20u8).map(|i| vec![i; 100]).collect();

        // Build tree via add_leaf (hashes raw data)
        let mut tree = MerkleTree::<5>::new();
        for d in &data {
            tree.add_leaf(d).unwrap();
        }

        // Build root via root_from_leaf_hashes (pre-hashed)
        let hashes: Vec<Hash> = data.iter().map(|d| hash_leaf(d)).collect();
        let root = root_from_leaf_hashes::<5>(&hashes);

        assert_eq!(tree.root(), root);
    }

    #[test]
    fn create_proof_from_leaf_hashes_matches_create_proof() {
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];

        let raw_proof = create_merkle_proof(&data, 2, 2).expect("valid raw proof");
        let hashes: Vec<Hash> = data.iter().map(|leaf| hash_leaf(leaf)).collect();
        let hash_proof = create_proof_from_leaf_hashes::<2>(&hashes, 2)
            .expect("valid hashed proof");

        assert_eq!(raw_proof, hash_proof);
    }

    #[test]
    fn zeroed_tree_can_be_initialized() {
        let mut tree = MerkleTree::<3> {
            root: Hash::default(),
            filled_subtrees: [Hash::default(); 3],
            next_index: 0,
        };

        assert!(tree.is_zeroed());

        tree.ensure_initialized();

        assert_eq!(tree, MerkleTree::<3>::new());
        assert!(!tree.is_zeroed());
    }

    #[test]
    fn verify_hash_matches_verify() {
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];

        let mut tree = MerkleTree::<2>::new();
        for d in &data {
            tree.add_leaf(d).unwrap();
        }

        let index = 2;
        let proof = tree.create_proof(&data, index).expect("valid proof");
        let leaf_hash = hash_leaf(&data[index]);

        assert!(tree.verify(index as u64, &proof, &data[index]).unwrap());
        assert!(tree.verify_hash(index as u64, &proof, leaf_hash).unwrap());
        assert_eq!(
            tree.verify_hash(data.len() as u64, &proof, leaf_hash),
            Err(MerkleError::InvalidProof)
        );
    }

    #[test]
    fn update_leaf_hash_matches_update_leaf() {
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];

        let mut raw_tree = MerkleTree::<2>::new();
        let mut hash_tree = MerkleTree::<2>::new();
        for d in &data {
            raw_tree.add_leaf(d).unwrap();
            hash_tree.add_leaf(d).unwrap();
        }

        let index = 1;
        let proof = raw_tree.create_proof(&data, index).expect("valid proof");
        let new_leaf = b"updated";

        raw_tree
            .update_leaf(index as u64, &proof, &data[index], new_leaf)
            .unwrap();
        hash_tree
            .update_leaf_hash(
                index as u64,
                &proof,
                hash_leaf(&data[index]),
                hash_leaf(new_leaf),
            )
            .unwrap();

        assert_eq!(raw_tree, hash_tree);
    }

    #[test]
    fn remove_leaf_hash_matches_remove_leaf() {
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];

        let mut raw_tree = MerkleTree::<2>::new();
        let mut hash_tree = MerkleTree::<2>::new();
        for d in &data {
            raw_tree.add_leaf(d).unwrap();
            hash_tree.add_leaf(d).unwrap();
        }

        let index = 3;
        let proof = raw_tree.create_proof(&data, index).expect("valid proof");

        raw_tree
            .remove_leaf(index as u64, &proof, &data[index])
            .unwrap();
        hash_tree
            .remove_leaf_hash(index as u64, &proof, hash_leaf(&data[index]))
            .unwrap();

        assert_eq!(raw_tree, hash_tree);
    }

    // NOTE: This is used for calculating EMPTY_ROOTS.
    #[test]
    fn empty_roots() {
        for height in 0..MAX_MERKLE_TREE_HEIGHT {
            let mut node = hash_leaf(&vec![]);
            for _ in 0..height {
                node = hash_pair(node, node);
            }
            println!("{}", hex::encode(node));
        }
    }
}

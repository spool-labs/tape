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
    hex!("21b04fab319f57c9ab68d5f246a5b6a07145b960907bd327d2dc77534395be69"),
    hex!("a900d7c2ac7d5a234f22cf34f5a792b07b06f463987e7e3b45c332f6583132ee"),
    hex!("1d2f524e70468006e35a395f721c9da42378271f3dbac375004e12d024ce096d"),
    hex!("c92b1ca50c666b745e37d49a03bed9ced3d653197fd3b36835d2597107016f2c"),
    hex!("c6e1e93788659c5fe0fcbc01a3b737fd9c0a2d7a0e797987fca7e79281e9bffe"),
    hex!("7108b4fe3940d4f0e77db5a3c8310ef287110d4947cf72103cfdbaa557072c40"),
    hex!("f65084cccba231b3030ed281f72d3c6cebee038d1df9dd74e3d05d40f137c95f"),
    hex!("0566001551b8f4a535e3a311f2e6b40f9424aacec726430dedc4698b398290fa"),
    hex!("40213f2ce6083dc46479d55ca121f29cde11d15da01d6ae9821ad297fcc7ee3b"),
    hex!("3dcf33abc197956262348255c5938dda581f5c2e016943c189221aa96996f114"),
    hex!("4364881ffb66a9355f9b20b5d55be0d812ac49df6fccae14cebe740e4b944b94"),
    hex!("8c08ec3be1be18ab20287a25b3d0b1655b5f2236fc3cee9a29d522da454d9530"),
    hex!("4195a782d5712fe805a0b305448727f9de2d2beefb92f83ebbd0485ac9054c8d"),
    hex!("a4d4c409894e22fa550418c8249c3c76737091d77efa227ee8d35b3ac0b8d890"),
    hex!("c1422a37c9a17140e0e997a6a3edfb0f39b4868bc6b9a4dc05f0a8fed0572381"),
    hex!("a5f5e2e1adbe140fd0ff3b4c42f3a30a4944c3cf1914e01a84dc12059c15c40f"),
    hex!("1b48d8fe7b697943a56522a57963bc1f9a002a74cf709c6de67e311ed79c7bd5"),
    hex!("c3976bd5944fc27cd92d591a7a162e49b962f4782b8147def8423148ee381d38"),
    hex!("288a66c25032f21128f817ebf13e971df4a1687be695e5075a82d61e15536419"),
    hex!("c72beeb39f411c20dcbfcd383bb611968ed5d62caef5c8094a6e1aa033c52c03"),
    hex!("77628441434332c58847ed40034c8e2f208f8223e00401acfd69b8aea1fcc8f1"),
    hex!("5d4d277da2f7d6dd000511e7955eed565c083eab5b6e8b4d2b8788c7d5ba3c02"),
    hex!("bd33b935a0abe7d9ab8a2fe6c4429aa61276ff5a616d50c3314a49075db8890d"),
    hex!("54284b4352053c2839797db34cc533cf69fc8bb2523054997d55f30375a24c34"),
    hex!("3aeea4598221673576c5f3a8db85e68a8d75ecd44faaf7cd097f145b16245f2f"),
    hex!("aec5825bf02b024a265467bda1e228614af95b0847e75d14a827ba272629f30c"),
    hex!("f86d0f07cca3445cebad759b75caddabac756c75ea202519da8a20a81c2392aa"),
    hex!("af752b76921db4a93770fb803d368184a91c8835d7d7e6e3abc2d3c442fc624e"),
    hex!("591b87cb849410857f347100b212eac548baf563e9e1b9ff71cf5ee0ee9ed0fd"),
    hex!("01e1c3a782cef57bb54727c4587b54c742dc51cc507ee703ff699bfcd0a0a5d7"),
    hex!("47e8329e33ea4bd3164811f064bb564e2d97aca07e91417e8b332ddf1f855d2f"),
    hex!("c9b879ce59c64ee91f1f5a2a61e83249d1ce6eb892b35fc0f31762a65c9b6b6f"),
];

/// Hash a leaf node for merkle tree construction.
/// Uses domain separation with "LEAF" prefix.
#[inline]
pub fn hash_leaf(data: &[u8]) -> Hash {
    hashv(&[LEAF_LABEL, data])
}

/// Hash a pair of child nodes into their parent node.
/// Uses domain separation with "LEFT" and "RIGHT" prefixes.
#[inline]
pub fn hash_pair(left: Hash, right: Hash) -> Hash {
    hashv(&[LEFT_LABEL, left.as_ref(), RIGHT_LABEL, right.as_ref()])
}

/// Root of a fully-empty subtree of the given height
#[inline]
pub fn empty_subtree_root(height: usize) -> Hash {
    EMPTY_ROOTS[height].into()
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

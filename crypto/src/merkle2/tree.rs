use crate::hash::{hashv, Hash};
use hex_literal::hex;
use bytemuck::{Pod, Zeroable};

// Maximum height of Merkle trees supported.
pub const MAX_MERKLE_TREE_HEIGHT: usize = 32;

// Labels to domain-separate leaf and inner node hashing.
const LEAF_LABEL:  &[u8] = b"LEAF";
const NODE_LABEL: &[u8] = b"NODE";

/// Pre-calculated empty nodes for heights 0..MAX_MERKLE_TREE_HEIGHT-1.
/// (See empty_roots() in tests below)
const EMPTY_ROOTS: [[u8; 32]; MAX_MERKLE_TREE_HEIGHT] = [
    hex!("6b1b9cbf7c90b58807fce04f9a575fea9d831620e1729c55de743f8326fdd258"),
    hex!("9024593678931c724d02768bc5672254c131921249b58bf630147776d494da25"),
    hex!("cab3a3a5943643d495b47e874e8995ca252a85c49830d0510778ae3b37bdaf50"),
    hex!("832382563af68a993ee780ef25bdc0bf3ce43a668c4f9ec6cba9dc605e79cdbb"),
    hex!("26cd7e9718116f7c3e45124f49f9ef719442d13127788538d8ede42e23c413d4"),
    hex!("d01120d92b0d18791119eea1e88f44584582882df80859ab436696031d2c81a3"),
    hex!("f9b7c66ef9515cf63491eec558d7b74d74caa4530d6a2804d7f2f6fb65792d33"),
    hex!("a8b0f35ab9415c4ef96fe2d7fe684d52b886e79f288f5c8d7a2144fd80159208"),
    hex!("34dc212276a4f7818d1baa8a14c5962109651e505401f86bde5907eeedb22c32"),
    hex!("49c2ebf27f36475c04e37cb4bec70a0f0af13a7510540aa472f5448350a7b98f"),
    hex!("7caf9f649e7ea8798a0f4a81cc6824331156ebd7afb1337420707cd604a46451"),
    hex!("e7d1975bd0f96ebe86a2a8c560adf88ec5348fcace948291ddbc4210a2aed5ab"),
    hex!("92d90fb75b11b4ce3f9c63095f969d591f6f3f622c400e0bbd30f511894f7f21"),
    hex!("932a7956530a93462a6ceed6d92224a162a63e0f2b01236b02b711653a7e7d88"),
    hex!("c146556d226574a0fd3088c9f057089c8349c96f5ab367e6c138e307128e1bff"),
    hex!("ef90f19f16d8f04ab14407b9001afa3ce7f05d621b9abb552f1ae7b10a26dde9"),
    hex!("85dfc05eedba5e31e96b77c9579aef306c8a6ddf8f777ab083386632076d5f81"),
    hex!("aa20af6a0023628a26ca37d8692203054535b7a5e18aa3b44e7146f23aa14638"),
    hex!("dce86b4656c93f974fc55b0426dee4529d297ca2eb241edc2ae2bb13044a823d"),
    hex!("fb909eee27b4202b5581d71b6e6ddb8cbd3921aacd9ac3f505368a8a299bbd47"),
    hex!("97c73864a297cb773317f381af23ab15f939352e1fde3089497a967d6f9cf28f"),
    hex!("69d31bd46244867d3a989c7688f2c0956403960537138a076acd69277399d75f"),
    hex!("1153c86efc3ef30f628c767a67b07ea6950a7138b3f9a593253473a2c2471bfa"),
    hex!("ea2324da09fad6c1eb3df20ff5dd8563c992d001450b0eb533f90a8baa35a257"),
    hex!("0ca7b015769dd4bfaea4ee6dacfcae73fdf0ed7b15c32e9b7d26fac28aa09a3d"),
    hex!("94da8463a7465b7813c83cecb1e3c2ce261fe85a44477b537e4146a252f307d7"),
    hex!("93016d5c4711dc0589bf040390f5bc5db5d84ead54a0950b86349d16c9c66217"),
    hex!("4732835f6fdbb87ed98af06a625dca1a4fd01d09613860ea358ff6b16fa853cf"),
    hex!("d971e48721938d4c646cb2153555963311e3ee2557709a9576f71e54935addbf"),
    hex!("92a3397a8c05d57e86d59509c2ea7581099628b657debcdcb47f5d63b877013e"),
    hex!("99512c14e194816e7b96450eba8b0579ddbc2024cec1c19bc11dd4577b6a2afc"),
    hex!("192c51b081ba657c2fad4e18724ecf742deadc5d901901ca7b816d000a01186e"),
];

#[inline]
fn hash_leaf(data: &[u8]) -> Hash {
    hashv(&[LEAF_LABEL, data])
}

#[inline]
fn hash_pair(left: Hash, right: Hash) -> Hash {
    let combined;
    if left.as_ref() <= right.as_ref() {
        combined = [NODE_LABEL, left.as_ref(), right.as_ref()];
    } else {
        combined = [NODE_LABEL, right.as_ref(), left.as_ref()];
    }

    hashv(&combined)
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
        debug_assert!(N > 0 && N <= MAX_MERKLE_TREE_HEIGHT);

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

    pub fn add_leaf(&mut self, leaf: &[u8]) -> Result<u64, MerkleError> {
        if self.next_index >= Self::capacity() {
            return Err(MerkleError::TreeFull);
        }

        let inserted_index = self.next_index;

        let mut cur = hash_leaf(leaf);
        let mut idx = inserted_index;

        for level in 0..N {
            if (idx & 1) == 0 {
                
                // Record the subtree and pair with empty at this level.
                self.filled_subtrees[level] = cur;
                cur = hash_pair(cur, EMPTY_ROOTS[level].into());
                
            } else {
                
                // Combine with previously stored left subtree.
                cur = hash_pair(self.filled_subtrees[level], cur);
            }
            
            idx >>= 1;
        }

        self.root = cur;
        self.next_index += 1;
        Ok(inserted_index)
    }

    /// Replaces a leaf in the tree with a new leaf using the provided proof.
    pub fn update_leaf(
        &mut self,
        proof: &[Hash],
        old_leaf: &[u8],
        new_leaf: &[u8],
    ) -> Result<(), MerkleError> {
        if proof.len() != N {
            return Err(MerkleError::InvalidProof);
        }

        let original_leaf_hash = hash_leaf(old_leaf);
        let new_leaf_hash = hash_leaf(new_leaf);

        let original_path = compute_path(proof, original_leaf_hash);
        let new_path = compute_path(proof, new_leaf_hash);

        if *original_path.last().unwrap() != self.root {
            return Err(MerkleError::InvalidProof);
        }

        for i in 0..N {
            if original_path[i] == self.filled_subtrees[i] {
                self.filled_subtrees[i] = new_path[i];
            }
        }
        self.root = *new_path.last().unwrap();
        Ok(())
    }

    pub fn contains(&self, proof: &[Hash], leaf: &[u8]) -> bool {
        verify_proof(leaf, &self.root, proof)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MerkleError {
    TreeFull,
    InvalidProof,
}

/// Computes the path from the leaf to the root using the provided proof.
pub fn compute_path(proof: &[Hash], leaf: Hash) -> Vec<Hash> {
    let mut computed_path = Vec::with_capacity(proof.len() + 1);
    let mut computed_hash = leaf;

    computed_path.push(computed_hash);

    for &proof_element in proof.iter() {
        computed_hash = hash_pair(computed_hash, proof_element);
        computed_path.push(computed_hash);
    }

    computed_path
}

pub fn create_merkle_proof<T: AsRef<[u8]>>(leaves: &[T], index: usize) -> Vec<Hash> {
    assert!(!leaves.is_empty(), "cannot create proof for empty leaf set");
    assert!(index < leaves.len(), "index out of bounds");

    // Compute leaf hashes.
    let mut level: Vec<Hash> = leaves
        .iter()
        .map(|l| hash_leaf(l.as_ref()))
        .collect();
    
    let mut i = index;
    let mut proof = Vec::new();
    let mut height = 0;

    // While more than one node at the current level
    while level.len() > 1 {
        
        // Push sibling hash (or empty if missing).
        let len = level.len();
        let sib = i ^ 1;
        if sib < len {
            proof.push(level[sib]);
        } else {
            proof.push(EMPTY_ROOTS[height].into());
        }

        // Build next level.
        let mut next = Vec::with_capacity((len + 1) / 2);
        for j in (0..len).step_by(2) {
            let left = level[j];
            let right = if j + 1 < len {
                level[j + 1]
            } else {
                EMPTY_ROOTS[height].into()
            };
            next.push(hash_pair(left, right));
        }

        level = next;
        i >>= 1;
        height += 1;
    }

    proof
}

pub fn verify_proof(data: &[u8], root: &Hash, proof: &[Hash]) -> bool {
    let mut node = hash_leaf(data);

    for h in proof {
        node = hash_pair(node, *h);
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
        let root = tree.root;

        // proof and verify all leaves
        let proof = create_merkle_proof(&data, 0);
        assert!(verify_proof(&data[0], &root, &proof));
        
        let proof = create_merkle_proof(&data, 1);
        assert!(verify_proof(&data[1], &root, &proof));
        
        let proof = create_merkle_proof(&data, 2);
        assert!(verify_proof(&data[2], &root, &proof));
        
        let proof = create_merkle_proof(&data, 3);
        assert!(verify_proof(&data[3], &root, &proof));
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
        assert!(tree.contains(&leaf1_proof, val1));
        assert!(tree.contains(&leaf2_proof, val2));
        assert!(tree.contains(&leaf3_proof, val3));

        // Check empty leaves
        assert!(tree.contains(&[c, i, n], empty_value));
        assert!(tree.contains(&[f, l, m], empty_value));
        assert!(tree.contains(&[e, l, m], empty_value));
        assert!(tree.contains(&[h, k, m], empty_value));
        assert!(tree.contains(&[g, k, m], empty_value));

        // Replace leaf2 with empty (simulate remove)
        tree.update_leaf(&leaf2_proof, val2, empty_value).unwrap();

        // Update the expected tree structure
        let i_new = hash_pair(a, empty);
        let m_new = hash_pair(i_new, j);
        let root_new = hash_pair(m_new, n);

        assert_eq!(tree.root, root_new);

        let leaf1_proof_new = vec![empty, j, n];
        let leaf3_proof_new = vec![d, i_new, n];

        assert!(tree.contains(&leaf1_proof_new, val1));
        assert!(tree.contains(&leaf2_proof, empty_value));
        assert!(tree.contains(&leaf3_proof_new, val3));

        // Check that leaf2 is no longer in the tree
        assert!(!tree.contains(&leaf2_proof, val2));

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

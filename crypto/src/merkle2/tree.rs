//! Implementation of a Merkle tree.
//!
//! It supports non-power-of-two leaf count by adding empty leaves.
//! That is, a tree with 3 leaves is equivalent to a tree with 4 leaves,
//! where the 4th leaf has the empty byte slice `&[]` as its data.
//!
//! The maximum height of trees supported is [`MAX_MERKLE_TREE_HEIGHT`].
//! Once constructed, the tree is immutable.
//!
//! Labels are used to reduce the impact of multiple attack vectors:
//! - multi-target attacks against this and other implementations
//! - rainbow tables / pre-calculation attacks
//! - ambiguity between leaf and inner nodes with unknown tree height

use core::marker::PhantomData;
use crate::hash::{hashv, Hash};
use hex_literal::hex;

/// Maximum height of Merkle trees currently supported.
pub const MAX_MERKLE_TREE_HEIGHT: usize = 32;

/// Maximum number of leaf nodes in the Merkle trees currently supported.
pub const MAX_MERKLE_TREE_LEAVES: usize = 1 << MAX_MERKLE_TREE_HEIGHT;

const LEAF_LABEL:  &[u8] = b"LEAF";
const LEFT_LABEL:  &[u8] = b"LEFT";
const RIGHT_LABEL: &[u8] = b"RIGHT";

/// Pre-calculated empty roots for up to `2 ^ MAX_MERKLE_TREE_HEIGHT` leaves.
///
/// These are calculated by running `cargo test -- empty_roots --no-capture`.
///
/// Used for efficient check whether leaf is last in [`MerkleTree::check_proof_last`].
const EMPTY_ROOTS: [[u8;32]; MAX_MERKLE_TREE_HEIGHT] = [
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

/// Marker trait for the leaf nodes of a Merkle tree.
pub trait MerkleLeaf: AsRef<[u8]> {}

/// Trait for the root of a Merkle tree.
pub trait MerkleRoot: From<Hash> {
    fn as_hash(&self) -> &Hash;
}

/// Marker trait for the proof of a Merkle tree.
pub trait MerkleProof: AsRef<[Hash]> + From<Vec<Hash>> {}

impl<T> MerkleLeaf for T where T: AsRef<[u8]> {}
impl MerkleRoot for Hash {
    fn as_hash(&self) -> &Hash {
        self
    }
}
impl MerkleProof for Vec<Hash> {}

/// A plain Merkle tree over arbitrary bytes.
///
/// Usually, you want the additional type-safety of not using these basic types.
/// For this implement [`MerkleLeaf`], [`MerkleRoot`] and [`MerkleProof`] on your own types.
pub type PlainMerkleTree = MerkleTree<Vec<u8>, Hash, Vec<Hash>>;

/// Implementation of a Merkle tree.
pub struct MerkleTree<Leaf: MerkleLeaf, Root: MerkleRoot, Proof: MerkleProof> {
    /// All hashes in the tree, leaf hashes and inner nodes.
    nodes: Vec<Hash>,
    /// For each level, has the offset in `nodes` and the number of hashes on that level.
    levels: Vec<(u32, u32)>,
    /// Marker for the type of the tree.
    _type: PhantomData<(Leaf, Root, Proof)>,
}

impl<Leaf: MerkleLeaf, Root: MerkleRoot, Proof: MerkleProof> MerkleTree<Leaf, Root, Proof> {
    /// Creates a new Merkle tree from the given data for each leaf.
    ///
    /// This will always create a perfect binary tree (filling with empty leaves as necessary).
    /// If you want to create a tree with more than half of the leaves empty,
    /// you have to explicitly pass in empty leaves as part of `data`.
    pub fn new<'a>(data: impl IntoIterator<Item = &'a Leaf>) -> Self
    where
        Leaf: 'a,
    {
        // calculate leaf hashes
        let mut nodes = data
            .into_iter()
            .map(|leaf| Self::hash_leaf(leaf))
            .collect::<Vec<Hash>>();
        assert!(!nodes.is_empty());

        // reserve enough space for inner nodes
        let mut num_inner_nodes = 1;
        for i in 1..=nodes.len().ilog2() {
            num_inner_nodes += nodes.len().div_ceil(1 << i);
        }
        nodes.reserve(num_inner_nodes);

        // prepare levels index with correct size
        let mut levels = Vec::with_capacity(nodes.len().ilog2() as usize + 1);
        levels.push((0, nodes.len().try_into().expect("too many leaves")));

        // calculate inner nodes
        let mut left = 0;
        let mut right = nodes.len();
        let mut len = right - left;
        let mut h = 0;
        while len > 1 {
            for i in (left..right).step_by(2) {
                if i == right {
                    break;
                } else if i + 1 == right {
                    let inner_node = Self::hash_pair(nodes[i], EMPTY_ROOTS[h].into());
                    nodes.push(inner_node);
                    break;
                }
                let inner_node = Self::hash_pair(nodes[i], nodes[i + 1]);
                nodes.push(inner_node);
            }

            len = len.div_ceil(2);
            left = right;
            right = left + len;
            h += 1;
            levels.push((left as u32, len as u32));
        }

        Self {
            nodes,
            levels,
            _type: PhantomData,
        }
    }

    /// Gives the root hash of the tree.
    #[must_use]
    pub fn get_root(&self) -> Root {
        let root_hash = *self.nodes.last().expect("empty tree");
        root_hash.into()
    }

    /// Gives the height of the tree.
    pub fn height(&self) -> usize {
        self.levels.len() - 1
    }

    /// Generates a proof of membership for the element at the given `index`.
    ///
    /// The proof is the Merkle path from the leaf to the root.
    #[must_use]
    pub fn create_proof(&self, index: usize) -> Proof {
        assert!(index < 1 << self.height());
        assert!(index < self.levels[0].1 as usize);

        let mut proof = Vec::with_capacity(self.height());
        let mut i = index;

        for (h, (offset, len)) in self.levels.iter().enumerate().take(self.height()) {
            if i ^ 1 >= *len as usize {
                proof.push(EMPTY_ROOTS[h].into());
            } else {
                proof.push(self.nodes[*offset as usize + (i ^ 1)]);
            }
            i /= 2;
        }
        proof.into()
    }

    /// Checks a Merkle path against a leaf's data.
    ///
    /// Returns `true` iff `proof` is a valid Merkle path for a leaf containing
    /// `data` at the given `index` in the tree corresponding to the given `root`.
    #[must_use]
    pub fn check_proof(data: &Leaf, index: usize, root: &Root, proof: &Proof) -> bool {
        let hash = Self::hash_leaf(data);
        Self::check_hash_proof(hash, index, root, proof)
    }

    /// Checks a Merkle path against a leaf hash.
    ///
    /// Returns `true` iff `proof` is a valid Merkle path for a leaf that hashes
    /// to the given `hash` at the given `index` in the tree corresponding to
    /// the given `root`.
    #[must_use]
    fn check_hash_proof(hash: Hash, index: usize, root: &Root, proof: &Proof) -> bool {
        let mut i = index;
        let mut node = hash;
        for h in proof.as_ref() {
            node = match i % 2 {
                0 => Self::hash_pair(node, *h),
                _ => Self::hash_pair(*h, node),
            };
            i /= 2;
        }
        node == *root.as_hash()
    }

    /// Checks a Merkle path proves the given leaf's data is last in the tree.
    ///
    /// Returns `true` iff the Merkle proof is valid and `index` is the last leaf in the tree.
    #[must_use]
    pub fn check_proof_last(leaf: &Leaf, index: usize, root: &Root, proof: &Proof) -> bool {
        let hash = Self::hash_leaf(leaf);
        Self::check_hash_proof_last(hash, index, root, proof)
    }

    /// Checks a Merkle path proves the given leaf hash is last in the tree.
    ///
    /// Returns `true` iff the Merkle proof is valid and `index` is the last leaf in the tree.
    #[must_use]
    fn check_hash_proof_last(hash: Hash, index: usize, root: &Root, proof: &Proof) -> bool {
        assert!(proof.as_ref().len() <= EMPTY_ROOTS.len());
        let mut i = index;
        let mut node = hash;
        for (height, h) in proof.as_ref().iter().enumerate() {
            node = match i % 2 {
                0 => Self::hash_pair(node, EMPTY_ROOTS[height].into()),
                _ => Self::hash_pair(*h, node),
            };
            i /= 2;
        }
        node == *root.as_hash()
    }

    /// Hashes some leaf data with a label into a leaf node.
    ///
    /// The label prevents the possibility to claim an intermediate node was a leaf.
    /// It also makes the Merkle tree more robust against pre-calculation attacks.
    fn hash_leaf(leaf: &Leaf) -> Hash {
        let data: &[u8] = leaf.as_ref();
        hashv(&[&LEAF_LABEL, data])
    }

    /// Hashes a pair of child hashes with labels into a parent (non-leaf) node.
    ///
    /// The labels prevent the possibility to claim an intermediate node was a leaf.
    /// They also make the Merkle tree more robust against pre-calculation attacks.
    fn hash_pair(left: Hash, right: Hash) -> Hash {
        hashv(&[&LEFT_LABEL, left.as_ref(), &RIGHT_LABEL, right.as_ref()])
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;

    use super::*;

    #[test]
    fn basic() {
        let data = [b"hello".to_vec(), b"world".to_vec()];
        let tree = PlainMerkleTree::new(&data);
        assert_eq!(tree.nodes.len(), 3);
    }

    #[test]
    fn two_leaves() {
        let data = [b"hello".to_vec(), b"world".to_vec()];
        let tree = PlainMerkleTree::new(&data);

        // calculate expected root hash manually
        let leaf1 = PlainMerkleTree::hash_leaf(&data[0]);
        let leaf2 = PlainMerkleTree::hash_leaf(&data[1]);
        let expected_root = PlainMerkleTree::hash_pair(leaf1, leaf2);

        assert_eq!(tree.get_root(), expected_root);
    }

    #[test]
    fn empty_trees() {
        // one empty leaf
        let data = [vec![]];
        let tree1 = PlainMerkleTree::new(&data);

        // two empty leaves
        let data = [vec![], vec![]];
        let tree2 = PlainMerkleTree::new(&data);

        // these should have different roots
        assert_ne!(tree1.get_root(), tree2.get_root());
    }

    #[test]
    fn proofs() {
        let data = [
            b"hello".to_vec(),
            b"world".to_vec(),
            b"data".to_vec(),
            b"test".to_vec(),
        ];
        let tree = PlainMerkleTree::new(&data);
        let root = tree.get_root();

        // proof and verify all leaves
        let proof = tree.create_proof(0);
        assert!(PlainMerkleTree::check_proof(&data[0], 0, &root, &proof));
        let proof = tree.create_proof(1);
        assert!(PlainMerkleTree::check_proof(&data[1], 1, &root, &proof));
        let proof = tree.create_proof(2);
        assert!(PlainMerkleTree::check_proof(&data[2], 2, &root, &proof));
        let proof = tree.create_proof(3);
        assert!(PlainMerkleTree::check_proof(&data[3], 3, &root, &proof));
    }

    #[test]
    fn three_leaves() {
        let data1 = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let tree1 = PlainMerkleTree::new(&data1);

        let data2 = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), vec![]];
        let tree2 = PlainMerkleTree::new(&data2);

        // missing leaves should be equivalent to empty leaves
        assert_eq!(tree1.get_root(), tree2.get_root());
    }

    #[test]
    fn non_power_of_two() {
        let data1 = vec![b"hello".to_vec(); 33];
        let tree1 = PlainMerkleTree::new(&data1);

        let mut data2 = vec![b"hello".to_vec(); 33];
        let empty_slice = vec![];
        data2.extend_from_slice(vec![empty_slice; 31].as_slice());
        let tree2 = PlainMerkleTree::new(data2.as_slice());

        // missing leaves should be equivalent to empty leaves
        assert_eq!(tree1.get_root(), tree2.get_root());
    }

    #[test]
    fn proof_last() {
        let data = vec![b"hello".to_vec(); 33];
        let tree = PlainMerkleTree::new(&data);
        let root = tree.get_root();

        let proof = tree.create_proof(31);
        assert!(!PlainMerkleTree::check_proof_last(
            &data[31], 31, &root, &proof
        ));

        let proof = tree.create_proof(32);
        assert!(PlainMerkleTree::check_proof_last(
            &data[32], 32, &root, &proof
        ));
    }

    #[test]
    fn fuzzing() {
        const ITERATIONS: u64 = 100;
        const MAX_NUM_LEAVES: usize = 64;
        const MAX_LEAF_DATA_LEN: usize = 64;
        const QUERIES_PER_TREE: usize = 10;

        let mut rng = rand::thread_rng();
        for _ in 0..ITERATIONS {
            let num_data = rng.gen_range(1..=MAX_NUM_LEAVES);
            let mut data = Vec::with_capacity(num_data);
            for _ in 0..num_data {
                let leaf_data_len = rng.gen_range(0..=MAX_LEAF_DATA_LEN);
                let mut leaf_data = vec![0; leaf_data_len];
                rng.fill_bytes(&mut leaf_data);
                data.push(leaf_data);
            }

            let tree = PlainMerkleTree::new(data.iter());
            let root = tree.get_root();
            for _ in 0..QUERIES_PER_TREE {
                let index = rng.gen_range(0..num_data);
                let proof = tree.create_proof(index);
                let leaf = &data[index];
                assert!(PlainMerkleTree::check_proof(leaf, index, &root, &proof));
                if index == num_data - 1 {
                    assert!(PlainMerkleTree::check_proof_last(
                        leaf, index, &root, &proof
                    ));
                }
            }
        }
    }

    // NOTE: This is used for calculating `EMPTY_ROOTS`.
    #[test]
    fn empty_roots() {
        for height in 0..MAX_MERKLE_TREE_HEIGHT {
            let mut node = PlainMerkleTree::hash_leaf(&vec![]);
            for _ in 0..height {
                node = PlainMerkleTree::hash_pair(node, node);
            }
            println!("{}", hex::encode(node));
        }
    }
}

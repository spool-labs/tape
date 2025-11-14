use bytemuck::{Pod, Zeroable};
use crate::hash::{Hash, hashv, HASH_BYTES};

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, Pod, Zeroable)]
pub struct MerkleLeaf(Hash);

impl From<[u8; HASH_BYTES]> for MerkleLeaf {
    fn from(from: [u8; 32]) -> Self {
        Self(Hash { value: from })
    }
}

impl AsRef<[u8]> for MerkleLeaf {
    fn as_ref(&self) -> &[u8] {
        &self.0.value
    }
}

impl From<MerkleLeaf> for Hash {
    fn from(leaf: MerkleLeaf) -> Self {
        leaf.0
    }
}

impl Hash {
    pub fn as_leaf(self) -> MerkleLeaf {
        MerkleLeaf(self)
    }
}

impl MerkleLeaf {
    pub fn new(data: &[&[u8]]) -> Self {
        let mut inputs = vec![b"LEAF".as_ref()];
        inputs.extend(data);
        MerkleLeaf(hashv(&inputs))
    }

    pub fn to_bytes(self) -> [u8; HASH_BYTES] {
        self.0.value
    }
}

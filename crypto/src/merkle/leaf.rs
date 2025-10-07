use bytemuck::{Pod, Zeroable};
use crate::hash::{Hash, hashv, HASH_BYTES};

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, Pod, Zeroable)]
pub struct Leaf(Hash);

impl From<[u8; HASH_BYTES]> for Leaf {
    fn from(from: [u8; 32]) -> Self {
        Self(Hash { value: from })
    }
}

impl AsRef<[u8]> for Leaf {
    fn as_ref(&self) -> &[u8] {
        &self.0.value
    }
}

impl From<Leaf> for Hash {
    fn from(leaf: Leaf) -> Self {
        leaf.0
    }
}

impl Hash {
    pub fn as_leaf(self) -> Leaf {
        Leaf(self)
    }
}

impl Leaf {
    pub fn new(data: &[&[u8]]) -> Self {
        let mut inputs = vec![b"LEAF".as_ref()];
        inputs.extend(data);
        Leaf(hashv(&inputs))
    }

    pub fn to_bytes(self) -> [u8; HASH_BYTES] {
        self.0.value
    }
}

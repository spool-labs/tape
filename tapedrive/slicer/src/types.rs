use super::shard_index::ShardIndex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    pub index: ShardIndex,
    pub data: Vec<u8>,
}

impl Shard {
    pub fn new(index: ShardIndex, data: Vec<u8>) -> Self {
        Self { index, data }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Blob {
    pub data: Vec<u8>,
}

impl From<Vec<u8>> for Blob {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl Blob {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}


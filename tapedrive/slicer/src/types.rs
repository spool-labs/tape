use super::slice_index::SliceIndex;

/// A single slice of an erasure-coded blob.
///
/// Each blob is encoded into SLICE_COUNT slices (DATA_SLICES data + CODING_SLICES parity).
/// The slice at index N for this blob will be stored in spool N on the network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    pub index: SliceIndex,
    pub data: Vec<u8>,
}

impl Slice {
    pub fn new(index: SliceIndex, data: Vec<u8>) -> Self {
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

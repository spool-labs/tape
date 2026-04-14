#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use bytemuck::{Pod, Zeroable};
use crate::erasure::{MEMBER_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Bitmap<const BITS: usize, const BYTES: usize>([u8; BYTES]);

/// Bitmap for committee members (up to MEMBER_COUNT), used to determine which node operators have
/// signed a given message.
pub type CommitteeBitmap =
    Bitmap<MEMBER_COUNT, { aligned_bytes_for_members(MEMBER_COUNT, 8) }>;

/// Bitmap for SpoolGroup members (up to SPOOL_GROUP_SIZE), used to determine which members of a
/// SpoolGroup have signed.
pub type SpoolGroupBitmap =
    Bitmap<SPOOL_GROUP_SIZE, { aligned_bytes_for_members(SPOOL_GROUP_SIZE, 8) }>;

/// Bitmap for spool groups, used to determine which groups have signed. This is not the same as
/// the CommitteeBitmap, which tracks signatures from individual node operators rather than groups,
/// and it not the same as the SpoolGroupBitmap, which tracks signatures from individual members
/// within a group.
pub type GroupBitmap =
    Bitmap<SPOOL_GROUP_COUNT, { aligned_bytes_for_members(SPOOL_GROUP_COUNT, 8) }>;

unsafe impl<const BITS: usize, const BYTES: usize> Zeroable for Bitmap<BITS, BYTES> {}
unsafe impl<const BITS: usize, const BYTES: usize> Pod for Bitmap<BITS, BYTES> {}

impl<const BITS: usize, const BYTES: usize> Bitmap<BITS, BYTES> {
    fn assert_storage() {
        assert!(bytes_for_members(BITS) <= BYTES, "bitmap storage too small for bits");
    }

    /// Returns the raw storage bytes for this bitmap.
    pub fn as_bytes(&self) -> &[u8; BYTES] {
        Self::assert_storage();
        &self.0
    }

    /// Returns the raw storage bytes for this bitmap.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; BYTES] {
        Self::assert_storage();
        &mut self.0
    }

    /// Creates a new bitmap from a list of indices.
    pub fn from_indices(indices: &[usize], n: usize) -> Self {
        Self::assert_storage();
        assert!(n <= BITS, "n exceeds bitmap capacity");
        let vec = indices_to_bitmap(indices, n);
        let mut arr = [0u8; BYTES];
        arr[..vec.len()].copy_from_slice(&vec);
        Self(arr)
    }

    /// Returns the list of set indices in the bitmap.
    pub fn indices(&self, n: usize) -> Vec<usize> {
        Self::assert_storage();
        assert!(n <= BITS, "n exceeds bitmap capacity");
        bitmap_indices(&self.0, n)
    }

    /// Sets the bit at the given index.
    pub fn set(&mut self, index: usize) {
        Self::assert_storage();
        assert!(index < BITS, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.0[byte_idx] |= 1u8 << bit_idx;
    }

    /// Checks if the bit at the given index is set.
    pub fn is_set(&self, index: usize) -> bool {
        Self::assert_storage();
        assert!(index < BITS, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        (self.0[byte_idx] >> bit_idx) & 1 == 1
    }

    /// Clears the bit at the given index.
    pub fn clear(&mut self, index: usize) {
        Self::assert_storage();
        assert!(index < BITS, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.0[byte_idx] &= !(1u8 << bit_idx);
    }

    /// Returns the number of set bits in the bitmap.
    pub fn count_ones(&self) -> usize {
        Self::assert_storage();
        self.indices(BITS).len()
    }
}

/// Returns the number of bytes needed to store n bits.
pub const fn bytes_for_members(n: usize) -> usize {
    (n + 7) / 8
}

/// Returns the number of bytes needed to store n bits, rounded up to alignment_bytes.
pub const fn aligned_bytes_for_members(n: usize, alignment_bytes: usize) -> usize {
    let byte_count = bytes_for_members(n);
    if alignment_bytes == 0 {
        return byte_count;
    }

    ((byte_count + alignment_bytes - 1) / alignment_bytes) * alignment_bytes
}

/// Returns the indices of set bits in the given bitmap up to n bits.
pub fn bitmap_indices(bitmap: &[u8], n: usize) -> Vec<usize> {
    assert!(n <= bitmap.len() * 8, "bitmap too small for n");
    let mut out = Vec::with_capacity(bitmap.len() * 4);

    for i in 0..n {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        let b = bitmap[byte_idx];
        if ((b >> bit_idx) & 1) == 1 {
            out.push(i);
        }
    }
    out
}

/// Creates a bitmap from a list of indices for n bits.
pub fn indices_to_bitmap(indices: &[usize], n: usize) -> Vec<u8> {
    let byte_len = (n + 7) / 8;
    let mut bitmap = vec![0u8; byte_len];

    for &i in indices {
        assert!(i < n, "index {} out of range for n={}", i, n);
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        bitmap[byte_idx] |= 1u8 << bit_idx;
    }
    bitmap
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 16;
    type TestBitmap = Bitmap<N, 2>;

    #[test]
    fn test_from_indices() {
        let indices = vec![0, 2, 4, 8, 10, 15];
        let bitmap = TestBitmap::from_indices(&indices, N);
        let retrieved = bitmap.indices(N);
        assert_eq!(retrieved, indices);
    }

    #[test]
    fn test_set_and_is_set() {
        let mut bitmap = TestBitmap::zeroed();
        bitmap.set(0);
        bitmap.set(7);
        bitmap.set(8);
        bitmap.set(15);

        assert!(bitmap.is_set(0));
        assert!(bitmap.is_set(7));
        assert!(bitmap.is_set(8));
        assert!(bitmap.is_set(15));
        assert!(!bitmap.is_set(1));
        assert!(!bitmap.is_set(9));
    }

    #[test]
    fn test_count_ones() {
        let indices = vec![1, 3, 5, 7, 9, 11, 13, 15];
        let bitmap = TestBitmap::from_indices(&indices, N);
        assert_eq!(bitmap.count_ones(), 8);
    }

    #[test]
    fn test_empty() {
        let indices: Vec<usize> = vec![];
        let bitmap = TestBitmap::from_indices(&indices, N);
        assert_eq!(bitmap.indices(N), Vec::<usize>::new());
        assert_eq!(bitmap.count_ones(), 0);
    }

    #[test]
    fn test_full() {
        let indices: Vec<usize> = (0..N).collect();
        let bitmap = TestBitmap::from_indices(&indices, N);
        assert_eq!(bitmap.indices(N), indices);
        assert_eq!(bitmap.count_ones(), N);
    }

    #[test]
    #[should_panic(expected = "bitmap storage too small for bits")]
    fn test_from_indices_too_small() {
        let indices = vec![0];
        let _ = Bitmap::<9, 1>::from_indices(&indices, 9); // 9 bits need 2 bytes
    }

    #[test]
    #[should_panic(expected = "n exceeds bitmap capacity")]
    fn test_indices_exceeds_capacity() {
        let bitmap = TestBitmap::zeroed();
        let _ = bitmap.indices(17);
    }

    #[test]
    fn count_ones_ignores_storage_padding() {
        let mut bitmap = Bitmap::<10, 2>::zeroed();
        bitmap.set(0);
        bitmap.set(9);
        assert_eq!(bitmap.count_ones(), 2);
    }

    #[test]
    fn aligned_bytes() {
        assert_eq!(aligned_bytes_for_members(50, 8), 8);
        assert_eq!(aligned_bytes_for_members(128, 8), 16);
        assert_eq!(aligned_bytes_for_members(50, 0), 7);
    }
}

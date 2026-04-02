#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Bitmap<const BYTES: usize>([u8; BYTES]);

unsafe impl<const BYTES: usize> Zeroable for Bitmap<BYTES> {}
unsafe impl<const BYTES: usize> Pod for Bitmap<BYTES> {}

impl<const BYTES: usize> Bitmap<BYTES> {
    /// Creates a new bitmap from a list of indices.
    pub fn from_indices(indices: &[usize], n: usize) -> Self {
        let required = bytes_for_members(n);
        assert!(required <= BYTES, "bitmap too small for n");
        let vec = indices_to_bitmap(indices, n);
        let mut arr = [0u8; BYTES];
        arr[..required].copy_from_slice(&vec);
        Self(arr)
    }

    /// Returns the list of set indices in the bitmap.
    pub fn indices(&self, n: usize) -> Vec<usize> {
        assert!(n <= BYTES * 8, "n exceeds bitmap capacity");
        bitmap_indices(&self.0, n)
    }

    /// Sets the bit at the given index.
    pub fn set(&mut self, index: usize) {
        assert!(index < BYTES * 8, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.0[byte_idx] |= 1u8 << bit_idx;
    }

    /// Checks if the bit at the given index is set.
    pub fn is_set(&self, index: usize) -> bool {
        assert!(index < BYTES * 8, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        (self.0[byte_idx] >> bit_idx) & 1 == 1
    }

    /// Clears the bit at the given index.
    pub fn clear(&mut self, index: usize) {
        assert!(index < BYTES * 8, "index out of range");
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.0[byte_idx] &= !(1u8 << bit_idx);
    }

    /// Returns the number of set bits in the bitmap.
    pub fn count_ones(&self) -> usize {
        self.0.iter().map(|&b| b.count_ones() as usize).sum()
    }
}

/// Returns the number of bytes needed to store n bits.
pub const fn bytes_for_members(n: usize) -> usize {
    (n + 7) / 8
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
    type TestBitmap = Bitmap<2>;

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
        assert_eq!(bitmap.indices(N), vec![]);
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
    #[should_panic(expected = "bitmap too small for n")]
    fn test_from_indices_too_small() {
        let indices = vec![0];
        let _ = Bitmap::<1>::from_indices(&indices, 9); // 9 bits need 2 bytes
    }

    #[test]
    #[should_panic(expected = "n exceeds bitmap capacity")]
    fn test_indices_exceeds_capacity() {
        let bitmap = TestBitmap::zeroed();
        let _ = bitmap.indices(17);
    }
}

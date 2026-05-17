#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};
use bytemuck::{Pod, Zeroable};
use crate::erasure::GROUP_SIZE;

/// Bitmap for Groups.
pub type GroupBitmap = DynamicBitmap;

/// Bitmap for Spools within a Group (fixed size)
pub type SpoolBitmap = Bitmap<GROUP_SIZE, { aligned_bytes_for_members(GROUP_SIZE, 8) }>;


/// Read operations over a bit-addressed `[u8]` region.
pub trait BitmapRead {
    fn bytes(&self) -> &[u8];
    fn bits(&self) -> usize;

    fn is_set(&self, i: usize) -> bool {
        assert!(i < self.bits(), "index out of range");
        (self.bytes()[i / 8] >> (i % 8)) & 1 == 1
    }

    fn count_ones(&self) -> usize {
        // Only count bits below `bits()`; storage bytes may extend past it.
        let full = self.bits() / 8;
        let tail_bits = self.bits() % 8;
        let bytes = self.bytes();
        let mut n: usize = bytes[..full].iter().map(|b| b.count_ones() as usize).sum();
        if tail_bits > 0 {
            let mask: u8 = (1u16 << tail_bits) as u8 - 1;
            n += (bytes[full] & mask).count_ones() as usize;
        }
        n
    }

    fn indices(&self) -> Vec<usize> {
        bitmap_indices(self.bytes(), self.bits())
    }
}

/// Mutating operations over a bit-addressed `[u8]` region.
pub trait BitmapWrite: BitmapRead {
    fn bytes_mut(&mut self) -> &mut [u8];

    fn set(&mut self, i: usize) {
        assert!(i < self.bits(), "index out of range");
        self.bytes_mut()[i / 8] |= 1u8 << (i % 8);
    }

    fn clear(&mut self, i: usize) {
        assert!(i < self.bits(), "index out of range");
        self.bytes_mut()[i / 8] &= !(1u8 << (i % 8));
    }
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct Bitmap<const BITS: usize, const BYTES: usize>([u8; BYTES]);

unsafe impl<const BITS: usize, const BYTES: usize> Zeroable for Bitmap<BITS, BYTES> {}
unsafe impl<const BITS: usize, const BYTES: usize> Pod for Bitmap<BITS, BYTES> {}

impl<const BITS: usize, const BYTES: usize> Bitmap<BITS, BYTES> {
    const _STORAGE_CHECK: () = assert!(bytes_for_members(BITS) <= BYTES, "bitmap storage too small for bits");

    pub fn as_bytes(&self) -> &[u8; BYTES] { &self.0 }
    pub fn as_bytes_mut(&mut self) -> &mut [u8; BYTES] { &mut self.0 }

    pub fn from_indices(indices: &[usize]) -> Self {
        let vec = indices_to_bitmap(indices, BITS);
        let mut arr = [0u8; BYTES];
        arr[..vec.len()].copy_from_slice(&vec);
        Self(arr)
    }
}

impl<const BITS: usize, const BYTES: usize> BitmapRead for Bitmap<BITS, BYTES> {
    fn bytes(&self) -> &[u8] { &self.0 }
    fn bits(&self) -> usize { BITS }
}

impl<const BITS: usize, const BYTES: usize> BitmapWrite for Bitmap<BITS, BYTES> {
    fn bytes_mut(&mut self) -> &mut [u8] { &mut self.0 }
}

/// Runtime-sized owned bitmap.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DynamicBitmap {
    bytes: Vec<u8>,
    bits: usize,
}

impl DynamicBitmap {
    /// Empty bitmap with capacity for `bits` bits.
    pub fn zeroed(bits: usize) -> Self {
        Self { bytes: vec![0u8; bytes_for_members(bits)], bits }
    }

    pub fn from_indices(indices: &[usize], bits: usize) -> Self {
        Self { bytes: indices_to_bitmap(indices, bits), bits }
    }

    pub fn as_bytes(&self) -> &[u8] { &self.bytes }
    pub fn as_bytes_mut(&mut self) -> &mut [u8] { &mut self.bytes }
    pub fn into_bytes(self) -> Vec<u8> { self.bytes }
}

impl BitmapRead for DynamicBitmap {
    fn bytes(&self) -> &[u8] { &self.bytes }
    fn bits(&self) -> usize { self.bits }
}

impl BitmapWrite for DynamicBitmap {
    fn bytes_mut(&mut self) -> &mut [u8] { &mut self.bytes }
}

/// Borrowed bitmap view over a `&[u8]` + explicit bit count.
pub struct BitmapView<'a> {
    bytes: &'a [u8],
    bits: usize,
}

impl<'a> BitmapView<'a> {
    pub fn new(bytes: &'a [u8], bits: usize) -> Self {
        assert!(bytes.len() >= bytes_for_members(bits), "backing slice too small");
        Self { bytes, bits }
    }
}

impl<'a> BitmapRead for BitmapView<'a> {
    fn bytes(&self) -> &[u8] { self.bytes }
    fn bits(&self) -> usize { self.bits }
}

/// Borrowed mutable bitmap view over `&mut [u8]` + explicit bit count.
pub struct BitmapMut<'a> {
    bytes: &'a mut [u8],
    bits: usize,
}

impl<'a> BitmapMut<'a> {
    pub fn new(bytes: &'a mut [u8], bits: usize) -> Self {
        assert!(bytes.len() >= bytes_for_members(bits), "backing slice too small");
        Self { bytes, bits }
    }
}

impl<'a> BitmapRead for BitmapMut<'a> {
    fn bytes(&self) -> &[u8] { self.bytes }
    fn bits(&self) -> usize { self.bits }
}

impl<'a> BitmapWrite for BitmapMut<'a> {
    fn bytes_mut(&mut self) -> &mut [u8] { self.bytes }
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
    fn from_indices() {
        let indices = vec![0, 2, 4, 8, 10, 15];
        let bitmap = TestBitmap::from_indices(&indices);
        assert_eq!(bitmap.indices(), indices);
    }

    #[test]
    fn set_and_is_set() {
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
    fn count_ones() {
        let indices = vec![1, 3, 5, 7, 9, 11, 13, 15];
        let bitmap = TestBitmap::from_indices(&indices);
        assert_eq!(BitmapRead::count_ones(&bitmap), 8);
    }

    #[test]
    fn empty() {
        let indices: Vec<usize> = vec![];
        let bitmap = TestBitmap::from_indices(&indices);
        assert_eq!(bitmap.indices(), Vec::<usize>::new());
        assert_eq!(BitmapRead::count_ones(&bitmap), 0);
    }

    #[test]
    fn full() {
        let indices: Vec<usize> = (0..N).collect();
        let bitmap = TestBitmap::from_indices(&indices);
        assert_eq!(bitmap.indices(), indices);
        assert_eq!(BitmapRead::count_ones(&bitmap), N);
    }

    #[test]
    fn count_ones_ignores_storage_padding() {
        let mut bitmap = Bitmap::<10, 2>::zeroed();
        bitmap.set(0);
        bitmap.set(9);
        assert_eq!(BitmapRead::count_ones(&bitmap), 2);
    }

    #[test]
    fn aligned_bytes() {
        assert_eq!(aligned_bytes_for_members(50, 8), 8);
        assert_eq!(aligned_bytes_for_members(128, 8), 16);
        assert_eq!(aligned_bytes_for_members(50, 0), 7);
    }

    #[test]
    fn bitmap_mut_view() {
        let mut bytes = [0u8; 4];
        let mut view = BitmapMut::new(&mut bytes, 20);
        view.set(0);
        view.set(19);
        assert_eq!(view.count_ones(), 2);
        assert!(view.is_set(0));
        assert!(view.is_set(19));
        assert!(!view.is_set(10));
    }
}

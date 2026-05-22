use core::marker::PhantomData;
use bytemuck::{Pod, Zeroable};

/// Pod-compatible header for an account that owns a trailing `[T]` region.
///
/// The trailing slice is sized `capacity * size_of::<T>()` bytes and lives
/// after the containing struct in account data. Reused by dynamic-sized
/// on-chain accounts.
#[repr(C)]
#[derive(Debug)]
pub struct Tail<T> {
    /// Trailing slice capacity (allocated entry count).
    pub capacity: u64,

    /// Populated entry count; positions 0..count are initialized.
    pub count: u64,

    _marker: PhantomData<T>,
}

impl<T> Clone for Tail<T> {
    fn clone(&self) -> Self { *self }
}
impl<T> Copy for Tail<T> {}
impl<T> PartialEq for Tail<T> {
    fn eq(&self, other: &Self) -> bool {
        self.capacity == other.capacity && self.count == other.count
    }
}
impl<T> Eq for Tail<T> {}

unsafe impl<T: 'static> Pod for Tail<T> {}
unsafe impl<T> Zeroable for Tail<T> {}

impl<T> Tail<T> {
    pub fn new(capacity: u64, count: u64) -> Self {
        Self { capacity, count, _marker: PhantomData }
    }

    pub fn empty(capacity: u64) -> Self {
        Self::new(capacity, 0)
    }

    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Shrinking to target_capacity would drop already-populated entries.
    pub fn would_orphan(&self, target_capacity: u64) -> bool {
        self.count > target_capacity
    }
}

impl<T: Pod> Tail<T> {
    /// Trailing slice byte length.
    pub fn trailing_size(&self) -> usize {
        (self.capacity as usize).saturating_mul(core::mem::size_of::<T>())
    }
}

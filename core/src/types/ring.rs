use bytemuck::{Pod, Zeroable};

/// A generic ring buffer to hold entries of type `T`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RingBuffer<T: Pod + Zeroable, const N: usize> {
    pub index: u64,
    pub length: u64,
    pub entries: [T; N],
}

unsafe impl<T: Pod + Zeroable, const N: usize> Zeroable for RingBuffer<T, N> {}
unsafe impl<T: Pod + Zeroable, const N: usize> Pod for RingBuffer<T, N> {}

impl<T: Pod + Zeroable, const N: usize> RingBuffer<T, N> {
    /// Create a new, empty ring buffer.
    pub fn new() -> Self {
        Self {
            index: 0,
            length: 0,
            entries: [T::zeroed(); N],
        }
    }

    /// Create a full ring buffer with all entries set to zero.
    pub fn filled_zero() -> Self {
        Self {
            index: 0,
            length: N as u64,
            entries: [T::zeroed(); N],
        }
    }

    /// Create a full ring buffer containing zeros with the logical "front"
    /// (oldest element) at `front_index`.
    pub fn filled_zero_at(front_index: usize) -> Self {
        debug_assert!(N > 0);
        Self {
            index: (front_index % N) as u64,
            length: N as u64,
            entries: [T::zeroed(); N],
        }
    }

    /// Returns true if the buffer has no entries.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns true if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.length as usize == N
    }

    /// Returns the current number of entries.
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        N
    }

    /// Push a new entry into the ring buffer.
    /// If full, overwrites the oldest entry.
    pub fn push(&mut self, entry: T) {
        let idx = (self.index + self.length) % N as u64;
        self.entries[idx as usize] = entry;

        if self.is_full() {
            // Overwrite: advance the start
            self.index = (self.index + 1) % N as u64;
        } else {
            self.length += 1;
        }
    }

    /// Returns a reference to the most recent entry, if any.
    pub fn back(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            let idx = (self.index + self.length - 1) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Returns a reference to the oldest entry, if any.
    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self.entries[self.index as usize])
        }
    }

    /// Get an entry by relative index (0 = oldest).
    pub fn get(&self, i: usize) -> Option<&T> {
        if i >= self.len() {
            None
        } else {
            let idx = (self.index + i as u64) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Get a mutable reference to an entry by relative index (0 = oldest).
    pub fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        if i >= self.len() {
            None
        } else {
            let idx = (self.index + i as u64) % N as u64;
            Some(&mut self.entries[idx as usize])
        }
    }

    /// Iterate over entries in order from oldest to newest.
    pub fn iter(&self) -> Iter<'_, T, N> {
        Iter {
            rb: self,
            front: 0,
            back: self.len(),
        }
    }
}

/// Iterator over the ring buffer from oldest to newest.
/// Supports reverse iteration via `.rev()`.
pub struct Iter<'a, T: Pod + Zeroable, const N: usize> {
    rb: &'a RingBuffer<T, N>,
    front: usize,
    back: usize,
}

impl<'a, T: Pod + Zeroable, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let i = self.front;
            self.front += 1;
            let idx = (self.rb.index + i as u64) % N as u64;
            Some(&self.rb.entries[idx as usize])
        } else {
            None
        }
    }
}

impl<'a, T: Pod + Zeroable, const N: usize> DoubleEndedIterator for Iter<'a, T, N> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let i = self.back - 1;
            self.back -= 1;
            let idx = (self.rb.index + i as u64) % N as u64;
            Some(&self.rb.entries[idx as usize])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestBuffer = RingBuffer<u64, 4>;

    #[test]
    fn new_empty_zeroed() {
        let rb = TestBuffer::new();
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
        assert!(!rb.is_full());
        assert_eq!(rb.capacity(), 4);
        assert_eq!(rb.front(), None);
        assert_eq!(rb.back(), None);
        assert_eq!(rb.get(0), None);
        assert_eq!(rb.iter().count(), 0);
        assert_eq!(rb.entries, [0, 0, 0, 0]);
    }

    #[test]
    fn filled_full_zeroes() {
        let rb = TestBuffer::filled_zero();
        assert!(rb.is_full());
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.index, 0);
        assert_eq!(rb.front(), Some(&0));
        assert_eq!(rb.back(), Some(&0));
        assert_eq!(rb.get(0), Some(&0));
        assert_eq!(rb.get(3), Some(&0));
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![0, 0, 0, 0]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![0, 0, 0, 0]);
    }

    #[test]
    fn push_grows() {
        let mut rb = TestBuffer::new();
        rb.push(10);
        rb.push(20);
        rb.push(30);

        assert_eq!(rb.len(), 3);
        assert_eq!(rb.index, 0);
        assert_eq!(rb.front(), Some(&10));
        assert_eq!(rb.back(), Some(&30));
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![10, 20, 30]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![30, 20, 10]);
    }

    #[test]
    fn overwrite_rotates() {
        let mut rb = TestBuffer::new();
        rb.push(1);
        rb.push(2);
        rb.push(3);
        rb.push(4);
        assert!(rb.is_full());
        assert_eq!(rb.index, 0);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![1, 2, 3, 4]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![4, 3, 2, 1]);

        rb.push(5);
        assert_eq!(rb.len(), 4);
        assert_eq!(rb.index, 1);
        assert_eq!(rb.front(), Some(&2));
        assert_eq!(rb.back(), Some(&5));
        let v2: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v2, vec![2, 3, 4, 5]);
        let rev_v2: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v2, vec![5, 4, 3, 2]);
    }

    #[test]
    fn filled_overwrite_order() {
        let mut rb = TestBuffer::filled_zero();
        assert_eq!(rb.index, 0);
        assert!(rb.is_full());

        rb.push(7);
        assert_eq!(rb.index, 1);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![0, 0, 0, 7]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![7, 0, 0, 0]);

        rb.push(8);
        assert_eq!(rb.index, 2);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![0, 0, 7, 8]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![8, 7, 0, 0]);

        rb.push(9);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![0, 7, 8, 9]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![9, 8, 7, 0]);

        rb.push(10);
        assert_eq!(rb.index, 0);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![7, 8, 9, 10]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![10, 9, 8, 7]);
    }

    #[test]
    fn get_wrap_order() {
        let mut rb = TestBuffer::new();
        rb.push(100);
        rb.push(200);
        rb.push(300);
        rb.push(400);

        assert_eq!(rb.get(0), Some(&100));
        assert_eq!(rb.get(1), Some(&200));
        assert_eq!(rb.get(3), Some(&400));

        *rb.get_mut(1).unwrap() += 1;
        assert_eq!(rb.get(1), Some(&201));

        rb.push(500);
        assert_eq!(rb.index, 1);
        let v: Vec<_> = rb.iter().copied().collect();
        assert_eq!(v, vec![201, 300, 400, 500]);
        let rev_v: Vec<_> = rb.iter().rev().copied().collect();
        assert_eq!(rev_v, vec![500, 400, 300, 201]);

        assert_eq!(rb.get(4), None);
        assert!(rb.get_mut(4).is_none());
    }

    #[test]
    fn front_back() {
        let mut rb = TestBuffer::new();
        assert!(rb.front().is_none());
        assert!(rb.back().is_none());

        rb.push(42);
        assert_eq!(rb.front(), Some(&42));
        assert_eq!(rb.back(), Some(&42));

        rb.push(43);
        assert_eq!(rb.front(), Some(&42));
        assert_eq!(rb.back(), Some(&43));
    }
}

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
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        (0..self.len()).map(move |i| {
            let idx = (self.index + i as u64) % N as u64;
            &self.entries[idx as usize]
        })
    }
}

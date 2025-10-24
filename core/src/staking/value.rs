use crate::types::EpochNumber;
use bytemuck::{Pod, Zeroable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochValuesError {
    Full,
    SizeMismatch,
    NotFound,
    Underflow,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EpochValues<const N: usize> {
    pub len: u64,
    pub keys: [EpochNumber; N],
    pub values: [u64; N],
}

unsafe impl<const N: usize> Zeroable for EpochValues<N> {}
unsafe impl<const N: usize> Pod for EpochValues<N> {}

impl<const N: usize> EpochValues<N> {
    /// Create a new empty EpochValues.
    pub fn new() -> Self {
        Self {
            len: 0,
            keys: [EpochNumber(0); N],
            values: [0; N],
        }
    }

    /// Creates a new EpochValues from keys and values slices.
    pub fn try_from(keys: &[EpochNumber], values: &[u64]) 
        -> Result<Self, EpochValuesError> {
        let len = keys.len();
        if len != values.len() || len > N {
            return Err(EpochValuesError::SizeMismatch);
        }

        let mut ev = Self::new();
        for i in 0..len {
            ev.keys[i] = keys[i];
            ev.values[i] = values[i];
        }

        ev.len = len as u64;
        Ok(ev)
    }

    /// Number of stored epochs.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Whether there are no stored epochs.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the value exactly at `epoch` (if present).
    pub fn get_at(&self, epoch: EpochNumber) -> Option<u64> {
        self.index_of(epoch).map(|i| self.values[i])
    }

    /// Create or replace the value at `epoch`.
    pub fn set_at(&mut self, epoch: EpochNumber, value: u64) -> Result<(), EpochValuesError> {
        if let Some(i) = self.index_of(epoch) {
            self.values[i] = value;
            return Ok(());
        }
        self.push(epoch, value)
    }

    /// Create or add `delta` to the value at `epoch`.
    /// Uses wrapping addition for deterministic behavior across build profiles.
    pub fn add_at(&mut self, epoch: EpochNumber, delta: u64) -> Result<(), EpochValuesError> {
        if let Some(i) = self.index_of(epoch) {
            self.values[i] = self.values[i].wrapping_add(delta);
            return Ok(());
        }
        self.push(epoch, delta)
    }

    /// Decrease the value at `epoch` by `delta`. Errors if not found or if it would underflow.
    pub fn decrease_at(&mut self, epoch: EpochNumber, delta: u64) -> Result<(), EpochValuesError> {
        let i = self.index_of(epoch).ok_or(EpochValuesError::NotFound)?;
        self.values[i] = self
            .values[i]
            .checked_sub(delta)
            .ok_or(EpochValuesError::Underflow)?;
        Ok(())
    }

    /// Remove and return the value exactly at `epoch`.
    pub fn remove_at(&mut self, epoch: EpochNumber) -> Option<u64> {
        if let Some(i) = self.index_of(epoch) {
            let v = self.values[i];
            self.swap_remove(i);
            Some(v)
        } else {
            None
        }
    }

    /// Saturating sum of all values with e <= `epoch` (read-only).
    pub fn sum_through(&self, epoch: EpochNumber) -> u64 {
        let mut total = 0u64;
        for i in 0..self.len() {
            if self.keys[i] <= epoch {
                total = total.saturating_add(self.values[i]);
            }
        }
        total
    }

    /// Saturating sum of all values with e < `epoch` (read-only).
    pub fn sum_before(&self, epoch: EpochNumber) -> u64 {
        let mut total = 0u64;
        for i in 0..self.len() {
            if self.keys[i] < epoch {
                total = total.saturating_add(self.values[i]);
            }
        }
        total
    }

    /// Remove and return the saturating sum of all values with e <= `epoch`.
    pub fn drain_through(&mut self, epoch: EpochNumber) -> u64 {
        self.drain(|e| e <= epoch)
    }

    /// Remove and return the saturating sum of all values with e < `epoch`.
    pub fn drain_before(&mut self, epoch: EpochNumber) -> u64 {
        self.drain(|e| e < epoch)
    }

    #[inline]
    fn index_of(&self, epoch: EpochNumber) -> Option<usize> {
        for i in 0..self.len() {
            if self.keys[i] == epoch {
                return Some(i);
            }
        }
        None
    }

    #[inline]
    fn push(&mut self, epoch: EpochNumber, value: u64) -> Result<(), EpochValuesError> {
        let len = self.len();
        if len >= N {
            return Err(EpochValuesError::Full);
        }
        self.keys[len] = epoch;
        self.values[len] = value;
        self.len = (len as u64) + 1;
        Ok(())
    }

    #[inline]
    fn swap_remove(&mut self, index: usize) {
        let len = self.len();
        debug_assert!(index < len);

        let last = len - 1;
        if index != last {
            self.keys[index] = self.keys[last];
            self.values[index] = self.values[last];
        }

        // Zero out the old last slot for determinism
        self.keys[last] = EpochNumber(0);
        self.values[last] = 0;
        self.len = last as u64;
    }

    fn drain<F: Fn(EpochNumber) -> bool>(&mut self, pred: F) -> u64 {
        let mut total = 0u64;
        let mut i = 0usize;
        while i < self.len() {
            if pred(self.keys[i]) {
                total = total.saturating_add(self.values[i]);
                self.swap_remove(i);
                // do not increment i; we swapped a new element into i
            } else {
                i += 1;
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }

    #[test]
    fn empty_start() {
        let pv = EpochValues::<5>::new();
        assert!(pv.is_empty());
        assert_eq!(pv.len(), 0);
        assert_eq!(pv.sum_through(epoch(10)), 0);
    }

    #[test]
    fn set_get() {
        let mut pv = EpochValues::<5>::new();
        assert_eq!(pv.get_at(epoch(1)), None);

        pv.set_at(epoch(1), 10).unwrap();
        assert_eq!(pv.get_at(epoch(1)), Some(10));

        pv.set_at(epoch(1), 42).unwrap();
        assert_eq!(pv.get_at(epoch(1)), Some(42));
    }

    #[test]
    fn add_values() {
        let mut pv = EpochValues::<5>::new();

        pv.add_at(epoch(2), 5).unwrap();
        assert_eq!(pv.get_at(epoch(2)), Some(5));

        pv.add_at(epoch(2), 7).unwrap();
        assert_eq!(pv.get_at(epoch(2)), Some(12));
    }

    #[test]
    fn add_wrap() {
        let mut pv = EpochValues::<5>::new();
        pv.set_at(epoch(9), u64::MAX - 1).unwrap();
        pv.add_at(epoch(9), 10).unwrap();

        assert_eq!(pv.get_at(epoch(9)), Some(8));
    }

    #[test]
    fn decrease_ok() {
        let mut pv = EpochValues::<5>::new();
        pv.set_at(epoch(3), 10).unwrap();
        let before_len = pv.len();
        pv.decrease_at(epoch(3), 10).unwrap();
        assert_eq!(pv.get_at(epoch(3)), Some(0));
        assert_eq!(pv.len(), before_len);
    }

    #[test]
    fn decrease_errors() {
        let mut pv = EpochValues::<5>::new();

        let err = pv.decrease_at(epoch(1), 1).unwrap_err();
        assert!(matches!(err, EpochValuesError::NotFound));

        pv.set_at(epoch(1), 5).unwrap();
        let err = pv.decrease_at(epoch(1), 6).unwrap_err();
        assert!(matches!(err, EpochValuesError::Underflow));
    }

    #[test]
    fn remove_value() {
        let mut pv = EpochValues::<5>::new();
        pv.set_at(epoch(4), 11).unwrap();

        let v = pv.remove_at(epoch(4));
        assert_eq!(v, Some(11));
        assert_eq!(pv.get_at(epoch(4)), None);
        assert_eq!(pv.sum_through(epoch(10)), 0);

        assert_eq!(pv.remove_at(epoch(4)), None);
    }

    #[test]
    fn sum_basic() {
        let mut pv = EpochValues::<5>::new();
        pv.add_at(epoch(3), 30).unwrap();
        pv.add_at(epoch(1), 10).unwrap();
        pv.add_at(epoch(2), 20).unwrap();

        assert_eq!(pv.sum_through(epoch(0)), 0);
        assert_eq!(pv.sum_through(epoch(1)), 10);
        assert_eq!(pv.sum_through(epoch(2)), 30);
        assert_eq!(pv.sum_through(epoch(3)), 60);
        assert_eq!(pv.sum_through(epoch(4)), 60);

        assert_eq!(pv.sum_before(epoch(0)), 0);
        assert_eq!(pv.sum_before(epoch(1)), 0);
        assert_eq!(pv.sum_before(epoch(2)), 10);
        assert_eq!(pv.sum_before(epoch(3)), 30);
        assert_eq!(pv.sum_before(epoch(4)), 60);
    }

    #[test]
    fn sum_saturate() {
        let mut pv = EpochValues::<5>::new();
        pv.set_at(epoch(1), u64::MAX).unwrap();
        pv.set_at(epoch(2), 1).unwrap();

        assert_eq!(pv.sum_through(epoch(2)), u64::MAX);
        assert_eq!(pv.sum_before(epoch(2)), u64::MAX);
    }

    #[test]
    fn drain_none() {
        let mut pv = EpochValues::<5>::new();
        assert_eq!(pv.drain_through(epoch(10)), 0);
        assert!(pv.is_empty());

        pv.set_at(epoch(5), 50).unwrap();
        let total = pv.drain_through(epoch(4));
        assert_eq!(total, 0);
        assert_eq!(pv.get_at(epoch(5)), Some(50));
    }

    #[test]
    fn drain_some() {
        let mut pv = EpochValues::<5>::new();
        pv.add_at(epoch(1), 10).unwrap();
        pv.add_at(epoch(3), 30).unwrap();
        pv.add_at(epoch(2), 20).unwrap();
        pv.add_at(epoch(4), 40).unwrap();

        let total = pv.drain_through(epoch(2));
        assert_eq!(total, 30);

        assert_eq!(pv.get_at(epoch(1)), None);
        assert_eq!(pv.get_at(epoch(2)), None);
        assert_eq!(pv.get_at(epoch(3)), Some(30));
        assert_eq!(pv.get_at(epoch(4)), Some(40));

        assert_eq!(pv.sum_through(epoch(3)), 30);
        assert_eq!(pv.sum_through(epoch(4)), 70);
    }

    #[test]
    fn drain_before() {
        let mut pv = EpochValues::<5>::new();
        pv.set_at(epoch(1), 10).unwrap();
        pv.set_at(epoch(2), 20).unwrap();
        pv.set_at(epoch(3), 30).unwrap();

        let total = pv.drain_before(epoch(3));
        assert_eq!(total, 30);

        assert_eq!(pv.get_at(epoch(1)), None);
        assert_eq!(pv.get_at(epoch(2)), None);
        assert_eq!(pv.get_at(epoch(3)), Some(30));
    }

    #[test]
    fn drain_all() {
        let mut pv = EpochValues::<5>::new();
        pv.add_at(epoch(1), 0).unwrap();
        pv.add_at(epoch(2), 20).unwrap();

        let total = pv.drain_through(epoch(3));
        assert_eq!(total, 20);
        assert!(pv.is_empty());
        assert_eq!(pv.sum_through(epoch(10)), 0);
    }

    #[test]
    fn capacity_full() {
        let mut pv = EpochValues::<2>::new();
        pv.set_at(epoch(1), 1).unwrap();
        pv.add_at(epoch(2), 2).unwrap();

        let err = pv.set_at(epoch(3), 3).unwrap_err();
        assert!(matches!(err, EpochValuesError::Full));

        pv.set_at(epoch(2), 99).unwrap();
        assert_eq!(pv.get_at(epoch(2)), Some(99));

        pv.add_at(epoch(1), 1).unwrap();
        assert_eq!(pv.get_at(epoch(1)), Some(2));
    }
}

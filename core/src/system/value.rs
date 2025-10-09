use crate::types::{EpochNumber, FixedMap};
use bytemuck::{Pod, Zeroable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingValuesError {
    ExceededCapacity,
    InsertFailed,
    InvalidEpoch,
    ValueTooLarge,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PendingValues<const N: usize>(pub FixedMap<EpochNumber, u64, N>);

unsafe impl<const N: usize> Zeroable for PendingValues<N> {}
unsafe impl<const N: usize> Pod for PendingValues<N> {}

impl<const N: usize> PendingValues<N> {
    pub fn new() -> Self {
        Self(FixedMap::new())
    }

    #[inline]
    pub fn get(&self, epoch: &EpochNumber) -> Option<&u64> {
        self.0.get(epoch)
    }

    pub fn insert_or_add(&mut self, epoch: EpochNumber, value: u64) -> Result<(), PendingValuesError> {
        if let Some(v) = self.0.get_mut(&epoch) { 
            *v += value;
        } else {
            if self.0.len() >= N {
                return Err(PendingValuesError::ExceededCapacity);
            }

            self.0.insert(epoch, value)
                .map_err(|_| PendingValuesError::InsertFailed)?;
        }

        Ok(())
    }

    pub fn insert_or_replace(&mut self, epoch: EpochNumber, value: u64) -> Result<(), PendingValuesError> {
        if let Some(v) = self.0.get_mut(&epoch) { 
            *v = value;
        } else {
            if self.0.len() >= N {
                return Err(PendingValuesError::ExceededCapacity);
            }

            self.0.insert(epoch, value)
                .map_err(|_| PendingValuesError::InsertFailed)?;
        }

        Ok(())
    }

    pub fn reduce_value(&mut self, epoch: EpochNumber, value: u64) -> Result<(), PendingValuesError> {
        let entry = self
            .0
            .get_mut(&epoch)
            .ok_or(PendingValuesError::InvalidEpoch)?;

        *entry = entry.checked_sub(value)
            .ok_or(PendingValuesError::ValueTooLarge)?;
        
        Ok(())
    }

    pub fn value_at(&self, epoch: EpochNumber) -> u64 {
        let mut total = 0u64;
        for i in 0..self.0.len() {
            let e = self.0.keys[i];
            if e <= epoch {
                total = total.saturating_add(self.0.values[i]);
            }
        }
        total
    }

    pub fn flush(&mut self, epoch: EpochNumber) -> u64 {
        let mut total = 0u64;
        let mut i = 0;
        while i < self.0.len() {
            let e = self.0.keys[i];
            if e <= epoch {
                if let Some(v) = self.0.remove(&e) {
                    total += v;
                }
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

    const CAP: usize = 5;
    type PV = PendingValues<CAP>;

    #[test]
    fn add_new() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        assert_eq!(pv.value_at(EpochNumber(0)), 0);
        assert_eq!(pv.value_at(EpochNumber(1)), 10);
        assert_eq!(pv.value_at(EpochNumber(2)), 10);
    }

    #[test]
    fn add_existing() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.insert_or_add(EpochNumber(1), 5).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 15);
    }

    #[test]
    fn add_capacity_exceed() {
        let mut pv = PV::new();
        for i in 0..CAP {
            pv.insert_or_add(EpochNumber(i as u64), 10).unwrap();
        }
        let err = pv.insert_or_add(EpochNumber(100), 10).err().unwrap();
        assert_eq!(err, PendingValuesError::ExceededCapacity);
    }

    #[test]
    fn replace_new() {
        let mut pv = PV::new();
        pv.insert_or_replace(EpochNumber(1), 10).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 10);
    }

    #[test]
    fn replace_existing() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.insert_or_replace(EpochNumber(1), 20).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 20);
    }

    #[test]
    fn replace_capacity_exceed() {
        let mut pv = PV::new();
        for i in 0..CAP {
            pv.insert_or_replace(EpochNumber(i as u64), 10).unwrap();
        }
        let err = pv.insert_or_replace(EpochNumber(100), 10).err().unwrap();
        assert_eq!(err, PendingValuesError::ExceededCapacity);
    }

    #[test]
    fn reduce_success() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.reduce_value(EpochNumber(1), 3).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 7);
    }

    #[test]
    fn reduce_to_zero() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.reduce_value(EpochNumber(1), 10).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 0);
    }

    #[test]
    fn reduce_invalid() {
        let mut pv = PV::new();
        let err = pv.reduce_value(EpochNumber(1), 0).err().unwrap();
        assert_eq!(err, PendingValuesError::InvalidEpoch);
    }

    #[test]
    fn reduce_too_large() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        let err = pv.reduce_value(EpochNumber(1), 11).err().unwrap();
        assert_eq!(err, PendingValuesError::ValueTooLarge);
    }

    #[test]
    fn value_empty() {
        let pv = PV::new();
        assert_eq!(pv.value_at(EpochNumber(0)), 0);
    }

    #[test]
    fn value_single() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        assert_eq!(pv.value_at(EpochNumber(1)), 10);
    }

    #[test]
    fn value_multiple() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(3), 30).unwrap();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.insert_or_add(EpochNumber(2), 20).unwrap();
        assert_eq!(pv.value_at(EpochNumber(0)), 0);
        assert_eq!(pv.value_at(EpochNumber(1)), 10);
        assert_eq!(pv.value_at(EpochNumber(2)), 30);
        assert_eq!(pv.value_at(EpochNumber(3)), 60);
        assert_eq!(pv.value_at(EpochNumber(4)), 60);
    }

    #[test]
    fn value_saturate() {
        let mut pv = PV::new();
        pv.insert_or_replace(EpochNumber(1), u64::MAX).unwrap();
        pv.insert_or_replace(EpochNumber(2), 1).unwrap();
        assert_eq!(pv.value_at(EpochNumber(2)), u64::MAX);
    }

    #[test]
    fn flush_empty() {
        let mut pv = PV::new();
        let total = pv.flush(EpochNumber(10));
        assert_eq!(total, 0);
        assert_eq!(pv.value_at(EpochNumber(10)), 0);
    }

    #[test]
    fn flush_none() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(5), 50).unwrap();
        let total = pv.flush(EpochNumber(4));
        assert_eq!(total, 0);
        assert_eq!(pv.value_at(EpochNumber(4)), 0);
        assert_eq!(pv.value_at(EpochNumber(5)), 50);
    }

    #[test]
    fn flush_some() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.insert_or_add(EpochNumber(3), 30).unwrap();
        pv.insert_or_add(EpochNumber(2), 20).unwrap();
        pv.insert_or_add(EpochNumber(4), 40).unwrap();
        let total = pv.flush(EpochNumber(2));
        assert_eq!(total, 30);
        assert_eq!(pv.value_at(EpochNumber(2)), 0);
        assert_eq!(pv.value_at(EpochNumber(3)), 30);
        assert_eq!(pv.value_at(EpochNumber(4)), 70);
    }

    #[test]
    fn flush_all() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 10).unwrap();
        pv.insert_or_add(EpochNumber(2), 20).unwrap();
        let total = pv.flush(EpochNumber(3));
        assert_eq!(total, 30);
        assert_eq!(pv.value_at(EpochNumber(3)), 0);
    }

    #[test]
    fn flush_zero() {
        let mut pv = PV::new();
        pv.insert_or_add(EpochNumber(1), 0).unwrap();
        let total = pv.flush(EpochNumber(1));
        assert_eq!(total, 0);
        assert_eq!(pv.value_at(EpochNumber(1)), 0);
    }
}

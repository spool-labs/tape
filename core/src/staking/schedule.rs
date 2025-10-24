use bytemuck::{Pod, Zeroable};
use crate::types::*;
use super::value::EpochValues;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    ScheduleFailed,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PoolSchedule<const N: usize> {
    /// The scheduled commission rate changes, scheduled for future epochs.
    pub commission_changes: EpochValues<N>,

    /// The scheduled stake additions, scheduled for future epochs.
    pub incoming_tokens: EpochValues<N>,

    /// The scheduled pre-active stake cancellations, scheduled for future epochs.
    pub outgoing_tokens: EpochValues<N>,

    /// The scheduled share withdrawals, scheduled for future epochs.
    pub outgoing_shares: EpochValues<N>,
}

impl<const N: usize> PoolSchedule<N> {
    pub fn new() -> Self {
        Self {
            commission_changes: EpochValues::new(),
            incoming_tokens: EpochValues::new(),
            outgoing_tokens: EpochValues::new(),
            outgoing_shares: EpochValues::new(),
        }
    }

    #[inline]
    pub fn set_commission(
        &mut self,
        epoch: EpochNumber,
        new_rate: BasisPoints,
    ) -> Result<(), ScheduleError> {
        self.commission_changes
            .set_at(epoch, new_rate.into())
            .map_err(|_| ScheduleError::ScheduleFailed)
    }

    #[inline]
    pub fn add_stake(
        &mut self,
        epoch: EpochNumber,
        amount: Coin<TAPE>,
    ) -> Result<(), ScheduleError> {
        self.incoming_tokens
            .add_at(epoch, amount.into())
            .map_err(|_| ScheduleError::ScheduleFailed)
    }

    #[inline]
    pub fn add_cancel(
        &mut self,
        epoch: EpochNumber,
        amount: Coin<TAPE>,
    ) -> Result<(), ScheduleError> {
        self.outgoing_tokens
            .add_at(epoch, amount.into())
            .map_err(|_| ScheduleError::ScheduleFailed)
    }

    #[inline]
    pub fn add_unstake(
        &mut self,
        epoch: EpochNumber,
        shares: u64,
    ) -> Result<(), ScheduleError> {
        self.outgoing_shares
            .add_at(epoch, shares)
            .map_err(|_| ScheduleError::ScheduleFailed)
    }

    /// Get the commission rate change scheduled exactly at `epoch` (if any).
    #[inline]
    pub fn commission_at(&self, epoch: EpochNumber) -> Option<BasisPoints> {
        self.commission_changes.get_at(epoch).map(BasisPoints)
    }

    /// Sum of incoming stake scheduled at or before `epoch`.
    #[inline]
    pub fn incoming_sum(&self, epoch: EpochNumber) -> u64 {
        self.incoming_tokens.sum_through(epoch)
    }

    /// Sum of pre-activation cancels scheduled at or before `epoch`.
    #[inline]
    pub fn outgoing_sum(&self, epoch: EpochNumber) -> u64 {
        self.outgoing_tokens.sum_through(epoch)
    }

    /// Sum of share withdrawals scheduled at or before `epoch`.
    #[inline]
    pub fn outgoing_shares_sum(&self, epoch: EpochNumber) -> u64 {
        self.outgoing_shares.sum_through(epoch)
    }

    /// If there is a commission rate change scheduled exactly at `epoch`,
    /// return it and clear all commission entries with e <= epoch.
    /// If there is no exact entry, do nothing and return None.
    #[inline]
    pub fn take_commission_change(&mut self, epoch: EpochNumber) -> Option<BasisPoints> {
        let exact = self.commission_changes.get_at(epoch);
        if exact.is_some() {
            // Clear all <= epoch to mirror previous semantics
            self.commission_changes.drain_through(epoch);
        }
        exact.map(BasisPoints)
    }

    /// Drain and sum all incoming stake with e <= epoch.
    #[inline]
    pub fn take_incoming(&mut self, epoch: EpochNumber) -> u64 {
        self.incoming_tokens.drain_through(epoch)
    }

    /// Drain and sum all cancels with e <= epoch.
    #[inline]
    pub fn take_outgoing(&mut self, epoch: EpochNumber) -> u64 {
        self.outgoing_tokens.drain_through(epoch)
    }

    /// Drain and sum all outgoing shares with e <= epoch.
    #[inline]
    pub fn take_outgoing_shares(&mut self, epoch: EpochNumber) -> u64 {
        self.outgoing_shares.drain_through(epoch)
    }

    #[inline]
    pub fn commission_count(&self) -> usize { self.commission_changes.len() }

    #[inline]
    pub fn incoming_count(&self) -> usize { self.incoming_tokens.len() }

    #[inline]
    pub fn outgoing_count(&self) -> usize { self.outgoing_tokens.len() }

    #[inline]
    pub fn outgoing_shares_count(&self) -> usize { self.outgoing_shares.len() }
}

unsafe impl<const N: usize> Zeroable for PoolSchedule<N> {}
unsafe impl<const N: usize> Pod for PoolSchedule<N> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u64) -> EpochNumber { EpochNumber(n) }
    fn tape(v: u64) -> Coin<TAPE> { TAPE(v) }

    #[test]
    fn new_ok() {
        let s = PoolSchedule::<4>::new();

        assert_eq!(s.commission_count(), 0);
        assert_eq!(s.incoming_count(), 0);
        assert_eq!(s.outgoing_count(), 0);
        assert_eq!(s.outgoing_shares_count(), 0);
    }

    #[test]
    fn schedule_commission() {
        let mut s = PoolSchedule::<4>::new();
        s.set_commission(epoch(5), BasisPoints(1000)).unwrap();
        s.set_commission(epoch(7), BasisPoints(1500)).unwrap();

        assert_eq!(s.commission_at(epoch(5)), Some(BasisPoints(1000)));
        assert_eq!(s.commission_at(epoch(6)), None);
        assert_eq!(s.commission_at(epoch(7)), Some(BasisPoints(1500)));

        // Taking at 5 clears entries <= 5 but keeps later ones
        let taken = s.take_commission_change(epoch(5));
        assert_eq!(taken, Some(BasisPoints(1000)));
        assert_eq!(s.commission_at(epoch(5)), None);
        assert_eq!(s.commission_at(epoch(7)), Some(BasisPoints(1500)));
    }

    #[test]
    fn schedule_and_drain() {
        let mut s = PoolSchedule::<8>::new();

        s.add_stake(epoch(4), tape(100)).unwrap();
        s.add_stake(epoch(6), tape(50)).unwrap();
        s.add_cancel(epoch(6), tape(20)).unwrap();

        assert_eq!(s.incoming_sum(epoch(3)), 0);
        assert_eq!(s.incoming_sum(epoch(4)), 100);
        assert_eq!(s.incoming_sum(epoch(6)), 150);
        assert_eq!(s.outgoing_sum(epoch(6)), 20);

        // drain incoming up to 5 keeps epoch 6
        let res = s.take_incoming(epoch(5));

        assert_eq!(res, 100);
        assert_eq!(s.incoming_sum(epoch(4)), 0);
        assert_eq!(s.incoming_sum(epoch(6)), 50);

        // drain cancels up to 6 clears that entry
        let res = s.take_outgoing(epoch(6));
        assert_eq!(res, 20);
        assert_eq!(s.outgoing_sum(epoch(6)), 0);
    }

    #[test]
    fn schedule_capacity() {
        // With capacity 1, two distinct epochs should exceed capacity
        let mut s = PoolSchedule::<1>::new();

        s.add_unstake(epoch(10), 123).unwrap();
        let err = s.add_unstake(epoch(11), 1).unwrap_err();
        assert!(matches!(err, ScheduleError::ScheduleFailed));
    }
}

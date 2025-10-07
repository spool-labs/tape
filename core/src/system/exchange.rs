use crate::types::{EpochNumber, RingBuffer};
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct ExchangeRate {
    pub tape: u64,
    pub other: u64,
}

impl ExchangeRate {
    pub fn new(tape_amount: u64, share_amount: u64) -> Self {
        if tape_amount == 0 || share_amount == 0 {
            ExchangeRate::flat()
        } else {
            ExchangeRate {
                tape: tape_amount,
                other: share_amount,
            }
        }
    }

    pub fn flat() -> Self {
        ExchangeRate { tape: 1, other: 1 }
    }

    pub fn convert_to_tape_amount(&self, other_amount: u64) -> u64 {
        ((other_amount as u128 * self.tape as u128) / self.other as u128) as u64
    }

    pub fn convert_to_other_amount(&self, tape_amount: u64) -> u64 {
        ((tape_amount as u128 * self.other as u128) / self.tape as u128) as u64
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct EpochExchangeRate {
    pub epoch: EpochNumber,
    pub rate: ExchangeRate,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PreviousRates<const N: usize>(RingBuffer<EpochExchangeRate, N>);

unsafe impl<const N: usize> Zeroable for PreviousRates<N> {}
unsafe impl<const N: usize> Pod for PreviousRates<N> {}

impl<const N: usize> PreviousRates<N> {
    pub fn new() -> Self {
        Self(RingBuffer::new())
    }

    /// Push a new rate for the given epoch.
    pub fn push(&mut self, epoch: EpochNumber, rate: ExchangeRate) {
        assert!(self.0.back().map_or(true, |r| r.epoch < epoch));

        self.0.push(EpochExchangeRate { epoch, rate });
    }

    /// Get the most recent rate at or before the given epoch, returning None if no such rate
    /// exists.
    pub fn on_or_before(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
        for i in self.0.iter().rev() {
            if i.epoch <= epoch {
                return Some(i.rate);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_creation() {
        let rate = ExchangeRate::flat();
        assert_eq!(rate.tape, 1);
        assert_eq!(rate.other, 1);
    }

    #[test]
    fn new_zero_tape() {
        let rate = ExchangeRate::new(0, 5);
        assert_eq!(rate, ExchangeRate::flat());
    }

    #[test]
    fn new_zero_other() {
        let rate = ExchangeRate::new(5, 0);
        assert_eq!(rate, ExchangeRate::flat());
    }

    #[test]
    fn new_both_zero() {
        let rate = ExchangeRate::new(0, 0);
        assert_eq!(rate, ExchangeRate::flat());
    }

    #[test]
    fn new_valid() {
        let rate = ExchangeRate::new(3, 4);
        assert_eq!(rate.tape, 3);
        assert_eq!(rate.other, 4);
    }

    #[test]
    fn convert_tape_flat() {
        let rate = ExchangeRate::flat();
        assert_eq!(rate.convert_to_tape_amount(10), 10);
    }

    #[test]
    fn convert_other_flat() {
        let rate = ExchangeRate::flat();
        assert_eq!(rate.convert_to_other_amount(10), 10);
    }

    #[test]
    fn convert_tape_ratio() {
        let rate = ExchangeRate::new(2, 1);
        assert_eq!(rate.convert_to_tape_amount(5), 10);
    }

    #[test]
    fn convert_other_ratio() {
        let rate = ExchangeRate::new(2, 1);
        assert_eq!(rate.convert_to_other_amount(10), 5);
    }

    #[test]
    fn convert_round_down() {
        let rate = ExchangeRate::new(1, 2);
        assert_eq!(rate.convert_to_tape_amount(1), 0);
    }

    #[test]
    fn convert_large() {
        let rate = ExchangeRate::new(u64::MAX / 2, u64::MAX / 2);
        assert_eq!(rate.convert_to_tape_amount(u64::MAX), u64::MAX);
    }

    type TestRates = PreviousRates<3>;

    #[test]
    fn new_empty() {
        let rates = TestRates::new();
        assert!(rates.on_or_before(EpochNumber(0)).is_none());
        assert!(rates.on_or_before(EpochNumber(100)).is_none());
    }

    #[test]
    fn push_one() {
        let mut rates = TestRates::new();
        let rate = ExchangeRate::new(1, 1);
        rates.push(EpochNumber(5), rate);
        assert_eq!(rates.on_or_before(EpochNumber(5)), Some(rate));
    }

    #[test]
    fn rate_before() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(5), ExchangeRate::new(1, 1));
        assert!(rates.on_or_before(EpochNumber(4)).is_none());
    }

    #[test]
    fn rate_exact() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(5), ExchangeRate::new(1, 1));
        assert_eq!(rates.on_or_before(EpochNumber(5)), Some(ExchangeRate::new(1, 1)));
    }

    #[test]
    fn rate_after() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(5), ExchangeRate::new(1, 1));
        assert_eq!(rates.on_or_before(EpochNumber(6)), Some(ExchangeRate::new(1, 1)));
    }

    #[test]
    fn push_multiple() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(1), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(3), ExchangeRate::new(2, 2));
        rates.push(EpochNumber(5), ExchangeRate::new(3, 3));
        assert_eq!(rates.on_or_before(EpochNumber(5)), Some(ExchangeRate::new(3, 3)));
    }

    #[test]
    fn rate_between() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(1), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(3), ExchangeRate::new(2, 2));
        assert_eq!(rates.on_or_before(EpochNumber(2)), Some(ExchangeRate::new(1, 1)));
    }

    #[test]
    #[should_panic]
    fn push_duplicate() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(1), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(1), ExchangeRate::new(2, 2));
    }

    #[test]
    #[should_panic]
    fn push_decrease() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(2), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(1), ExchangeRate::new(2, 2));
    }

    #[test]
    fn overflow_ring() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(1), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(2), ExchangeRate::new(2, 2));
        rates.push(EpochNumber(3), ExchangeRate::new(3, 3));
        rates.push(EpochNumber(4), ExchangeRate::new(4, 4));
        // Now should have 2,3,4
    }

    #[test]
    fn rate_overflow() {
        let mut rates = TestRates::new();
        rates.push(EpochNumber(1), ExchangeRate::new(1, 1));
        rates.push(EpochNumber(2), ExchangeRate::new(2, 2));
        rates.push(EpochNumber(3), ExchangeRate::new(3, 3));
        rates.push(EpochNumber(4), ExchangeRate::new(4, 4));
        assert!(rates.on_or_before(EpochNumber(1)).is_none());
        assert_eq!(rates.on_or_before(EpochNumber(2)), Some(ExchangeRate::new(2, 2)));
        assert_eq!(rates.on_or_before(EpochNumber(5)), Some(ExchangeRate::new(4, 4)));
    }
}

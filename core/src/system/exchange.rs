use crate::ring::RingBuffer;
use crate::types::EpochNumber;
use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct ExchangeRate {
    pub tape: u64,
    pub other: u64,
}

impl ExchangeRate {
    pub fn flat() -> Self {
        ExchangeRate { tape: 1, other: 1 }
    }

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
    pub fn rate_at(&self, epoch: EpochNumber) -> Option<ExchangeRate> {
        for i in self.0.iter().rev() {
            if i.epoch <= epoch {
                return Some(i.rate);
            }
        }

        None
    }
}

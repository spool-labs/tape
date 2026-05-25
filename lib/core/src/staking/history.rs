use bytemuck::{bytes_of, Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tape_crypto::address::Address;
use tape_crypto::hash::{hash, hashv};
use tape_crypto::Hash;

use crate::system::ExchangeRate;
use crate::track::types::CompressedTrackProof;
use crate::types::*;

pub const RATE_SPAN_V1: &[u8; 16] = b"POOL_RATE_SPAN_1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateError {
    EmptySpan,
    EpochOutsideSpan,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct RateSpan {
    pub node: Address,
    pub start_epoch: EpochNumber,
    pub end_epoch: EpochNumber,
    pub rate: ExchangeRate,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum RateKind {
    Current = 0,
    ClosedSpan,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct PoolRate {
    pub kind: u64,
    pub span: RateSpan,
    pub track: CompressedTrackProof,
}

impl PoolRate {
    pub fn current() -> Self {
        Self {
            kind: RateKind::Current.into(),
            span: RateSpan::zeroed(),
            track: CompressedTrackProof::zeroed(),
        }
    }

    pub fn closed_span(span: RateSpan, track: CompressedTrackProof) -> Self {
        Self {
            kind: RateKind::ClosedSpan.into(),
            span,
            track,
        }
    }

    pub fn kind(&self) -> Option<RateKind> {
        RateKind::try_from(self.kind).ok()
    }

    pub fn is_current(&self) -> bool {
        matches!(self.kind(), Some(RateKind::Current))
    }

    pub fn is_closed_span(&self) -> bool {
        matches!(self.kind(), Some(RateKind::ClosedSpan))
    }
}

impl RateSpan {
    #[inline(always)]
    pub fn new(
        node: Address,
        start_epoch: EpochNumber,
        end_epoch: EpochNumber,
        rate: ExchangeRate,
    ) -> Self {
        Self {
            node,
            start_epoch,
            end_epoch,
            rate,
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.start_epoch < self.end_epoch
    }

    #[inline(always)]
    pub fn contains(&self, epoch: EpochNumber) -> bool {
        self.start_epoch <= epoch && epoch < self.end_epoch
    }

    pub fn check_contains(&self, epoch: EpochNumber) -> Result<(), RateError> {
        if !self.is_valid() {
            return Err(RateError::EmptySpan);
        }
        if !self.contains(epoch) {
            return Err(RateError::EpochOutsideSpan);
        }
        Ok(())
    }

    #[inline(always)]
    pub fn key(&self) -> Hash {
        hashv(&[
            RATE_SPAN_V1,
            self.node.as_ref(),
            &self.start_epoch.pack(),
            &self.end_epoch.pack(),
        ])
    }

    #[inline(always)]
    pub fn value_hash(&self) -> Hash {
        hash(bytes_of(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u64) -> EpochNumber {
        EpochNumber(n)
    }

    #[test]
    fn span_contains_start_and_excludes_end() {
        let span = RateSpan::new(
            Address::from([7; 32]),
            epoch(10),
            epoch(20),
            ExchangeRate::flat(),
        );

        assert!(span.contains(epoch(10)));
        assert!(span.contains(epoch(19)));
        assert!(!span.contains(epoch(20)));
        assert!(!span.contains(epoch(9)));
    }

    #[test]
    fn empty_span_is_invalid() {
        let span = RateSpan::new(
            Address::from([7; 32]),
            epoch(10),
            epoch(10),
            ExchangeRate::flat(),
        );

        assert!(!span.is_valid());
        assert!(matches!(
            span.check_contains(epoch(10)),
            Err(RateError::EmptySpan)
        ));
    }

    #[test]
    fn key_changes_with_bounds() {
        let node = Address::from([7; 32]);
        let a = RateSpan::new(node, epoch(10), epoch(20), ExchangeRate::flat());
        let b = RateSpan::new(node, epoch(10), epoch(21), ExchangeRate::flat());

        assert_ne!(a.key(), b.key());
    }
}

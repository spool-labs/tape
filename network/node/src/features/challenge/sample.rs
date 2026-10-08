//! The hash that picks one track and one leaf for a spool in a round

use core::mem::size_of;
use core::ops::Range;

use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
use tape_crypto::hash::hashv;
use tape_crypto::{Address, Hash};

pub const SAMPLE_TAG: &[u8; 8] = b"SAMPLE\0\0";

/// These bytes of the sample choose the leaf
const LEAF_BYTES: Range<usize> = 8..16;

/// One spool's sample in one round
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Sample(pub Hash);

impl Sample {
    /// Hash a round's seed with the spool that answers
    ///
    /// `spool` is the live spool of the answering holder, which already
    /// carries its group and position. Every spool gets its own sample, so no
    /// two nodes of a group prove the same leaf.
    pub fn new(epoch: EpochNumber, round: RoundNumber, spool: SpoolIndex, seed: Hash) -> Self {
        Self(hashv(&[
            SAMPLE_TAG,
            &epoch.pack(),
            &round.pack(),
            &spool.pack(),
            &seed.to_bytes(),
        ]))
    }

    /// The walk over the track index starts from this address
    pub fn start(&self) -> Address {
        Address::from(self.0)
    }

    /// Choose one of `leaf_count` leaves
    pub fn leaf_index(&self, leaf_count: usize) -> Option<usize> {
        if leaf_count == 0 {
            return None;
        }

        let mut bytes = [0u8; size_of::<u64>()];
        bytes.copy_from_slice(&self.0.to_bytes()[LEAF_BYTES]);
        let leaf = u64::from_le_bytes(bytes) % leaf_count as u64;

        Some(leaf as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPOCH: EpochNumber = EpochNumber(12);
    const ROUND: RoundNumber = RoundNumber(40);
    const SPOOL: SpoolIndex = SpoolIndex(64);

    fn seed() -> Hash {
        Hash::from(core::array::from_fn(|index| index as u8))
    }

    // the sample is SHA-256 over the tag, epoch, round, spool and seed, and nothing else
    #[test]
    fn vectors() {
        let sample = Sample::new(EPOCH, ROUND, SPOOL, seed());
        let first = Sample::new(EpochNumber(0), RoundNumber(1), SpoolIndex(0), Hash::default());

        assert_eq!(
            hex::encode(sample.0.to_bytes()),
            "a78f3b2af6a3aeefa9202ede6a4589907008a4e34d1e04e161ae06c09d6847d2"
        );
        assert_eq!(
            hex::encode(first.0.to_bytes()),
            "b8219e5f2812d1c9b4779c2c4a104b7ec0cd4311cac717e437ca23c4bc31342d"
        );
    }

    // changing any one input gives another sample
    #[test]
    fn every_input() {
        let sample = Sample::new(EPOCH, ROUND, SPOOL, seed());

        assert_ne!(sample, Sample::new(EpochNumber(13), ROUND, SPOOL, seed()));
        assert_ne!(sample, Sample::new(EPOCH, RoundNumber(41), SPOOL, seed()));
        assert_ne!(sample, Sample::new(EPOCH, ROUND, SpoolIndex(65), seed()));
        assert_ne!(sample, Sample::new(EPOCH, ROUND, SPOOL, Hash::default()));
    }

    // the walk starts at the sample's own bytes
    #[test]
    fn start_address() {
        let sample = Sample::new(EPOCH, ROUND, SPOOL, seed());

        assert_eq!(sample.start().to_bytes(), sample.0.to_bytes());
    }

    // the leaf is bytes 8 to 16 of the sample, little-endian, modulo the leaf count
    #[test]
    fn leaf() {
        let sample = Sample::new(EPOCH, ROUND, SPOOL, seed());
        let number = 10_414_931_938_567_397_545u64;

        assert_eq!(sample.leaf_index(1), Some(0));
        assert_eq!(sample.leaf_index(7), Some((number % 7) as usize));
        assert_eq!(sample.leaf_index(65_536), Some((number % 65_536) as usize));
        assert_eq!(sample.leaf_index(0), None);
    }
}

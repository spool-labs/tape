use crate::consts::*;
use crate::state::{Tape, Archive};

const BLOCKS_PER_YEAR: u64 = 60 * 60 * 24 * 365 / BLOCK_DURATION_SECONDS;

/// Rent this tape pays each block.
#[inline]
pub const fn rent_per_block(total_segments: u64) -> u64 {
    total_segments
        .saturating_mul(RENT_PER_SEGMENT)
}

/// Get the minimum rent required for a tape to finalize.
#[inline]
pub const fn min_finalization_rent(total_segments: u64) -> u64 {
    rent_per_block(total_segments)
        .saturating_mul(BLOCKS_PER_YEAR)
}

/// Rent owed from `last_block` (exclusive) up to `current_block` (inclusive).
#[inline]
pub const fn rent_owed(
    total_segments: u64,
    last_block:     u64,
    current_block:  u64,
) -> u64 {
    let blocks = current_block.saturating_sub(last_block) as u128;
    (rent_per_block(total_segments) as u128 * blocks) as u64
}

impl Tape {

    /// Check if this tape is subsidized.
    #[inline]
    pub fn has_minimum_rent(&self) -> bool {
        self.balance >= self.rent_per_block()
    }

    /// Check if this tape has enough balance to cover finalization.
    #[inline]
    pub fn can_finalize(&self) -> bool {
        self.balance >= min_finalization_rent(self.total_segments)
    }

    /// Rent this tape owes per block.
    #[inline]
    pub fn rent_per_block(&self) -> u64 {
        rent_per_block(self.total_segments)
    }

    /// Rent owed since last_rent_block.
    #[inline]
    pub fn rent_owed(&self, current_block: u64) -> u64 {
        rent_owed(self.total_segments, self.last_rent_block, current_block)
    }
}

impl Archive {
    /// Global reward to miners for the current block.
    #[inline]
    pub fn block_reward(&self) -> u64 {
        rent_per_block(self.segments_stored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rent_per_block_zero_segments() {
        assert_eq!(rent_per_block(0), 0);
    }

    #[test]
    fn rent_per_block_one_segment() {
        assert_eq!(rent_per_block(1), RENT_PER_SEGMENT);
    }

    #[test]
    fn rent_per_block_max_segments_saturates() {
        assert_eq!(rent_per_block(u64::MAX), u64::MAX);
    }

    #[test]
    fn rent_owed_zero_blocks() {
        assert_eq!(rent_owed(10, 5, 5), 0);
    }

    #[test]
    fn rent_owed_basic() {
        let segments = 10;
        let last = 100_u64;
        let current = 110_u64;
        assert_eq!(
            rent_owed(segments, last, current),
            segments * RENT_PER_SEGMENT * (current - last)
        );
    }
}

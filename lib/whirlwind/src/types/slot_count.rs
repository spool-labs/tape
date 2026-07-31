//! A count of Solana slots, used for schedule durations and offsets.

use std::fmt::Display;

#[cfg(feature = "simulations")]
use serde::Serialize;

/// A span of Solana slots such as a round window, a deadline, or an offset.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "simulations", derive(Serialize))]
pub struct SlotCount(u64);

impl SlotCount {
    /// Wrap a raw slot count.
    pub const fn new(slots: u64) -> Self {
        Self(slots)
    }

    /// The slot count as a u64.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// The slot count as an f64, for conversion to milliseconds.
    pub const fn as_f64(self) -> f64 {
        self.0 as f64
    }
}

impl Display for SlotCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

use std::fmt::Display;

#[cfg(feature = "simulations")]
use serde::Serialize;

/// Zero-based index of a challenge round within an epoch.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "simulations", derive(Serialize))]
pub struct RoundNumber(u64);

impl RoundNumber {
    /// Wrap a raw round index.
    pub const fn new(round: u64) -> Self {
        Self(round)
    }

    /// The round index as a u64.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// The round index as a usize.
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl Display for RoundNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

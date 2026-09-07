use std::fmt::Display;

#[cfg(feature = "simulations")]
use serde::Serialize;

/// Position of a spool within its group, from zero up to the group size.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "simulations", derive(Serialize))]
pub struct GroupPosition(usize);

impl GroupPosition {
    /// Wrap a raw group position.
    pub const fn new(position: usize) -> Self {
        Self(position)
    }

    /// The position as a usize, for indexing.
    pub const fn as_usize(self) -> usize {
        self.0
    }

    /// The position as a u64, for hashing into a seed or message.
    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }
}

impl Display for GroupPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

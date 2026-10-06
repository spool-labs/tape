/// A track's leaf is stamped with one virtual group and keeps it for life. `GroupFold` gives the live group that carries it.

use crate::erasure::MAX_GROUP_SIZE;
use crate::types::SpoolIndex;
use bytemuck::{Pod, Zeroable};

/// Bits in a virtual group number.
pub const VIRTUAL_GROUP_BITS: u32 = 11;

/// How many virtual groups a leaf can be stamped with, on every network.
/// The store's two-byte spool key has room for this many and no more.
pub const VIRTUAL_GROUP_COUNT: u64 = 1 << VIRTUAL_GROUP_BITS;

/// The most spool groups that can be live. A live group can split only while
/// it carries at least two virtual groups.
pub const MAX_LIVE_GROUPS: u64 = VIRTUAL_GROUP_COUNT / 2;

#[repr(transparent)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, serde::Serialize, serde::Deserialize)]
pub struct VirtualGroup(pub u64);

impl VirtualGroup {
    /// Get the virtual group a stored spool belongs to.
    #[inline]
    pub fn containing(spool: SpoolIndex) -> Self {
        Self(spool.as_u64() / MAX_GROUP_SIZE as u64)
    }

    /// First stored spool of this virtual group.
    #[inline]
    pub fn base_spool(&self) -> SpoolIndex {
        SpoolIndex::from(self.0 * MAX_GROUP_SIZE as u64)
    }

    /// Stored spool for a position within this virtual group.
    #[inline]
    pub fn spool_at(&self, position: usize) -> SpoolIndex {
        assert!(position < MAX_GROUP_SIZE);
        SpoolIndex::from(self.0 * MAX_GROUP_SIZE as u64 + position as u64)
    }

    /// Position within this virtual group for a stored spool, if the spool belongs to it.
    #[inline]
    pub fn position_of(&self, spool: SpoolIndex) -> Option<usize> {
        let spool = spool.as_usize();

        if spool / MAX_GROUP_SIZE != self.as_usize() {
            return None;
        }

        Some(spool % MAX_GROUP_SIZE)
    }

    /// Check if a stored spool belongs to this virtual group.
    #[inline]
    pub fn contains(&self, spool: SpoolIndex) -> bool {
        VirtualGroup::containing(spool) == *self
    }

    /// Unpack from little-endian bytes.
    #[inline]
    pub fn unpack(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    /// Pack into little-endian bytes.
    #[inline]
    pub fn pack(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Returns the inner u64 value.
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Converts the value to usize.
    #[inline]
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    /// Fallibly converts the value to usize.
    #[inline]
    pub fn try_as_usize(&self) -> Result<usize, core::num::TryFromIntError> {
        usize::try_from(self.0)
    }
}

impl From<u64> for VirtualGroup {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<VirtualGroup> for u64 {
    #[inline]
    fn from(g: VirtualGroup) -> u64 {
        g.0
    }
}

impl std::fmt::Debug for VirtualGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "virtual-group:{}", self.0)
    }
}

impl std::fmt::Display for VirtualGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

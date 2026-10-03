/// Group account index. Each group contains GROUP_SIZE contiguous spools.

use crate::erasure::GROUP_SIZE;
use crate::types::SpoolIndex;
use bytemuck::{Pod, Zeroable};

#[repr(transparent)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, serde::Serialize, serde::Deserialize)]
pub struct GroupIndex(pub u64);

impl GroupIndex {
    /// Get the group that owns a given spool.
    #[inline]
    pub fn containing(spool: SpoolIndex) -> Self {
        Self(spool.as_u64() / GROUP_SIZE as u64)
    }

    /// First spool index in this group.
    #[inline]
    pub fn base_spool(&self) -> SpoolIndex {
        SpoolIndex::from(self.0 * GROUP_SIZE as u64)
    }

    /// Global spool index for a position within this group.
    #[inline]
    pub fn spool_at(&self, position: usize) -> SpoolIndex {
        assert!(position < GROUP_SIZE);
        SpoolIndex::from(self.0 * GROUP_SIZE as u64 + position as u64)
    }

    /// Position within this group for a spool, if the spool belongs to this group.
    #[inline]
    pub fn position_of(&self, spool: SpoolIndex) -> Option<usize> {
        let spool = spool.as_usize();

        // Not in this group.
        if spool / GROUP_SIZE != self.as_usize() {
            return None;
        }

        Some(spool % GROUP_SIZE)
    }

    /// Check if a spool belongs to this group.
    #[inline]
    pub fn contains(&self, spool: SpoolIndex) -> bool {
        GroupIndex::containing(spool) == *self
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

impl From<u64> for GroupIndex {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<GroupIndex> for u64 {
    #[inline]
    fn from(g: GroupIndex) -> u64 {
        g.0
    }
}

impl std::fmt::Debug for GroupIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group:{}", self.0)
    }
}

impl std::fmt::Display for GroupIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

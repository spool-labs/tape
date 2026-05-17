/// Spool group index. Each group contains GROUP_SIZE contiguous spools.

use crate::erasure::GROUP_SIZE;
use crate::types::SpoolIndex;
use bytemuck::{Pod, Zeroable};

#[repr(transparent)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, serde::Serialize, serde::Deserialize)]
pub struct SpoolGroup(pub u64);

impl SpoolGroup {
    /// Get the group that owns a given spool.
    #[inline]
    pub fn of(spool: SpoolIndex) -> Self {
        Self(spool.as_u64() / GROUP_SIZE as u64)
    }

    /// First spool index in this group.
    #[inline]
    pub fn base(&self) -> SpoolIndex {
        SpoolIndex::from(self.0 * GROUP_SIZE as u64)
    }

    /// Global spool index for a slice position within this group.
    #[inline]
    pub fn spool_at(&self, slice_in_group: usize) -> SpoolIndex {
        assert!(slice_in_group < GROUP_SIZE);
        SpoolIndex::from(self.0 * GROUP_SIZE as u64 + slice_in_group as u64)
    }

    /// Slice position within this group for a spool, if the spool belongs to this group.
    #[inline]
    pub fn slice_of(&self, spool: SpoolIndex) -> Option<SpoolIndex> {
        let spool = spool.as_usize();

        // Not in this group.
        if spool / GROUP_SIZE != self.0 as usize {
            return None;
        }

        // Slice position within the group.
        Some(SpoolIndex::from((spool % GROUP_SIZE) as u64))
    }

    /// Check if a spool belongs to this group.
    #[inline]
    pub fn contains(&self, spool: SpoolIndex) -> bool {
        SpoolGroup::of(spool) == *self
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
}

impl From<u64> for SpoolGroup {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<SpoolGroup> for u64 {
    #[inline]
    fn from(g: SpoolGroup) -> u64 {
        g.0
    }
}

impl std::fmt::Debug for SpoolGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group:{}", self.0)
    }
}

impl std::fmt::Display for SpoolGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

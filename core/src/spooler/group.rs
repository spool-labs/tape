/// Spool group index (0 to SPOOL_GROUP_COUNT-1).
///
/// Each group contains SPOOL_GROUP_SIZE contiguous spools.

use crate::erasure::{SPOOL_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use super::SpoolIndex;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
#[repr(transparent)]
pub struct SpoolGroup(pub u64);

unsafe impl bytemuck::Zeroable for SpoolGroup {}
unsafe impl bytemuck::Pod for SpoolGroup {}

impl SpoolGroup {
    /// Get the group that owns a given spool.
    #[inline]
    pub fn of(spool: SpoolIndex) -> Self {
        assert!((spool as usize) < SPOOL_COUNT);
        Self(spool as u64 / SPOOL_GROUP_SIZE as u64)
    }

    /// First spool index in this group.
    #[inline]
    pub fn base(&self) -> SpoolIndex {
        assert!((self.0 as usize) < SPOOL_GROUP_COUNT);
        (self.0 as usize * SPOOL_GROUP_SIZE) as SpoolIndex
    }

    /// Global spool index for a slice position within this group.
    #[inline]
    pub fn spool_at(&self, slice_in_group: usize) -> SpoolIndex {
        assert!((self.0 as usize) < SPOOL_GROUP_COUNT);
        assert!(slice_in_group < SPOOL_GROUP_SIZE);
        (self.0 as usize * SPOOL_GROUP_SIZE + slice_in_group) as SpoolIndex
    }

    /// Slice position within this group for a spool, if the spool belongs to this group.
    #[inline]
    pub fn slice_of(&self, spool: SpoolIndex) -> Option<usize> {
        assert!((spool as usize) < SPOOL_COUNT);
        assert!((self.0 as usize) < SPOOL_GROUP_COUNT);
        if SpoolGroup::of(spool) != *self {
            return None;
        }
        Some(spool as usize % SPOOL_GROUP_SIZE)
    }

    /// Check if a spool belongs to this group.
    #[inline]
    pub fn contains(&self, spool: SpoolIndex) -> bool {
        assert!((spool as usize) < SPOOL_COUNT);
        assert!((self.0 as usize) < SPOOL_GROUP_COUNT);
        SpoolGroup::of(spool) == *self
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

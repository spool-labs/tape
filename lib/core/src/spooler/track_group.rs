/// A track is written to one group and its leaf keeps that group for life. `GroupFold` gives the live group that carries it.

use crate::erasure::MAX_GROUP_SIZE;
use crate::types::SpoolIndex;
use bytemuck::{Pod, Zeroable};

use super::GroupIndex;

#[repr(transparent)]
#[cfg_attr(feature = "wincode", derive(wincode_derive::SchemaRead, wincode_derive::SchemaWrite))]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, serde::Serialize, serde::Deserialize)]
pub struct TrackGroup(pub u64);

impl TrackGroup {
    /// Get the track group a stored spool belongs to.
    #[inline]
    pub fn containing(spool: SpoolIndex) -> Self {
        Self(spool.as_u64() / MAX_GROUP_SIZE as u64)
    }

    /// First stored spool of this track group.
    #[inline]
    pub fn base_spool(&self) -> SpoolIndex {
        SpoolIndex::from(self.0 * MAX_GROUP_SIZE as u64)
    }

    /// Stored spool for a position within this track group.
    #[inline]
    pub fn spool_at(&self, position: usize) -> SpoolIndex {
        assert!(position < MAX_GROUP_SIZE);
        SpoolIndex::from(self.0 * MAX_GROUP_SIZE as u64 + position as u64)
    }

    /// Position within this track group for a stored spool, if the spool belongs to it.
    #[inline]
    pub fn position_of(&self, spool: SpoolIndex) -> Option<usize> {
        let spool = spool.as_usize();

        if spool / MAX_GROUP_SIZE != self.as_usize() {
            return None;
        }

        Some(spool % MAX_GROUP_SIZE)
    }

    /// Check if a stored spool belongs to this track group.
    #[inline]
    pub fn contains(&self, spool: SpoolIndex) -> bool {
        TrackGroup::containing(spool) == *self
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

impl From<u64> for TrackGroup {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<TrackGroup> for u64 {
    #[inline]
    fn from(g: TrackGroup) -> u64 {
        g.0
    }
}

/// A live group always carries the track group with its own id.
impl From<GroupIndex> for TrackGroup {
    #[inline]
    fn from(g: GroupIndex) -> Self {
        Self(g.0)
    }
}

impl std::fmt::Debug for TrackGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "track-group:{}", self.0)
    }
}

impl std::fmt::Display for TrackGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

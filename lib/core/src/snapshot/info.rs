#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use bytemuck::Zeroable;

use crate::erasure::SPOOL_GROUP_COUNT;
use crate::spooler::SpoolGroup;
use crate::track::blob::BlobInfo;
use crate::types::{SnapshotGroupBitmap, TrackNumber};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub enum SnapshotStatus {
    Pending,
    Built,
    Initialized,
    PartiallyCertified,
    Finalized,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub enum SnapshotGroupStatus {
    Missing,
    Built,
    CertifiedOnChain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotInfo {
    pub status: SnapshotStatus,
    pub certified_groups: SnapshotGroupBitmap,
    pub groups: [SnapshotGroupInfo; SPOOL_GROUP_COUNT],
}

impl SnapshotInfo {
    pub fn new(status: SnapshotStatus) -> Self {
        Self {
            status,
            certified_groups: SnapshotGroupBitmap::zeroed(),
            groups: [SnapshotGroupInfo::default(); SPOOL_GROUP_COUNT],
        }
    }

    pub fn group(&self, group: SpoolGroup) -> &SnapshotGroupInfo {
        &self.groups[group.0 as usize]
    }

    pub fn group_mut(&mut self, group: SpoolGroup) -> &mut SnapshotGroupInfo {
        &mut self.groups[group.0 as usize]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotGroupInfo {
    pub status: SnapshotGroupStatus,
    pub blob: BlobInfo,
    pub track_number: Option<TrackNumber>,
}

impl Default for SnapshotGroupInfo {
    fn default() -> Self {
        Self {
            status: SnapshotGroupStatus::Missing,
            blob: BlobInfo::zeroed(),
            track_number: None,
        }
    }
}

#[cfg(test)]
#[cfg(feature = "wincode")]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::SPOOL_GROUP_SIZE;
    use crate::track::blob::BlobInfo;
    use crate::types::{StorageUnits, StripeCount};
    use tape_crypto::hash::Hash;

    #[test]
    fn status_variants_exist() {
        let snapshot = SnapshotStatus::Initialized;
        let group = SnapshotGroupStatus::CertifiedOnChain;

        assert_ne!(snapshot, SnapshotStatus::Finalized);
        assert_ne!(group, SnapshotGroupStatus::Missing);
    }

    #[test]
    fn snapshot_info_roundtrip() {
        let mut snapshot = SnapshotInfo::new(SnapshotStatus::PartiallyCertified);
        snapshot.certified_groups.set(3);
        *snapshot.group_mut(SpoolGroup(3)) = SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            blob: BlobInfo {
                size: StorageUnits::from_bytes(4_096),
                commitment: Hash::new_unique(),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(1024),
                stripe_count: StripeCount(4),
                leaves: [Hash::new_unique(); SPOOL_GROUP_SIZE],
            },
            track_number: Some(TrackNumber(7)),
        };

        let bytes = wincode::serialize(&snapshot).unwrap();
        let decoded: SnapshotInfo = wincode::deserialize(&bytes).unwrap();

        assert_eq!(decoded, snapshot);
    }
}

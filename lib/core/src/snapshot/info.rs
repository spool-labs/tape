#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::track::blob::BlobInfo;
use crate::types::{EpochNumber, SnapshotGroupBitmap, TrackNumber};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub enum SnapshotEpochStatus {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotEpochInfo {
    pub parent_epoch: EpochNumber,
    pub status: SnapshotEpochStatus,
    pub certified_groups: SnapshotGroupBitmap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotGroupInfo {
    pub status: SnapshotGroupStatus,
    pub blob: BlobInfo,
    pub track_number: Option<TrackNumber>,
}

#[cfg(test)]
#[cfg(feature = "wincode")]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
    use crate::track::blob::BlobInfo;
    use crate::types::{StorageUnits, StripeCount};
    use tape_crypto::hash::Hash;

    #[test]
    fn status_variants_exist() {
        let epoch = SnapshotEpochStatus::Initialized;
        let group = SnapshotGroupStatus::CertifiedOnChain;

        assert_ne!(epoch, SnapshotEpochStatus::Finalized);
        assert_ne!(group, SnapshotGroupStatus::Missing);
    }

    #[test]
    fn snapshot_info_roundtrip() {
        let epoch = SnapshotEpochInfo {
            parent_epoch: EpochNumber(41),
            status: SnapshotEpochStatus::PartiallyCertified,
            certified_groups: SnapshotGroupBitmap::from_indices(&[0, 2, 4], SPOOL_GROUP_COUNT),
        };

        let group = SnapshotGroupInfo {
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

        let epoch_bytes = wincode::serialize(&epoch).unwrap();
        let group_bytes = wincode::serialize(&group).unwrap();

        let decoded_epoch: SnapshotEpochInfo = wincode::deserialize(&epoch_bytes).unwrap();
        let decoded_group: SnapshotGroupInfo = wincode::deserialize(&group_bytes).unwrap();

        assert_eq!(decoded_epoch, epoch);
        assert_eq!(decoded_group, group);
    }
}

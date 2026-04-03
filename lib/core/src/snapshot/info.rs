use crate::bls::BlsSignature;
use crate::erasure::{MEMBER_COUNT, SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use crate::spooler::SpoolGroup;
use crate::types::{Bitmap, EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use super::chunk::SnapshotChunkMeta;

pub type SnapshotGroupBitmap = Bitmap<{ (SPOOL_GROUP_COUNT + 7) / 8 }>;
pub type CommitteeBitmap = Bitmap<{ (MEMBER_COUNT + 7) / 8 }>;

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
    CertifiedLocally,
    CertifiedOnChain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotEpochInfo {
    pub epoch: EpochNumber,
    pub parent_epoch: EpochNumber,
    pub tape: Address,
    pub status: SnapshotEpochStatus,
    pub certified_groups: SnapshotGroupBitmap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
pub struct SnapshotGroupInfo {
    pub epoch: EpochNumber,
    pub parent_epoch: EpochNumber,
    pub group: SpoolGroup,
    pub status: SnapshotGroupStatus,
    pub meta: SnapshotChunkMeta,
    pub leaves: [Hash; SPOOL_GROUP_SIZE],
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
    pub track: Option<Address>,
    pub track_number: Option<TrackNumber>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "wincode")]
    use crate::encoding::EncodingProfile;
    #[cfg(feature = "wincode")]
    use crate::types::{StorageUnits, StripeCount};
    #[cfg(feature = "wincode")]
    use bytemuck::Zeroable;

    #[test]
    fn status_variants_exist() {
        let epoch = SnapshotEpochStatus::Initialized;
        let group = SnapshotGroupStatus::CertifiedOnChain;

        assert_ne!(epoch, SnapshotEpochStatus::Finalized);
        assert_ne!(group, SnapshotGroupStatus::Missing);
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn snapshot_info_roundtrip() {
        let epoch = SnapshotEpochInfo {
            epoch: EpochNumber(42),
            parent_epoch: EpochNumber(41),
            tape: Address::from([1u8; 32]),
            status: SnapshotEpochStatus::PartiallyCertified,
            certified_groups: SnapshotGroupBitmap::from_indices(&[0, 2, 4], SPOOL_GROUP_COUNT),
        };

        let group = SnapshotGroupInfo {
            epoch: EpochNumber(42),
            parent_epoch: EpochNumber(41),
            group: SpoolGroup(3),
            status: SnapshotGroupStatus::Built,
            meta: SnapshotChunkMeta {
                commitment: Hash::new_unique(),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(1024),
                stripe_count: StripeCount(4),
            },
            leaves: [Hash::new_unique(); SPOOL_GROUP_SIZE],
            bitmap: CommitteeBitmap::from_indices(&[0, 1, 2], MEMBER_COUNT),
            signature: BlsSignature::zeroed(),
            track: Some(Address::from([2u8; 32])),
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

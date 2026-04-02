use bytemuck::{Pod, Zeroable};
use tape_crypto::Hash;
use tape_crypto::hash::{hashv, hash};

use crate::encoding::EncodingProfile;
use crate::spooler::SpoolGroup;
use crate::types::{EpochNumber, StorageUnits, StripeCount};

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";
pub const SNAPSHOT_CHUNK_VALUE_V1: &[u8; 24] = b"SNAPSHOT_CHUNK_VALUE_V1\0";

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotChunkMeta {
    pub commitment: Hash,
    pub profile: EncodingProfile,
    pub stripe_size: StorageUnits,
    pub stripe_count: StripeCount,
}

#[inline]
pub fn snapshot_chunk_key(
    snapshot_epoch: EpochNumber,
    group: SpoolGroup,
    parent_epoch: EpochNumber,
) -> Hash {
    hashv(&[
        SNAPSHOT_KEY_V1,
        &snapshot_epoch.pack(),
        &group.pack(),
        &parent_epoch.pack(),
    ])
}

#[inline]
pub fn snapshot_chunk_value_hash(chunk_meta: &SnapshotChunkMeta) -> Hash {
    hashv(&[SNAPSHOT_CHUNK_VALUE_V1, bytemuck::bytes_of(chunk_meta)])
}

#[inline]
pub fn snapshot_chunk_meta_hash(chunk_meta: &SnapshotChunkMeta) -> Hash {
    hash(bytemuck::bytes_of(chunk_meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_chunk_hashing_is_stable() {
        let chunk_meta = SnapshotChunkMeta {
            commitment: Hash::from([0x11; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
        };

        assert_eq!(
            snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(8)),
            Hash::from([
                246, 60, 132, 78, 80, 231, 72, 231, 197, 74, 20, 46, 122, 240, 187, 3, 185,
                69, 30, 226, 67, 141, 19, 154, 223, 28, 171, 108, 37, 131, 79, 31,
            ]),
        );

        assert_eq!(
            snapshot_chunk_value_hash(&chunk_meta),
            Hash::from([
                99, 152, 138, 33, 214, 128, 127, 141, 89, 20, 227, 243, 203, 56, 198, 244,
                223, 150, 19, 71, 153, 11, 142, 228, 237, 105, 165, 249, 119, 1, 16, 28,
            ]),
        );

        assert_eq!(
            snapshot_chunk_meta_hash(&chunk_meta),
            Hash::from([
                204, 112, 35, 90, 255, 199, 43, 254, 131, 177, 235, 16, 252, 169, 20, 94, 51,
                241, 159, 185, 220, 224, 4, 146, 253, 144, 13, 120, 160, 90, 20, 58,
            ]),
        );
    }
}

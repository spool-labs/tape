#[cfg(test)]
use bytemuck::bytes_of;

use tape_crypto::Hash;
use tape_crypto::hash::{hash, hashv};

use crate::spooler::SpoolGroup;
use crate::types::EpochNumber;

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";

#[inline]
pub fn snapshot_chunk_key(
    epoch: EpochNumber,
    group: SpoolGroup,
    parent_epoch: EpochNumber,
) -> Hash {
    hashv(&[
        SNAPSHOT_KEY_V1,
        &epoch.pack(),
        &group.pack(),
        &parent_epoch.pack(),
    ])
}

#[inline]
pub fn snapshot_chunk_root(chunk: &[u8]) -> Hash {
    hash(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::SPOOL_GROUP_SIZE;
    use crate::track::blob::BlobInfo;
    use crate::types::{StorageUnits, StripeCount};

    #[test]
    fn snapshot_chunk_hashing_is_stable() {
        let chunk = b"snapshot chunk";
        let blob = BlobInfo {
            size: StorageUnits::from_bytes(chunk.len() as u64),
            root: snapshot_chunk_root(chunk),
            commitment: Hash::from([0x11; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x22; 32]); SPOOL_GROUP_SIZE],
        };

        assert_eq!(
            snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(8)),
            Hash::from([
                246, 60, 132, 78, 80, 231, 72, 231, 197, 74, 20, 46, 122, 240, 187, 3, 185,
                69, 30, 226, 67, 141, 19, 154, 223, 28, 171, 108, 37, 131, 79, 31,
            ]),
        );

        assert_eq!(
            snapshot_chunk_root(chunk),
            Hash::from([
                145, 144, 29, 44, 174, 99, 32, 191, 231, 195, 102, 212, 194, 107, 145, 137,
                27, 96, 221, 173, 145, 123, 4, 2, 196, 158, 174, 124, 114, 52, 68, 130,
            ]),
        );

        assert_eq!(
            blob.get_hash(),
            hash(bytes_of(&blob)),
        );
    }
}

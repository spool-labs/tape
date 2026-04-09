use tape_crypto::Hash;
use tape_crypto::hash::hashv;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_chunk_key_is_stable() {
        assert_eq!(
            snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(8)),
            Hash::from([
                246, 60, 132, 78, 80, 231, 72, 231, 197, 74, 20, 46, 122, 240, 187, 3, 185,
                69, 30, 226, 67, 141, 19, 154, 223, 28, 171, 108, 37, 131, 79, 31,
            ]),
        );
    }

    #[test]
    fn snapshot_chunk_key_distinguishes_parent_epoch() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(8));
        let b = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(7));
        assert_ne!(a, b);
    }

    #[test]
    fn snapshot_chunk_key_distinguishes_epoch_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), EpochNumber(8));
        let b = snapshot_chunk_key(EpochNumber(10), SpoolGroup(3), EpochNumber(9));
        assert_ne!(a, b);
    }
}

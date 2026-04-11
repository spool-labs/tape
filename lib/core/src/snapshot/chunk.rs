use tape_crypto::Hash;
use tape_crypto::hash::hashv;

use crate::spooler::SpoolGroup;
use crate::types::EpochNumber;

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";

#[inline]
pub fn snapshot_chunk_key(
    epoch: EpochNumber,
    group: SpoolGroup,
) -> Hash {
    hashv(&[
        SNAPSHOT_KEY_V1,
        &epoch.pack(),
        &group.pack(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_chunk_key_is_stable() {
        assert_eq!(
            snapshot_chunk_key(EpochNumber(9), SpoolGroup(3)),
            Hash::from([
                125, 24, 57, 155, 139, 250, 226, 142, 58, 117, 91, 0, 187, 177, 4, 238, 250,
                249, 96, 33, 110, 127, 162, 61, 185, 15, 118, 134, 76, 233, 26, 123,
            ]),
        );
    }

    #[test]
    fn distinguishes_epoch_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3));
        let b = snapshot_chunk_key(EpochNumber(10), SpoolGroup(3));
        assert_ne!(a, b);
    }
}

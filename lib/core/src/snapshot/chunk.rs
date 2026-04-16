use tape_crypto::Hash;
use tape_crypto::hash::hashv;

use crate::spooler::SpoolGroup;
use crate::types::{ChunkNumber, EpochNumber};

pub const SNAPSHOT_KEY_V1: &[u8; 16] = b"SNAPSHOT_KEY_V1\0";

/// Derives the track key for a snapshot chunk. A single group may contribute multiple chunks
/// per epoch, `chunk_index` is a group-local ordinal that disambiguates them.
#[inline]
pub fn snapshot_chunk_key(
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk_index: ChunkNumber,
) -> Hash {
    hashv(&[
        SNAPSHOT_KEY_V1,
        &epoch.pack(),
        &group.pack(),
        &chunk_index.pack(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_epoch_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(10), SpoolGroup(3), ChunkNumber(0));
        assert_ne!(a, b);
    }

    #[test]
    fn distinguishes_group_pair() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(9), SpoolGroup(4), ChunkNumber(0));
        assert_ne!(a, b);
    }

    #[test]
    fn distinguishes_chunk_index() {
        let a = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), ChunkNumber(0));
        let b = snapshot_chunk_key(EpochNumber(9), SpoolGroup(3), ChunkNumber(1));
        assert_ne!(a, b);
    }
}

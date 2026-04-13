use bytemuck::{bytes_of, bytes_of_mut};
use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::prelude::*;
use tape_core::types::SnapshotGroupBitmap;
use tape_crypto::Hash;
use tape_solana::*;

use super::AccountType;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub epoch: EpochNumber,
    pub group_bitmap: SnapshotGroupBitmap,
    pub chunk_size: StorageUnits,
    pub groups: [SnapshotChunkRecord; SPOOL_GROUP_COUNT],
}

unsafe impl Pod for SnapshotManifest {}
unsafe impl Zeroable for SnapshotManifest {}

tape_solana::state!(AccountType, SnapshotManifest);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotChunkRecord {
    pub value_hash: Hash,
    pub commitment: Hash,
    pub track_number: TrackNumber,
}

pub type PackedSnapshotChunkRecord = [u8; core::mem::size_of::<SnapshotChunkRecord>()];

impl SnapshotChunkRecord {
    pub fn pack(&self) -> PackedSnapshotChunkRecord {
        let mut out = [0u8; core::mem::size_of::<Self>()];
        out.copy_from_slice(bytes_of(self));
        out
    }

    pub fn unpack(data: PackedSnapshotChunkRecord) -> Self {
        let mut value = Self::zeroed();
        bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_chunk_record_pack_roundtrip() {
        let record = SnapshotChunkRecord {
            value_hash: Hash::from([0x10; 32]),
            commitment: Hash::from([0x11; 32]),
            track_number: TrackNumber(7),
        };

        let packed = record.pack();
        let unpacked = SnapshotChunkRecord::unpack(packed);

        assert_eq!(unpacked, record);
    }

    #[test]
    fn snapshot_manifest_pack_roundtrip() {
        let epoch = EpochNumber(1);
        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        group_bitmap.set(3);

        let mut groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];

        groups[3] = SnapshotChunkRecord {
            value_hash: Hash::from([0x21; 32]),
            commitment: Hash::from([0x22; 32]),
            track_number: TrackNumber(5),
        };

        let manifest = SnapshotManifest {
            epoch,
            group_bitmap,
            chunk_size: StorageUnits::from_bytes(3_456),
            groups,
        };

        let packed = manifest.pack();
        let unpacked =
            SnapshotManifest::unpack_with_discriminator(&packed)
            .expect("unpack snapshot manifest");

        assert_eq!(unpacked, &manifest);
        assert_eq!(unpacked.epoch, epoch);
        assert!(unpacked.group_bitmap.is_set(3));
        assert_eq!(unpacked.chunk_size, StorageUnits::from_bytes(3_456));
    }
}

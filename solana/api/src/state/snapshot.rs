use tape_core::erasure::SPOOL_GROUP_COUNT;
use tape_core::prelude::*;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_solana::*;

use super::AccountType;

pub type SnapshotGroupBitmap = Bitmap<{ (SPOOL_GROUP_COUNT + 7) / 8 }>;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotState {
    pub tail_epoch: EpochNumber,
}

tape_solana::state!(AccountType, SnapshotState);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
pub struct SnapshotChunkRecord {
    pub commitment: Hash,
    pub track_number: TrackNumber,
    pub profile: EncodingProfile,
    pub stripe_size: StorageUnits,
    pub stripe_count: StripeCount,
}

pub type PackedSnapshotChunkRecord = [u8; core::mem::size_of::<SnapshotChunkRecord>()];

impl SnapshotChunkRecord {
    pub fn pack(&self) -> PackedSnapshotChunkRecord {
        let mut out = [0u8; core::mem::size_of::<Self>()];
        out.copy_from_slice(bytemuck::bytes_of(self));
        out
    }

    pub fn unpack(data: PackedSnapshotChunkRecord) -> Self {
        let mut value = Self::zeroed();
        bytemuck::bytes_of_mut(&mut value).copy_from_slice(&data);
        value
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub epoch: EpochNumber,
    pub parent_epoch: EpochNumber,
    pub tape: Address,
    pub certified_count: SpoolCount,
    pub group_bitmap: SnapshotGroupBitmap,
    // Explicit padding keeps the account POD-safe before the 8-byte-aligned group array.
    pub reserved: [u8; 7],
    pub groups: [SnapshotChunkRecord; SPOOL_GROUP_COUNT],
}

unsafe impl Zeroable for SnapshotManifest {}
unsafe impl Pod for SnapshotManifest {}

tape_solana::state!(AccountType, SnapshotManifest);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::tapedrive::snapshot_tape_pda;

    #[test]
    fn snapshot_state_pack_roundtrip() {
        let state = SnapshotState {
            tail_epoch: EpochNumber(9),
        };

        let packed = state.pack();
        let unpacked =
            SnapshotState::unpack_with_discriminator(&packed).expect("unpack snapshot state");

        assert_eq!(unpacked, &state);
    }

    #[test]
    fn snapshot_chunk_record_pack_roundtrip() {
        let record = SnapshotChunkRecord {
            commitment: Hash::from([0x11; 32]),
            track_number: TrackNumber(7),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
        };

        let packed = record.pack();
        let unpacked = SnapshotChunkRecord::unpack(packed);

        assert_eq!(unpacked, record);
    }

    #[test]
    fn snapshot_manifest_pack_roundtrip() {
        let (tape, _) = snapshot_tape_pda(EpochNumber(12));
        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        group_bitmap.set(3);

        let mut groups = [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT];
        groups[3] = SnapshotChunkRecord {
            commitment: Hash::from([0x22; 32]),
            track_number: TrackNumber(5),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(4096),
            stripe_count: StripeCount(8),
        };

        let manifest = SnapshotManifest {
            epoch: EpochNumber(12),
            parent_epoch: EpochNumber(11),
            tape,
            certified_count: 1,
            group_bitmap,
            reserved: [0; 7],
            groups,
        };

        let packed = manifest.pack();
        let unpacked =
            SnapshotManifest::unpack_with_discriminator(&packed).expect("unpack snapshot manifest");

        assert_eq!(unpacked, &manifest);
        assert!(unpacked.group_bitmap.is_set(3));
    }
}

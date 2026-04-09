use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};

use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::{BlobInfo, PackedBlobInfo};
use tape_core::track::data::TrackMeta;
use tape_core::track::types::{TrackKind, TrackState};
use tape_core::types::{CommitteeBitmap, EpochNumber};
use tape_crypto::address::Address;
use tape_solana::*;

use crate::errors::TapeError;
use crate::program::tapedrive;
use crate::program::tapedrive::{
    archive_pda, epoch_pda, snapshot_manifest_pda, snapshot_state_pda, snapshot_tape_pda,
    system_pda,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InitSnapshotEpoch {
    pub epoch: [u8; 8],
}

/// Wire layout for `CertifySnapshotGroup`.
///
/// `blob` is stored as `PackedBlobInfo` (a byte array) rather than `BlobInfo`
/// directly so the struct keeps alignment 1. Solana hands instruction data to
/// programs via a slice that starts one byte past the discriminator, and
/// `bytemuck::try_from_bytes` would reject any struct that requires stricter
/// alignment than the slice it is given.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CertifySnapshotGroup {
    pub epoch: [u8; 8],
    pub signing_epoch: [u8; 8],
    pub group: [u8; 8],
    pub blob: PackedBlobInfo,
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
}

unsafe impl Pod for CertifySnapshotGroup {}
unsafe impl Zeroable for CertifySnapshotGroup {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeSnapshotEpoch {
    pub epoch: [u8; 8],
}

pub fn snapshot_blob_from_certification(
    certification: &CertifySnapshotGroup,
) -> Result<BlobInfo, ProgramError> {
    let blob = BlobInfo::unpack(certification.blob);
    if blob.commitment_root() != blob.commitment {
        return Err(TapeError::InvalidCommitment.into());
    }
    Ok(blob)
}

pub fn get_snapshot_track_meta(
    certification: &CertifySnapshotGroup,
) -> Result<TrackMeta, ProgramError> {
    let blob = snapshot_blob_from_certification(certification)?;

    Ok(TrackMeta {
        kind: TrackKind::Blob,
        size: blob.size,
        initial_state: TrackState::Certified,
        value_hash: blob.get_hash(),
    })
}

pub fn build_init_snapshot_epoch_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(epoch);
    let (snapshot_tape_address, _) = snapshot_tape_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new_readonly(snapshot_state_address.into(), false),
            AccountMeta::new(manifest_address.into(), false),
            AccountMeta::new(snapshot_tape_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: InitSnapshotEpoch {
            epoch: epoch.pack(),
        }
        .to_bytes(),
    }
}

pub fn build_certify_snapshot_group_ix(
    fee_payer: Address,
    epoch: EpochNumber,
    signing_epoch: EpochNumber,
    group: SpoolGroup,
    blob: &BlobInfo,
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(epoch);
    let (snapshot_tape_address, _) = snapshot_tape_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new_readonly(snapshot_state_address.into(), false),
            AccountMeta::new(manifest_address.into(), false),
            AccountMeta::new(snapshot_tape_address.into(), false),
        ],
        data: CertifySnapshotGroup {
            epoch: epoch.pack(),
            signing_epoch: signing_epoch.pack(),
            group: group.pack(),
            blob: blob.pack(),
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}

pub fn build_finalize_snapshot_epoch_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (epoch_address, _) = epoch_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(snapshot_state_address.into(), false),
            AccountMeta::new_readonly(manifest_address.into(), false),
        ],
        data: FinalizeSnapshotEpoch {
            epoch: epoch.pack(),
        }
        .to_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instruction::TapeInstruction;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;

    #[test]
    fn snapshot_track_meta_uses_blob_hashing() {
        let leaves = [Hash::from([0x11; 32]); SPOOL_GROUP_SIZE];
        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_537),
            root: Hash::from([0x22; 32]),
            commitment: root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves,
        };
        let certification = CertifySnapshotGroup {
            epoch: EpochNumber(9).pack(),
            signing_epoch: EpochNumber(10).pack(),
            group: SpoolGroup(3).pack(),
            blob: blob.pack(),
            bitmap: CommitteeBitmap::zeroed(),
            signature: BlsSignature::zeroed(),
        };

        let meta = get_snapshot_track_meta(&certification).expect("snapshot track meta");

        assert_eq!(meta.kind, TrackKind::Blob);
        assert_eq!(meta.initial_state, TrackState::Certified);
        assert_eq!(meta.size, blob.size);
        assert_eq!(meta.value_hash, blob.get_hash());
    }

    #[test]
    fn build_init_snapshot_epoch_ix_smoke() {
        let instruction = build_init_snapshot_epoch_ix(Address::new_unique(), EpochNumber(7));

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 9);
        assert_eq!(instruction.data[0], TapeInstruction::InitSnapshotEpoch as u8);
    }

    #[test]
    fn build_certify_snapshot_group_ix_smoke() {
        let leaves = [Hash::from([0x55; 32]); SPOOL_GROUP_SIZE];
        let blob = BlobInfo {
            size: StorageUnits::from_bytes(1_025),
            root: Hash::from([0x33; 32]),
            commitment: root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(3),
            leaves,
        };
        let instruction = build_certify_snapshot_group_ix(
            Address::new_unique(),
            EpochNumber(7),
            EpochNumber(8),
            SpoolGroup(2),
            &blob,
            CommitteeBitmap::zeroed(),
            BlsSignature::zeroed(),
        );

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 6);
        assert_eq!(instruction.data[0], TapeInstruction::CertifySnapshotGroup as u8);
    }

    #[test]
    fn build_finalize_snapshot_epoch_ix_smoke() {
        let instruction = build_finalize_snapshot_epoch_ix(Address::new_unique(), EpochNumber(7));

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 4);
        assert_eq!(instruction.data[0], TapeInstruction::FinalizeSnapshotEpoch as u8);
    }
}

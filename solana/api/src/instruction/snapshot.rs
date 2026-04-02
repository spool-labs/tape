use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};
use solana_program::pubkey::Pubkey;
use tape_core::bls::BlsSignature;
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::{COMMITMENT_TREE_HEIGHT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::chunk::{SnapshotChunkMeta, snapshot_chunk_value_hash};
use tape_core::spooler::SpoolGroup;
use tape_core::track::data::TrackMeta;
use tape_core::track::types::{TrackKind, TrackState};
use tape_core::types::{EpochNumber, StorageUnits, StripeCount};
use tape_crypto::Hash;
use tape_crypto::merkle::root_from_leaf_hashes;
use tape_solana::*;

use crate::errors::TapeError;
use crate::program::tapedrive;
use crate::program::tapedrive::{
    CommitteeBitmap, archive_pda, epoch_pda, snapshot_manifest_pda, snapshot_state_pda,
    snapshot_tape_pda, system_pda,
};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InitSnapshotEpoch {
    pub snapshot_epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifySnapshotGroup {
    pub snapshot_epoch: [u8; 8],
    pub signing_epoch: [u8; 8],
    pub group: [u8; 8],
    pub commitment: Hash,
    pub profile: [u8; 16],
    pub stripe_size: [u8; 8],
    pub stripe_count: [u8; 8],
    pub leaves: [Hash; SPOOL_GROUP_SIZE],
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeSnapshotEpoch {
    pub snapshot_epoch: [u8; 8],
}

pub fn get_snapshot_track_meta(args: &CertifySnapshotGroup) -> Result<TrackMeta, ProgramError> {
    let computed_root = root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&args.leaves);
    if computed_root != args.commitment {
        return Err(TapeError::InvalidCommitment.into());
    }

    let stripe_size = StorageUnits::unpack(args.stripe_size);
    let stripe_count = StripeCount::unpack(args.stripe_count);
    let chunk_meta = SnapshotChunkMeta {
        commitment: args.commitment,
        profile: EncodingProfile::unpack(args.profile),
        stripe_size,
        stripe_count,
    };
    let size = stripe_size
        .checked_mul(StorageUnits::from_bytes(stripe_count.as_u64()))
        .ok_or(ProgramError::ArithmeticOverflow)?;

    Ok(TrackMeta {
        kind: TrackKind::Blob,
        size,
        initial_state: TrackState::Certified,
        value_hash: snapshot_chunk_value_hash(&chunk_meta),
    })
}

pub fn build_init_snapshot_epoch_ix(
    fee_payer: Pubkey,
    snapshot_epoch: EpochNumber,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new_readonly(snapshot_state_address, false),
            AccountMeta::new(manifest_address, false),
            AccountMeta::new(snapshot_tape_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: InitSnapshotEpoch {
            snapshot_epoch: snapshot_epoch.pack(),
        }
        .to_bytes(),
    }
}

pub fn build_certify_snapshot_group_ix(
    fee_payer: Pubkey,
    snapshot_epoch: EpochNumber,
    signing_epoch: EpochNumber,
    group: SpoolGroup,
    commitment: Hash,
    profile: EncodingProfile,
    stripe_size: StorageUnits,
    stripe_count: StripeCount,
    leaves: [Hash; SPOOL_GROUP_SIZE],
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    let (snapshot_tape_address, _) = snapshot_tape_pda(snapshot_epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new_readonly(snapshot_state_address, false),
            AccountMeta::new(manifest_address, false),
            AccountMeta::new(snapshot_tape_address, false),
        ],
        data: CertifySnapshotGroup {
            snapshot_epoch: snapshot_epoch.pack(),
            signing_epoch: signing_epoch.pack(),
            group: group.pack(),
            commitment,
            profile: profile.pack(),
            stripe_size: stripe_size.pack(),
            stripe_count: stripe_count.pack(),
            leaves,
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}

pub fn build_finalize_snapshot_epoch_ix(
    fee_payer: Pubkey,
    snapshot_epoch: EpochNumber,
) -> Instruction {
    let (epoch_address, _) = epoch_pda();
    let (snapshot_state_address, _) = snapshot_state_pda();
    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(snapshot_state_address, false),
            AccountMeta::new_readonly(manifest_address, false),
        ],
        data: FinalizeSnapshotEpoch {
            snapshot_epoch: snapshot_epoch.pack(),
        }
        .to_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instruction::TapeInstruction;

    #[test]
    fn snapshot_track_meta_uses_compact_hashing() {
        let leaves = [Hash::from([0x11; 32]); SPOOL_GROUP_SIZE];
        let args = CertifySnapshotGroup {
            snapshot_epoch: EpochNumber(9).pack(),
            signing_epoch: EpochNumber(10).pack(),
            group: SpoolGroup(3).pack(),
            commitment: root_from_leaf_hashes::<COMMITMENT_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::basic_default().pack(),
            stripe_size: StorageUnits::from_bytes(512).pack(),
            stripe_count: StripeCount(4).pack(),
            leaves,
            bitmap: CommitteeBitmap::zeroed(),
            signature: BlsSignature::zeroed(),
        };

        let meta = get_snapshot_track_meta(&args).expect("snapshot track meta");

        assert_eq!(meta.kind, TrackKind::Blob);
        assert_eq!(meta.initial_state, TrackState::Certified);
        assert_eq!(meta.size, StorageUnits::from_bytes(2048));
        assert_eq!(
            meta.value_hash,
            snapshot_chunk_value_hash(&SnapshotChunkMeta {
                commitment: args.commitment,
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(512),
                stripe_count: StripeCount(4),
            }),
        );
    }

    #[test]
    fn build_init_snapshot_epoch_ix_smoke() {
        let instruction = build_init_snapshot_epoch_ix(Pubkey::new_unique(), EpochNumber(7));

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 9);
        assert_eq!(instruction.data[0], TapeInstruction::InitSnapshotEpoch as u8);
    }

    #[test]
    fn build_certify_snapshot_group_ix_smoke() {
        let instruction = build_certify_snapshot_group_ix(
            Pubkey::new_unique(),
            EpochNumber(7),
            EpochNumber(8),
            SpoolGroup(2),
            Hash::from([0x44; 32]),
            EncodingProfile::basic_default(),
            StorageUnits::from_bytes(512),
            StripeCount(3),
            [Hash::from([0x55; 32]); SPOOL_GROUP_SIZE],
            CommitteeBitmap::zeroed(),
            BlsSignature::zeroed(),
        );

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 6);
        assert_eq!(instruction.data[0], TapeInstruction::CertifySnapshotGroup as u8);
    }

    #[test]
    fn build_finalize_snapshot_epoch_ix_smoke() {
        let instruction = build_finalize_snapshot_epoch_ix(Pubkey::new_unique(), EpochNumber(7));

        assert_eq!(instruction.program_id, tapedrive::ID);
        assert_eq!(instruction.accounts.len(), 4);
        assert_eq!(instruction.data[0], TapeInstruction::FinalizeSnapshotEpoch as u8);
    }
}

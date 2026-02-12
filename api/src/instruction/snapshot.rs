//! Snapshot instruction builders.
//!
//! Instructions for registering and managing epoch snapshot tracks.

use tape_core::prelude::*;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use crate::program::tapedrive::*;
use tape_solana::*;

/// Instruction data for ReserveSnapshotTape.
///
/// Creates the system-owned snapshot tape during network initialization.
/// No fields needed — the tape's capacity and epoch range are set by the processor.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ReserveSnapshotTape {}

/// Instruction data for RegisterSnapshot.
///
/// Registers a snapshot track for a specific epoch and spool group.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterSnapshot {
    /// Epoch this snapshot covers.
    pub epoch: [u8; 8],

    /// Spool group index (0..SPOOL_GROUP_COUNT-1), packed as u64 LE.
    pub spool_group: [u8; 8],

    /// Commitment hash (merkle root over 20 inner slices).
    pub commitment: Hash,

    /// Encoding profile.
    pub profile: [u8; 16],

    /// Stripe size in bytes.
    pub stripe_size: [u8; 8],

    /// Number of stripes.
    pub stripe_count: [u8; 8],

    /// Per-slice commitment leaf hashes.
    pub leaves: [Hash; SPOOL_GROUP_SIZE],
}

/// Instruction data for CertifySnapshot.
///
/// Certifies a snapshot track with an aggregate BLS signature.
#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifySnapshot {
    /// Epoch this snapshot covers.
    pub epoch: [u8; 8],

    /// Bitmap of committee members who signed.
    pub bitmap: CommitteeBitmap,

    /// Aggregated BLS signature.
    pub signature: BlsSignature,
}

/// Build a RegisterSnapshot instruction.
///
/// Registers a snapshot track on the system-owned snapshot tape.
/// The authority is always the system PDA (derived internally).
/// The fee_payer must be a committee member.
pub fn build_register_snapshot_ix(
    fee_payer: Pubkey,
    epoch_number: EpochNumber,
    spool_group: u64,
    commitment: Hash,
    profile: EncodingProfile,
    stripe_size: u64,
    stripe_count: u64,
    leaves: [Hash; SPOOL_GROUP_SIZE],
) -> Instruction {
    let (node_address, _) = node_pda(fee_payer);
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (tape_address, _) = tape_pda(system_address);
    let (track_address, _) = snapshot_pda(epoch_number, commitment);
    let (snapshot_state_address, _) = snapshot_state_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(node_address, false),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),
            AccountMeta::new(snapshot_state_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterSnapshot {
            epoch: epoch_number.pack(),
            spool_group: spool_group.to_le_bytes(),
            commitment,
            profile: profile.pack(),
            stripe_size: stripe_size.to_le_bytes(),
            stripe_count: stripe_count.to_le_bytes(),
            leaves,
        }
        .to_bytes(),
    }
}

/// Build a CertifySnapshot instruction.
///
/// Certifies a snapshot track with an aggregate BLS signature.
/// No authority account needed — the system PDA is derived internally by the processor.
pub fn build_certify_snapshot_ix(
    fee_payer: Pubkey,
    epoch: EpochNumber,
    commitment: Hash,
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (tape_address, _) = tape_pda(system_address);
    let (track_address, _) = snapshot_pda(epoch, commitment);
    let (snapshot_state_address, _) = snapshot_state_pda();

    let epoch = epoch.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new_readonly(tape_address, false),
            AccountMeta::new(track_address, false),
            AccountMeta::new(snapshot_state_address, false),
        ],
        data: CertifySnapshot {
            epoch,
            bitmap,
            signature,
        }
        .to_bytes(),
    }
}

/// Build a ReserveSnapshotTape instruction.
///
/// Creates the system-owned snapshot tape. Called once during network init.
/// The tape is owned by the system PDA; the program uses `invoke_signed`
/// to create it on behalf of the system authority.
pub fn build_reserve_snapshot_tape_ix(fee_payer: Pubkey) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (tape_address, _) = tape_pda(system_address);
    let (snapshot_state_address, _) = snapshot_state_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(snapshot_state_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ReserveSnapshotTape {}.to_bytes(),
    }
}

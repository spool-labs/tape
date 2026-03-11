use tape_core::prelude::*;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_crypto::Hash;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use tape_solana::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterTrack {
    pub key: Hash,
    pub root: Hash,            // Merkle root of original data
    pub commitment: Hash,      // Erasure coding commitment
    pub size: [u8; 8],         // Size in bytes (including parity data)
    pub profile: [u8; 16],     // Packed EncodingProfile
    pub stripe_size: [u8; 8],  // Stripe size in bytes
    pub stripe_count: [u8; 8], // Number of stripes
    pub leaves: [Hash; SPOOL_GROUP_SIZE], // Per-slice commitment leaf hashes
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyTrack {
    pub epoch: [u8; 8],
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateTrack {
    pub epoch: [u8; 8],
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
    pub computed_root: Hash,
}


pub fn build_register_track_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    storage_units: StorageUnits,
    root: Hash,         // Data merkle root
    commitment: Hash,   // Erasure coding root
    key: Hash,          // Track identifier (e.g., file path hash)
    profile: EncodingProfile, // Encoding profile (type + params)
    stripe_size: u64,   // Stripe size in bytes
    stripe_count: u64,  // Number of stripes
    leaves: [Hash; SPOOL_GROUP_SIZE], // Per-slice commitment leaf hashes
) -> Instruction {
    assert!(stripe_size > 0, "stripe_size must be non-zero");
    assert!(stripe_count > 0, "stripe_count must be non-zero");

    let (epoch_address, _) = epoch_pda();
    let (tape_address, _) = tape_pda(authority);
    let (track_address, _) = track_pda(authority, key);

    let size = storage_units.pack();
    let profile = profile.pack();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),

            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterTrack {
            key,
            root,
            commitment,
            size,
            profile,
            stripe_size: stripe_size.to_le_bytes(),
            stripe_count: stripe_count.to_le_bytes(),
            leaves,
        }.to_bytes(),
    }
}

pub fn build_delete_track_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    id: Hash
) -> Instruction {

    let (tape_address, _) = tape_pda(authority);
    let (track_address, _) = track_pda(authority, id);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: DeleteTrack {}.to_bytes(),
    }
}

pub fn build_certify_track_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    id: Hash,
    epoch: EpochNumber,
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (system_address, _) = system_pda();
    let (tape_address, _) = tape_pda(authority);
    let (track_address, _) = track_pda(authority, id);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),
        ],
        data: CertifyTrack {
            epoch: epoch.pack(),
            bitmap,
            signature,
        }.to_bytes(),
    }
}

pub fn build_invalidate_track_ix(
    fee_payer: Pubkey,
    system_address: Pubkey,
    epoch_address: Pubkey,
    tape_address: Pubkey,
    track_address: Pubkey,
    epoch: EpochNumber,
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
    computed_root: Hash,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),

            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new_readonly(tape_address, false),
            AccountMeta::new(track_address, false),
        ],
        data: InvalidateTrack {
            epoch: epoch.pack(),
            bitmap,
            signature,
            computed_root,
        }.to_bytes(),
    }
}

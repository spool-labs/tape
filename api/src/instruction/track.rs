use tape_core::prelude::*;
use crate::program::tapedrive::*;
use tape_solana::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterTrack {
    pub key: Hash,
    pub root: Hash,            // Merkle root of original data
    pub commitment: Hash,      // Erasure coding commitment
    pub size: [u8; 8],         // Size in bytes (including parity data)
    pub profile: EncodingProfile, // Encoding profile (type + params)
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyTrack {
    pub bitmap: CommitteeBitmap,
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateTrack {}


pub fn build_register_track_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    storage_units: StorageUnits,
    root: Hash,         // Data merkle root
    commitment: Hash,   // Erasure coding root
    key: Hash,          // Track identifier (e.g., file path hash)
    profile: EncodingProfile, // Encoding profile (type + params)
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (tape_address, _) = tape_pda(authority);
    let (track_address, _) = track_pda(authority, key);

    let size = storage_units.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),

            AccountMeta::new(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterTrack {
            key,
            root,
            commitment,
            size,
            profile,
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
        program_id: crate::program::tapedrive::ID,
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
    bitmap: CommitteeBitmap,
    signature: BlsSignature,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (system_address, _) = system_pda();
    let (tape_address, _) = tape_pda(authority);
    let (track_address, _) = track_pda(authority, id);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),
        ],
        data: CertifyTrack {
            bitmap,
            signature,
        }.to_bytes(),
    }
}

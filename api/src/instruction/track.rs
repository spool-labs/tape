use core::marker::PhantomData;
use tape_core::prelude::*;
use crate::program::tapedrive::*;
use steel::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterTrack {
    pub id: Hash,
    pub root: Hash,       // Merkle root of original data
    pub commitment: Hash, // Erasure coding commitment
    pub size: [u8; 8],    // Size in bytes (including parity data)
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyTrack {
    pub bitmap: [u8; (MEMBER_COUNT+7) / 8],
    pub signature: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateTrack {}


#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AppendStreamSegment {
    pub size: [u8; 8],
    pub data: PhantomData<[u8]>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UpdateStreamSegment {
    pub segment_number: [u8; 8],
    //pub old_data: [u8; SEGMENT_SIZE],
    //pub new_data: [u8; SEGMENT_SIZE],
    //pub proof: ProofPath,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeStream {}


pub fn build_register_track_ix(
    signer: Pubkey,
    storage_units: StorageUnits,
    root: Hash,         // Data merkle root
    commitment: Hash,   // Erasure coding root
    id: Hash,           // Track identifier (e.g., file path hash)
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (tape_address, _) = tape_pda(signer);
    let (track_address, _) = track_pda(signer, id);

    let size = storage_units.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new(epoch_address, false),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterTrack {
            id,
            root,
            commitment,
            size,
        }.to_bytes(),
    }
}

pub fn build_delete_track_ix(
    signer: Pubkey,
    id: Hash
) -> Instruction {

    let (tape_address, _) = tape_pda(signer);
    let (track_address, _) = track_pda(signer, id);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: DeleteTrack {}.to_bytes(),
    }
}

pub fn build_certify_track_ix(
    signer: Pubkey,
    id: Hash,
    bitmap: [u8; (MEMBER_COUNT+7) / 8],
    signature: BlsSignature,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (system_address, _) = system_pda();
    let (tape_address, _) = tape_pda(signer);
    let (track_address, _) = track_pda(signer, id);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

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

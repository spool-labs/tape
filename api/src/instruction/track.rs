use core::marker::PhantomData;
use tape_core::prelude::*;
use crate::program::tapedrive::*;
use steel::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterTrack {
    pub id: Hash,
    pub kind: [u8; 8],
    pub size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {}

// Blob

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyBlob {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateBlob {}

// Stream

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
    track_kind: TrackKind,
    track_id: Hash,
) -> Instruction {

    let (tape_address, _) = tape_pda(signer);
    let (track_address, _) = track_pda(signer, track_id);

    let size = storage_units.pack();
    let kind = track_kind.pack();
    let id = track_id;

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new(tape_address, false),
            AccountMeta::new(track_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterTrack {
            id,
            kind,
            size,
        }.to_bytes(),
    }
}

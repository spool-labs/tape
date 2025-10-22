use steel::*;
use crate::program::tapedrive::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateArchive {}

pub fn build_create_archive_ix(
    signer: Pubkey,
) -> Instruction {
    let (archive_address, _) = archive_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(archive_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateArchive {}.to_bytes(),
    }
}


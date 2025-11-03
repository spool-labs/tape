use steel::*;
use crate::program::tapedrive::*;
use crate::program::token::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateArchive {}

pub fn build_create_archive_ix(
    signer: Pubkey,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(archive_ata, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateArchive {}.to_bytes(),
    }
}


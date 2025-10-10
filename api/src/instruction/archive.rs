use steel::*;
use crate::pda::*;
use crate::utils::ata;
use tape_core::types::ArchiveNumber;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateArchive {}

pub fn build_create_archive_ix(
    signer: Pubkey,
    archive: ArchiveNumber
) -> Instruction {

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda(archive);
    let (archive_ata, _) = archive_ata(archive_address);
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();

    let signer_ata = ata(&signer);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(archive_address, false),
            AccountMeta::new(archive_ata, false),
            AccountMeta::new(mint_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateArchive {}.to_bytes(),
    }
}


use steel::*;
use crate::pda::*;
use crate::utils::ata;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

pub fn build_initialize_ix(
    signer: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();
    let (mint_address, _) = mint_pda();
    let (metadata_address, _) = metadata_pda();
    let (committee_address, _) = current_committee_pda();
    let (prev_committee_address, _) = previous_committee_pda();

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
            AccountMeta::new(committee_address, false),
            AccountMeta::new(prev_committee_address, false),
            AccountMeta::new(mint_address, false),
            AccountMeta::new(metadata_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Initialize {}.to_bytes(),
    }
}


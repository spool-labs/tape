use steel::*;
use crate::utils::ata;
use crate::program::token::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InitializeMint {}

pub fn build_initialize_mint_ix(
    signer: Pubkey,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (metadata_address, _) = metadata_pda();
    let (treasury_address, _) = treasury_pda();

    let signer_ata = ata(&signer);

    Instruction {
        program_id: crate::program::token::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(mint_address, false),
            AccountMeta::new(metadata_address, false),
            AccountMeta::new(treasury_address, false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(mpl_token_metadata::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: InitializeMint {}.to_bytes(),
    }
}


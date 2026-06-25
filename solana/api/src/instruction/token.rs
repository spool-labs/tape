use tape_solana::*;
use tape_crypto::address::Address;
use crate::utils::ata;
use crate::program::metaplex;
use crate::program::token;
use crate::program::token::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InitializeMint {}

pub fn build_initialize_mint_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (metadata_address, _) = metadata_pda();
    let (treasury_address, _) = treasury_pda();

    let authority_ata = ata(&authority);

    Instruction {
        program_id: token::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new(mint_address.into(), false),
            AccountMeta::new(metadata_address.into(), false),
            AccountMeta::new(treasury_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(metaplex::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: InitializeMint {}.to_bytes(),
    }
}

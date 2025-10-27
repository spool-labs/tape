use crate::declare_id;
use solana_program::pubkey::Pubkey;
use super::token::MINT_ADDRESS;

declare_id!("taQ4ccnpwKHP9SxPxda76YrwxhDwsCMYg8vjf6KRiNh"); 

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };

pub const VAULT: &[u8] = b"vault";

#[inline(always)]
pub fn vault_pda(stake: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[VAULT, stake.as_ref()], &id())
}

#[inline(always)]
pub fn vault_ata(vault: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            vault.as_ref(),
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        &spl_associated_token_account::ID,
    )
}

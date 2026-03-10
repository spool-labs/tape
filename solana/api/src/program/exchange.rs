use solana_program::pubkey::Pubkey;
use super::token::MINT_ADDRESS;

tape_solana::declare_id!("taAfD9hTjxpiVUSjTNx5ezKT6CXW9W2Ya4ky1RMev5f"); 

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };

pub const EXCHANGE: &[u8] = b"exchange";

#[inline(always)]
pub fn exchange_pda(authority: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EXCHANGE, authority.as_ref()], &id())
}

#[inline(always)]
pub fn exchange_ata(exchange: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            exchange.as_ref(), 
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        &spl_associated_token_account::ID,
    )
}

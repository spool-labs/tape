use solana_program::pubkey::Pubkey;
use tape_crypto::address::Address;

use super::token::MINT_ADDRESS;

tape_solana::declare_id!("c8KFakci8uCzkFbkPu1xX2AwX5bPfvCyrfgzMkZBipp");

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };

pub const EXCHANGE: &[u8] = b"exchange";

#[inline(always)]
pub fn exchange_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[EXCHANGE, authority.as_ref()], id())
}

#[inline(always)]
pub fn exchange_ata(exchange: Address) -> (Address, u8) {
    Address::find_program_address(
        &[
            exchange.as_ref(), 
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        spl_associated_token_account::ID,
    )
}

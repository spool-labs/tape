use crate::declare_id;
use solana_program::pubkey::Pubkey;

declare_id!("taQ4ccnpwKHP9SxPxda76YrwxhDwsCMYg8vjf6KRiNh"); 

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };

pub const STAKE: &[u8] = b"stake";

#[inline(always)]
pub fn stake_ata(authority: Pubkey, pool: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[STAKE, authority.as_ref(), pool.as_ref()], &id())
}

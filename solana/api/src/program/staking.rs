use solana_program::pubkey::Pubkey;
use tape_crypto::address::Address;

tape_solana::declare_id!("G97Xa3SirZfQRdJfVEB5qW5GL1TVnT1frHPZdd1N5iMi");

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };

pub const VAULT: &[u8] = b"vault";

#[inline(always)]
pub fn vault_pda(stake: Address) -> (Address, u8) {
    Address::find_program_address(&[VAULT, stake.as_ref()], id())
}

use solana_program::pubkey::Pubkey;
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;

use crate::state::System;
use crate::consts::NAME_LENGTH;
use crate::program::token;

use spl_associated_token_account::get_associated_token_address;

/// Helper: convert a slice to a fixed-size array, truncating or padding with zeros as needed
#[inline(always)]
pub fn padded_array<const N: usize>(input: &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    let len = input.len().min(N);
    out[..len].copy_from_slice(&input[..len]);
    out
}

/// Helper: convert a name to a fixed-size array
#[inline(always)]
pub fn to_name<T>(val: T) -> [u8; NAME_LENGTH]
where
    T: AsRef<[u8]>,
{
    let bytes = val.as_ref();

    assert!(
        bytes.len() <= NAME_LENGTH,
        "name too long ({} > {})",
        bytes.len(),
        NAME_LENGTH
    );

    padded_array::<NAME_LENGTH>(bytes)
}

/// Helper: convert a name to a string
#[inline(always)]
pub fn from_name(val: &[u8; NAME_LENGTH]) -> String {
    let mut name_bytes = val.to_vec();
    name_bytes.retain(|&x| x != 0);
    String::from_utf8_lossy(&name_bytes).into_owned()
}

/// Helper: get the current epoch from the System account.
#[inline(always)]
pub fn current_epoch(system: &System) -> EpochNumber {
    system.current_epoch
}

/// Helper: get the next epoch from the System account.
#[inline(always)]
pub fn next_epoch(system: &System) -> EpochNumber {
    system.current_epoch.next()
}

/// Helper: get the previous epoch from the System account.
#[inline(always)]
pub fn prev_epoch(system: &System) -> EpochNumber {
    system.current_epoch.prev()
}

/// Helper: get the associated token account
#[inline(always)]
pub fn ata(owner: &Address) -> Address {
    let owner_pubkey: Pubkey = owner.into();
    let mint_address: Pubkey = token::MINT_ADDRESS.into();

    get_associated_token_address(&owner_pubkey, &mint_address).into()
}

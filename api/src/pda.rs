use crate::consts::*;
use tape_core::prelude::*;
use steel::*;

pub fn system_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[SYSTEM], &crate::id())
}

pub fn archive_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[ARCHIVE], &crate::id())
}

pub fn epoch_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EPOCH], &crate::id())
}

pub fn pool_pda(authority: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[POOL, authority.as_ref()], &crate::id())
}

pub fn stake_pda(authority: Pubkey, pool: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[STAKE, authority.as_ref(), pool.as_ref()], &crate::id())
}

pub fn blob_pda(authority: Pubkey, hash: Hash) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[BLOB, authority.as_ref(), hash.as_ref()], &crate::id())
}

pub fn treasury_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[TREASURY], &crate::id())
}

pub fn treasury_ata() -> (Pubkey, u8) {
    let (treasury_pda, _bump) = treasury_pda();
    let (mint_pda, _bump) = mint_pda();
    Pubkey::find_program_address(
        &[
            treasury_pda.as_ref(),
            spl_token::ID.as_ref(),
            mint_pda.as_ref(),
        ],
        &spl_associated_token_account::ID,
    )
}

pub fn mint_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[MINT, MINT_SEED], &crate::id())
}

pub fn metadata_pda(mint: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[METADATA, mpl_token_metadata::ID.as_ref(), mint.as_ref()],
        &mpl_token_metadata::ID,
    )
}


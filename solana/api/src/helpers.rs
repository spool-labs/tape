//! Helper functions for building common instruction patterns.
//!
//! These helpers are used for operations that need to set up token accounts
//! for ephemeral authority keypairs (e.g., staking with a unique authority).

use solana_program::{
    instruction::Instruction,
    program_error::ProgramError,
    pubkey::Pubkey,
};
use spl_associated_token_account::instruction::create_associated_token_account;
use spl_token::instruction::{close_account, transfer_checked};
use tape_core::types::coin::{Coin, TAPE};
use tape_crypto::address::Address;

use crate::program::token::{MINT_ADDRESS, TOKEN_DECIMALS};
use crate::utils::ata;

/// Build instructions to create an ATA for the authority and transfer TAPE tokens.
///
/// Returns two instructions:
/// 1. Create the associated token account for the authority
/// 2. Transfer TAPE tokens from fee_payer's ATA to authority's ATA
///
/// # Arguments
/// * `fee_payer` - The account paying for ATA creation and providing TAPE (must sign)
/// * `authority` - The new authority that will own the ATA and receive TAPE
/// * `amount` - Amount of TAPE tokens to transfer
pub fn build_authority_with_tokens_ix(
    fee_payer: Address,
    authority: Address,
    amount: Coin<TAPE>,
) -> Result<Vec<Instruction>, ProgramError> {
    let fee_payer_pubkey: Pubkey = fee_payer.into();
    let authority_pubkey: Pubkey = authority.into();
    let mint_address: Pubkey = MINT_ADDRESS.into();
    let fee_payer_ata = ata(&fee_payer);
    let authority_ata = ata(&authority);
    let fee_payer_ata_pubkey: Pubkey = fee_payer_ata.into();
    let authority_ata_pubkey: Pubkey = authority_ata.into();

    Ok(vec![
        // Create ATA for authority (fee_payer pays rent)
        create_associated_token_account(
            &fee_payer_pubkey,
            &authority_pubkey,
            &mint_address,
            &spl_token::ID,
        ),

        // Transfer TAPE from fee_payer's ATA to authority's ATA
        transfer_checked(
            &spl_token::ID,
            &fee_payer_ata_pubkey,
            &mint_address,
            &authority_ata_pubkey,
            &fee_payer_pubkey,
            &[],
            amount.as_u64(),
            TOKEN_DECIMALS,
        )?,
    ])
}

/// Build an instruction to close an empty ATA and reclaim rent.
///
/// The ATA must have zero token balance to be closed.
/// Rent is returned to the destination account.
///
/// # Arguments
/// * `authority` - The owner of the ATA (must sign)
/// * `destination` - The account that will receive the reclaimed rent
pub fn build_close_ata_ix(
    authority: Address,
    destination: Address,
) -> Result<Instruction, ProgramError> {
    let authority_ata = ata(&authority);
    let authority_ata_pubkey: Pubkey = authority_ata.into();
    let destination_pubkey: Pubkey = destination.into();
    let authority_pubkey: Pubkey = authority.into();

    close_account(
        &spl_token::ID,
        &authority_ata_pubkey,
        &destination_pubkey,
        &authority_pubkey,
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_authority_with_tokens_ix() {
        let fee_payer = Address::new_unique();
        let authority = Address::new_unique();
        let amount = TAPE(1_000_000);

        let ixs = build_authority_with_tokens_ix(fee_payer, authority, amount)
            .expect("build authority with tokens instructions");

        assert_eq!(ixs.len(), 2);
        // First instruction creates ATA
        assert_eq!(ixs[0].program_id, spl_associated_token_account::ID);
        // Second instruction transfers tokens
        assert_eq!(ixs[1].program_id, spl_token::ID);
    }

    #[test]
    fn test_build_close_ata_ix() {
        let authority = Address::new_unique();
        let destination = Address::new_unique();

        let ix = build_close_ata_ix(authority, destination).expect("build close ATA instruction");

        assert_eq!(ix.program_id, spl_token::ID);
    }
}

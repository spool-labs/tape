//! Helper functions for building common instruction patterns.
//!
//! These helpers are used for operations that need to set up token accounts
//! for ephemeral authority keypairs (e.g., staking with a unique authority).

use solana_program::{
    instruction::Instruction,
    pubkey::Pubkey,
    system_instruction,
};
use spl_associated_token_account::instruction::create_associated_token_account;
use spl_token::instruction::{close_account, transfer_checked};
use tape_core::types::coin::{Coin, TAPE};

use crate::program::token::MINT_ADDRESS;
use crate::utils::ata;

/// Token decimals for TAPE.
const TAPE_DECIMALS: u8 = 6;

/// Build an instruction to transfer SOL to a new authority account.
///
/// This is used to fund ephemeral authority keypairs with enough SOL for rent.
///
/// # Arguments
/// * `fee_payer` - The account paying for the transfer (must sign)
/// * `authority` - The new authority that will receive the SOL
/// * `lamports` - Amount of lamports to transfer
pub fn build_authority_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    lamports: u64,
) -> Instruction {
    system_instruction::transfer(&fee_payer, &authority, lamports)
}

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
    fee_payer: Pubkey,
    authority: Pubkey,
    amount: Coin<TAPE>,
) -> Vec<Instruction> {
    let fee_payer_ata = ata(&fee_payer);
    let authority_ata = ata(&authority);

    vec![
        // Create ATA for authority (fee_payer pays rent)
        create_associated_token_account(
            &fee_payer,
            &authority,
            &MINT_ADDRESS,
            &spl_token::ID,
        ),
        // Transfer TAPE from fee_payer's ATA to authority's ATA
        transfer_checked(
            &spl_token::ID,
            &fee_payer_ata,
            &MINT_ADDRESS,
            &authority_ata,
            &fee_payer,
            &[],
            amount.as_u64(),
            TAPE_DECIMALS,
        )
        .unwrap(),
    ]
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
    authority: Pubkey,
    destination: Pubkey,
) -> Instruction {
    let authority_ata = ata(&authority);

    close_account(
        &spl_token::ID,
        &authority_ata,
        &destination,
        &authority,
        &[],
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_authority_with_tokens_ix() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let amount = TAPE(1_000_000);

        let ixs = build_authority_with_tokens_ix(fee_payer, authority, amount);

        assert_eq!(ixs.len(), 2);
        // First instruction creates ATA
        assert_eq!(ixs[0].program_id, spl_associated_token_account::ID);
        // Second instruction transfers tokens
        assert_eq!(ixs[1].program_id, spl_token::ID);
    }

    #[test]
    fn test_build_close_ata_ix() {
        let authority = Pubkey::new_unique();
        let destination = Pubkey::new_unique();

        let ix = build_close_ata_ix(authority, destination);

        assert_eq!(ix.program_id, spl_token::ID);
    }
}

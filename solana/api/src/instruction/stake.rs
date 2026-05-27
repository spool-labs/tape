use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::types::coin::{Coin, TAPE};
use crate::program::staking;
use crate::utils::ata;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeTokens {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UnstakeTokens {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitStake {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergeStake {}


pub fn build_stake_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    amount: Coin<TAPE>,
) -> Instruction {

    let (mint_address, _)  = mint_pda();
    let (stake_address, _) = stake_pda(authority);
    let (vault_address, _) = vault_pda(stake_address);
    let authority_ata      = ata(&authority);

    Instruction {
        program_id: staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new_readonly(pool.into(), false),
            AccountMeta::new(vault_address.into(), false),

            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: StakeTokens { amount }.to_bytes(),
    }
}

pub fn build_unstake_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {

    let (stake_address, _) = stake_pda(authority);
    let (vault_address, _) = vault_pda(stake_address);
    let authority_ata      = ata(&authority);

    Instruction {
        program_id: staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),  // writable: receives vault rent refund
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new(vault_address.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: UnstakeTokens {}.to_bytes(),
    }
}

pub fn build_split_stake_ix(
    fee_payer: Address,
    authority: Address,
    recipient: Address,
    amount: Coin<TAPE>,
) -> Instruction {

    // Source (authority) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(authority);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    let (mint_address, _) = mint_pda();

    Instruction {
        program_id: staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(recipient.into(), true),

            AccountMeta::new(source_vault_address.into(), false),
            AccountMeta::new(dest_vault_address.into(), false),

            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: SplitStake { amount }.to_bytes(),
    }
}

pub fn build_merge_stake_ix(
    fee_payer: Address,
    authority: Address,
    recipient: Address,
) -> Instruction {

    // Source (donor) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(authority);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    Instruction {
        program_id: staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),  // writable: receives vault rent refund
            AccountMeta::new_readonly(recipient.into(), true),

            AccountMeta::new(source_vault_address.into(), false),
            AccountMeta::new(dest_vault_address.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: MergeStake {}.to_bytes(),
    }
}

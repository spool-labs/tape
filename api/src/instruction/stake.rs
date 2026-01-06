use steel::*;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeTokens {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UnstakeTokens {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitStake {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergeStake {}


pub fn build_stake_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    pool: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (mint_address, _)  = mint_pda();
    let (stake_address, _) = stake_pda(authority, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let authority_ata      = ata(&authority);

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(authority_ata, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(vault_address, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: StakeTokens { amount }.to_bytes(),
    }
}

pub fn build_unstake_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let (stake_address, _) = stake_pda(authority, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let authority_ata      = ata(&authority);

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new(authority, true),  // writable: receives vault rent refund
            AccountMeta::new(authority_ata, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(vault_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: UnstakeTokens {}.to_bytes(),
    }
}

pub fn build_split_stake_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    pool: Pubkey,
    recipient: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    // Source (authority) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(authority, pool);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient, pool);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    let (mint_address, _) = mint_pda();

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new_readonly(recipient, true),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_vault_address, false),
            AccountMeta::new(dest_vault_address, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: SplitStake { amount }.to_bytes(),
    }
}

pub fn build_merge_stake_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    pool: Pubkey,
    recipient: Pubkey,
) -> Instruction {

    // Source (donor) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(authority, pool);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient, pool);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new(authority, true),  // writable: receives vault rent refund
            AccountMeta::new_readonly(recipient, true),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_vault_address, false),
            AccountMeta::new(dest_vault_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: MergeStake {}.to_bytes(),
    }
}

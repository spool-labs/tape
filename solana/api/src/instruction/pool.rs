use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::staking::PoolRate;
use tape_core::types::EpochNumber;
use tape_core::types::coin::{Coin, TAPE};
use crate::program::{staking, tapedrive};
use crate::utils::ata;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvancePool {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeWithPool {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RequestStakeUnlock {
    pub rate: PoolRate,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UnstakeFromPool {
    pub rate: PoolRate,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitPoolStake {
    pub amount: Coin<TAPE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergePoolStake {}

pub fn build_advance_pool_ix(
    fee_payer: Address,
    pool: Address,
    current_epoch: EpochNumber,
) -> Instruction {
    let prev = current_epoch.prev();

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (prev_epoch_address, _) = epoch_pda(prev);
    let (prev_committee_address, _) = committee_pda(prev);
    let (history_address, _) = history_pda(pool);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new_readonly(prev_epoch_address.into(), false),
            AccountMeta::new_readonly(prev_committee_address.into(), false),
            AccountMeta::new(pool.into(), false),
            AccountMeta::new(history_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: AdvancePool {}.to_bytes(),
    }
}

pub fn build_stake_with_pool_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    amount: Coin<TAPE>,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (mint_address, _)   = mint_pda();
    let (stake_address, _)  = stake_pda(authority);
    let (vault_address, _)  = vault_pda(stake_address);
    let authority_ata       = ata(&authority);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(pool.into(), false),
            AccountMeta::new(stake_address.into(), false),
            AccountMeta::new(vault_address.into(), false),

            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(staking::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: StakeWithPool { amount }.to_bytes(),
    }
}

pub fn build_request_stake_unlock_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    pool_rate: PoolRate,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (stake_address, _) = stake_pda(authority);
    let (history_address, _) = history_pda(pool);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new(stake_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(pool.into(), false),
            AccountMeta::new_readonly(history_address.into(), false),
        ],
        data: RequestStakeUnlock { rate: pool_rate }.to_bytes(),
    }
}


pub fn build_unstake_from_pool_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    pool_rate: PoolRate,
) -> Instruction {
    let authority_ata        = ata(&authority);
    let (archive_address, _) = archive_pda();
    let (archive_ata, _)     = archive_ata();
    let (system_address, _)  = system_pda();
    let (stake_address, _)   = stake_pda(authority);
    let (vault_address, _)   = vault_pda(stake_address);
    let (history_address, _) = history_pda(pool);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),  // writable: receives vault rent refund via CPI
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new_readonly(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),

            AccountMeta::new(stake_address.into(), false),
            AccountMeta::new(vault_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(pool.into(), false),
            AccountMeta::new_readonly(history_address.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(staking::ID, false),
        ],
        data: UnstakeFromPool { rate: pool_rate }.to_bytes(),
    }
}

pub fn build_split_pool_stake_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    recipient: Address,
    amount: Coin<TAPE>,
) -> Instruction {
    let (source_stake, _) = stake_pda(authority);
    let (dest_stake, _)   = stake_pda(recipient);

    let (source_vault, _) = vault_pda(source_stake);
    let (dest_vault, _)   = vault_pda(dest_stake);
    let (mint_address, _) = mint_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(recipient.into(), true),

            AccountMeta::new_readonly(pool.into(), false),
            AccountMeta::new(source_stake.into(), false),
            AccountMeta::new(dest_stake.into(), false),
            AccountMeta::new(source_vault.into(), false),
            AccountMeta::new(dest_vault.into(), false),

            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(staking::ID, false),
        ],
        data: SplitPoolStake { amount }.to_bytes(),
    }
}

pub fn build_merge_pool_stake_ix(
    fee_payer: Address,
    authority: Address,
    pool: Address,
    recipient: Address,
) -> Instruction {
    let (source_stake, _) = stake_pda(authority);
    let (dest_stake, _)   = stake_pda(recipient);

    let (source_vault, _) = vault_pda(source_stake);
    let (dest_vault, _)   = vault_pda(dest_stake);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(authority.into(), true),  // writable: receives vault rent refund via CPI
            AccountMeta::new_readonly(recipient.into(), true),

            AccountMeta::new_readonly(pool.into(), false),
            AccountMeta::new(source_stake.into(), false),
            AccountMeta::new(dest_stake.into(), false),
            AccountMeta::new(source_vault.into(), false),
            AccountMeta::new(dest_vault.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(staking::ID, false),
        ],
        data: MergePoolStake {}.to_bytes(),
    }
}

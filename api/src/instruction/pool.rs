use steel::*;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AdvancePool {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeWithPool {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RequestStakeUnlock {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UnstakeFromPool {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SplitPoolStake {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct MergePoolStake {}

pub fn build_advance_pool_ix(
    signer: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _)  = epoch_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new_readonly(archive_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(pool, false),
        ],
        data: AdvancePool { }.to_bytes(),
    }
}

pub fn build_stake_with_pool_ix(
    signer: Pubkey,
    pool: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (epoch_address, _)  = epoch_pda();
    let (mint_address, _)   = mint_pda();
    let (stake_address, _)  = stake_pda(signer, pool);
    let (vault_address, _)  = vault_pda(stake_address);
    let signer_ata          = ata(&signer);

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(pool, false),
            AccountMeta::new(stake_address, false),
            AccountMeta::new(vault_address, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(crate::program::staking::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: StakeWithPool { amount }.to_bytes(),
    }
}

pub fn build_request_stake_unlock_ix(
    signer: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (stake_address, _) = stake_pda(signer, pool);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new(stake_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(pool, false),
        ],
        data: RequestStakeUnlock {}.to_bytes(),
    }
}


pub fn build_unstake_from_pool_ix(
    signer: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let signer_ata         = ata(&signer);
    let (epoch_address, _) = epoch_pda();
    let (stake_address, _) = stake_pda(signer, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let pool_ata           = ata(&pool);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(stake_address, false),
            AccountMeta::new(vault_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(pool, false),
            AccountMeta::new(pool_ata, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(crate::program::staking::ID, false),
        ],
        data: UnstakeFromPool {}.to_bytes(),
    }
}

pub fn build_split_pool_stake_ix(
    signer: Pubkey,
    pool: Pubkey,
    recipient: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {
    let (source_stake, _) = stake_pda(signer, pool);
    let (dest_stake, _)   = stake_pda(recipient, pool);

    let (source_vault, _) = vault_pda(source_stake);
    let (dest_vault, _)   = vault_pda(dest_stake);
    let (mint_address, _) = mint_pda();

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(recipient, true),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_stake, false),
            AccountMeta::new(dest_stake, false),
            AccountMeta::new(source_vault, false),
            AccountMeta::new(dest_vault, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(crate::program::staking::ID, false),
        ],
        data: SplitPoolStake { amount }.to_bytes(),
    }
}

pub fn build_merge_pool_stake_ix(
    signer: Pubkey,
    pool: Pubkey,
    recipient: Pubkey,
) -> Instruction {
    let (source_stake, _) = stake_pda(signer, pool);
    let (dest_stake, _)   = stake_pda(recipient, pool);

    let (source_vault, _) = vault_pda(source_stake);
    let (dest_vault, _)   = vault_pda(dest_stake);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(recipient, true),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_stake, false),
            AccountMeta::new(dest_stake, false),
            AccountMeta::new(source_vault, false),
            AccountMeta::new(dest_vault, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(crate::program::staking::ID, false),
        ],
        data: MergePoolStake {}.to_bytes(),
    }
}

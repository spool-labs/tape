use steel::*;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeWithPool {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RequestStakeWithdraw {}

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


pub fn build_stake_with_pool_ix(
    signer: Pubkey,
    pool: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (epoch_address, _) = epoch_pda();
    let (mint_address, _)  = mint_pda();
    let (stake_address, _) = stake_pda(signer, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let signer_ata         = ata(&signer);

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(stake_address, false),
            AccountMeta::new(vault_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new(pool, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(crate::program::staking::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: StakeWithPool { amount }.to_bytes(),
    }
}

pub fn build_unstake_from_pool_ix(
    signer: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let (stake_address, _) = stake_pda(signer, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let signer_ata         = ata(&signer);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(vault_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
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

    // Source (signer) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(signer, pool);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient, pool);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    let (mint_address, _) = mint_pda();

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(recipient, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_vault_address, false),
            AccountMeta::new(dest_vault_address, false),

            AccountMeta::new_readonly(mint_address, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: SplitPoolStake { amount }.to_bytes(),
    }
}

pub fn build_merge_pool_stake_ix(
    signer: Pubkey,
    pool: Pubkey,
    recipient: Pubkey,
) -> Instruction {

    // Source (donor) stake/vault token PDA
    let (source_stake_address, _) = stake_pda(signer, pool);
    let (source_vault_address, _) = vault_pda(source_stake_address);

    // Destination (recipient) stake/vault token PDA
    let (dest_stake_address, _)   = stake_pda(recipient, pool);
    let (dest_vault_address, _)   = vault_pda(dest_stake_address);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(recipient, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(source_vault_address, false),
            AccountMeta::new(dest_vault_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: MergePoolStake {}.to_bytes(),
    }
}


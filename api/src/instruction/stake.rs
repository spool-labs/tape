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
    signer: Pubkey,
    pool: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (mint_address, _) = mint_pda();
    let (stake_address, _) = stake_pda(signer, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let vault_ata = ata(&vault_address);
    let signer_ata = ata(&signer);

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(vault_address, false),
            AccountMeta::new(vault_ata, false),
            AccountMeta::new_readonly(mint_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: StakeTokens {
            amount
        }.to_bytes(),
    }
}

pub fn build_unstake_ix(
    signer: Pubkey,
    pool: Pubkey,
) -> Instruction {

    let (stake_address, _) = stake_pda(signer, pool);
    let (vault_address, _) = vault_pda(stake_address);
    let vault_ata = ata(&vault_address);
    let signer_ata = ata(&signer);

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new_readonly(pool, false),
            AccountMeta::new(vault_address, false),
            AccountMeta::new(vault_ata, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: UnstakeTokens {
        }.to_bytes(),
    }
}

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
    node_address: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let signer_ata = ata(&signer);
    let (stake_ata, _) = stake_ata(signer, node_address);
    let (mint_address, _) = mint_pda();

    let amount = amount.pack();

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(node_address, false),
            AccountMeta::new(stake_ata, false),
            AccountMeta::new_readonly(mint_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
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
    node_address: Pubkey,
) -> Instruction {

    let signer_ata = ata(&signer);
    let (stake_ata, _) = stake_ata(signer, node_address);
    let (mint_address, _) = mint_pda();

    Instruction {
        program_id: crate::program::staking::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),

            AccountMeta::new(node_address, false),
            AccountMeta::new(stake_ata, false),
            AccountMeta::new_readonly(mint_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: UnstakeTokens {
        }.to_bytes(),
    }
}

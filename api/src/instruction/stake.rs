use steel::*;
use crate::pda::*;
use crate::utils::ata;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StakeWithNode {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UnstakeFromNode {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ClaimStake {}

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

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (mint_address, _) = mint_pda();
    let (stake_address, _) = stake_pda(signer, node_address);
    let stake_ata = ata(&stake_address);
    let signer_ata = ata(&signer);

    let amount = amount.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(signer_ata, false),
            AccountMeta::new(stake_address, false),
            AccountMeta::new(stake_ata, false),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),
            AccountMeta::new(mint_address, false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: StakeWithNode {
            amount
        }.to_bytes(),
    }
}



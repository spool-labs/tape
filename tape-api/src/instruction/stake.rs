use steel::*;
use crate::pda::*;
use crate::types::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum StakeInstruction {
    Stake = 0x50,
    Unstake,
    Claim,
    Split,
    Merge,
}

instruction!(StakeInstruction, Stake);
instruction!(StakeInstruction, Unstake);
instruction!(StakeInstruction, Claim);
instruction!(StakeInstruction, Split);
instruction!(StakeInstruction, Merge);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Stake {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unstake {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Claim {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Split {
    pub amount: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Merge {}


pub fn build_stake_ix(
    signer: Pubkey,
    pool_address: Pubkey,
    amount: Coin<TAPE>,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (stake_address, _) = stake_pda(signer, pool_address);

    let amount = amount.pack();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(pool_address, false),
            AccountMeta::new(stake_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Stake {
            amount
        }.to_bytes(),
    }
}


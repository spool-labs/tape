use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::{committee_pda, epoch_pda, system_pda};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateCommittee {
    pub epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ResizeCommittee {}

pub fn build_create_committee_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (committee_address, _) = committee_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateCommittee { epoch }.to_bytes(),
    }
}

pub fn build_resize_committee_ix(
    fee_payer: Address,
    current_epoch: EpochNumber,
) -> Instruction {
    let (system_address, _) = system_pda();
    let next_epoch = current_epoch.next();
    let target_epoch = current_epoch.saturating_add(EpochNumber(2));
    let (current_epoch_address, _) = epoch_pda(current_epoch);
    let (next_epoch_address, _) = epoch_pda(next_epoch);
    let (target_epoch_address, _) = epoch_pda(target_epoch);
    let (committee_address, _) = committee_pda(target_epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(current_epoch_address.into(), false),
            AccountMeta::new_readonly(next_epoch_address.into(), false),
            AccountMeta::new_readonly(target_epoch_address.into(), false),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ResizeCommittee {}.to_bytes(),
    }
}

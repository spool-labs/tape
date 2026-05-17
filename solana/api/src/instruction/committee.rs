use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};
use tape_core::types::EpochNumber;
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::{committee_pda, system_pda};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateCommittee {
    pub epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ResizeCommittee {
    pub epoch: [u8; 8],
}

pub fn build_create_committee_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (committee_address, _) = committee_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateCommittee { epoch: epoch.pack() }.to_bytes(),
    }
}

pub fn build_resize_committee_ix(
    fee_payer: Address,
    epoch: EpochNumber,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (committee_address, _) = committee_pda(epoch);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ResizeCommittee { epoch: epoch.pack() }.to_bytes(),
    }
}

use steel::*;
use crate::pda::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateCommittee {
    pub epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ExpandCommittee {
    pub epoch: [u8; 8],
}

pub fn build_create_committee(
    signer: Pubkey,
    epoch: EpochNumber,
) -> Instruction {
    let (committee_address, _) = committee_pda(epoch);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(committee_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateCommittee {
            epoch: epoch.pack(),
        }.to_bytes(),
    }
}

pub fn build_expand_committee_ix(
    signer: Pubkey,
    epoch: EpochNumber,
) -> Instruction {
    let (committee_address, _) = committee_pda(epoch);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(committee_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ExpandCommittee {
            epoch: epoch.pack(),
        }.to_bytes(),
    }
}

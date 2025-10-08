use steel::*;
use crate::pda::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateCommittee {
    pub id: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ExpandCommittee {
    pub id: [u8; 8],
}

pub fn build_create_committee_ix(
    signer: Pubkey,
    committee: CommitteeNumber,
) -> Instruction {
    assert!(committee.is_valid());

    let (committee_address, _) = committee_pda(committee);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(committee_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateCommittee {
            id: committee.pack(),
        }.to_bytes(),
    }
}

pub fn build_expand_committee_ix(
    signer: Pubkey,
    committee: CommitteeNumber,
) -> Instruction {
    assert!(committee.is_valid());

    let (committee_address, _) = committee_pda(committee);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(committee_address, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ExpandCommittee {
            id: committee.pack(),
        }.to_bytes(),
    }
}

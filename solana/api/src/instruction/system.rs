use tape_core::spooler::GroupIndex;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_core::types::coin::{Coin, TAPE};
use tape_solana::*;
use tape_crypto::address::Address;

use crate::program::tapedrive;
use crate::program::tapedrive::{
    archive_ata, archive_pda, committee_pda, epoch_pda, group_pda, peer_set_pda,
    snapshot_tape_pda, subsidy_ata, subsidy_pda, system_pda,
};
use crate::program::token::mint_pda;
use crate::utils::ata;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateSystem {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateArchive {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StageGenesisNode {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct StartNetwork {
    /// Genesis committee size.
    pub committee_size: [u8; 8],

    /// Genesis spool group count.
    pub spool_groups: [u8; 8],

    /// TAPE flux units transferred into the subsidy vault.
    pub subsidy_amount: [u8; 8],

    /// Initial storage payment burn rate in basis points.
    pub burn_fee_bps: [u8; 8],

    /// Initial subsidy decay rate in basis points.
    pub subsidy_decay_bps: [u8; 8],
}


pub fn build_create_system_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(system_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateSystem {}.to_bytes(),
    }
}

pub fn build_create_archive_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();
    let (subsidy_address, _) = subsidy_pda();
    let (subsidy_ata, _) = subsidy_ata();
    let (peer_set_address, _) = peer_set_pda();
    let (mint_address, _) = mint_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),
            AccountMeta::new_readonly(subsidy_address.into(), false),
            AccountMeta::new(subsidy_ata.into(), false),
            AccountMeta::new(peer_set_address.into(), false),

            AccountMeta::new_readonly(mint_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(spl_associated_token_account::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateArchive {}.to_bytes(),
    }
}


pub fn build_stage_genesis_node_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda(EpochNumber(1));
    let (committee_address, _) = committee_pda(EpochNumber(1));
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(epoch_address.into(), false),
            AccountMeta::new(committee_address.into(), false),
            AccountMeta::new(peer_set_address.into(), false),
            AccountMeta::new_readonly(node_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: StageGenesisNode {}.to_bytes(),
    }
}

pub fn build_start_network_ix(
    fee_payer: Address,
    subsidy_authority: Address,
    committee_size: u64,
    spool_groups: u64,
    subsidy_amount: Coin<TAPE>,
    burn_fee_bps: BasisPoints,
    subsidy_decay_bps: BasisPoints,
) -> Instruction {
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (epoch_address, _) = epoch_pda(EpochNumber(1));
    let (committee_address, _) = committee_pda(EpochNumber(1));
    let (candidate_epoch_address, _) = epoch_pda(EpochNumber(2));
    let (candidate_committee_address, _) = committee_pda(EpochNumber(2));
    let (peer_set_address, _) = peer_set_pda();
    let (group_address, _) = group_pda(EpochNumber(1), GroupIndex(0));
    let (snapshot_tape_address, _) = snapshot_tape_pda(EpochNumber(0));
    let (subsidy_address, _) = subsidy_pda();
    let (subsidy_ata, _) = subsidy_ata();
    let subsidy_authority_ata = ata(&subsidy_authority);

    let accounts = vec![
        AccountMeta::new(fee_payer.into(), true),
        AccountMeta::new_readonly(subsidy_authority.into(), true),
        AccountMeta::new(subsidy_authority_ata.into(), false),

        AccountMeta::new(system_address.into(), false),
        AccountMeta::new(archive_address.into(), false),
        AccountMeta::new(epoch_address.into(), false),
        AccountMeta::new(committee_address.into(), false),
        AccountMeta::new(candidate_epoch_address.into(), false),
        AccountMeta::new(candidate_committee_address.into(), false),
        AccountMeta::new(peer_set_address.into(), false),
        AccountMeta::new(group_address.into(), false),
        AccountMeta::new(snapshot_tape_address.into(), false),
        AccountMeta::new_readonly(subsidy_address.into(), false),
        AccountMeta::new(subsidy_ata.into(), false),
        AccountMeta::new_readonly(spl_token::ID, false),
        AccountMeta::new_readonly(system_program::ID, false),
        AccountMeta::new_readonly(sysvar::rent::ID, false),
    ];

    Instruction {
        program_id: tapedrive::ID,
        accounts,
        data: StartNetwork {
            committee_size: committee_size.to_le_bytes(),
            spool_groups: spool_groups.to_le_bytes(),
            subsidy_amount: subsidy_amount.pack(),
            burn_fee_bps: burn_fee_bps.pack(),
            subsidy_decay_bps: subsidy_decay_bps.pack(),
        }.to_bytes(),
    }
}

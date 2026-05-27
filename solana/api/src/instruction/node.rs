use tape_solana::*;
use tape_crypto::address::Address;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use crate::consts::NAME_LENGTH;
use crate::utils::to_name;
use crate::utils::ata;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::types::{GroupIndex, SpoolIndex};
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::system::NodePreferences;
use tape_core::types::{BasisPoints, EpochDuration, EpochNumber, StorageUnits, VersionId};
use tape_core::types::coin::{Coin, TAPE};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterNode {
    pub name: [u8; NAME_LENGTH],
    pub commission_rate: [u8; 8],
    pub network_address: NetworkAddress,
    pub network_tls: NetworkTlsPubkey,
    pub bls_pubkey: BlsPubkey,
    pub bls_pop: BlsSignature,
    pub preferences: NodePreferences,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct JoinCommittee {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SyncSpool {
    pub spool: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetAuthority {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetNetworkAddress {
    pub network_address: NetworkAddress,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetNetworkTls {
    pub network_tls: NetworkTlsPubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetBlsPubkey {
    pub bls_pubkey: BlsPubkey,
    pub bls_pop: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetName {
    pub name: [u8; NAME_LENGTH],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetStoragePrice {
    pub price: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetBurnFeeBps {
    pub burn_fee_bps: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetSubsidyDecayBps {
    pub subsidy_decay_bps: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetEpochDuration {
    pub epoch_duration: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetStorageCapacity {
    pub size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetCommissionRate {
    pub commission_rate: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetCommitteeSize {
    pub committee_size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetSpoolGroups {
    pub spool_groups: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetMinVersion {
    pub min_version: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ClaimCommission {}


pub fn build_register_node_ix(
    fee_payer: Address,
    authority: Address,
    name: [u8; NAME_LENGTH],
    commission_rate: BasisPoints,
    network_address: NetworkAddress,
    network_tls: NetworkTlsPubkey,
    bls_pubkey: BlsPubkey,
    bls_pop: BlsSignature,
    preferences: NodePreferences,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (node_address, _) = node_pda(authority);
    let (history_address, _) = history_pda(node_address);
    let (blacklist_address, _) = blacklist_pda(node_address);

    let commission_rate = commission_rate.pack();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new(system_address.into(), false),
            AccountMeta::new(node_address.into(), false),
            AccountMeta::new(history_address.into(), false),
            AccountMeta::new(blacklist_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: RegisterNode {
            name,
            commission_rate,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
            preferences,
        }.to_bytes(),
    }
}

pub fn build_join_committee_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    current_epoch: EpochNumber,
) -> Instruction {

    let (system_address, _) = system_pda();
    let next_epoch = current_epoch.next();
    let (curr_epoch_address, _) = epoch_pda(current_epoch);
    let (curr_committee_address, _) = committee_pda(current_epoch);
    let (next_committee_address, _) = committee_pda(next_epoch);
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(curr_epoch_address.into(), false),
            AccountMeta::new_readonly(curr_committee_address.into(), false),
            AccountMeta::new(next_committee_address.into(), false),
            AccountMeta::new(peer_set_address.into(), false),
            AccountMeta::new(node_address.into(), false),
        ],
        data: JoinCommittee {}.to_bytes(),
    }
}

pub fn build_sync_spool_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    epoch: EpochNumber,
    group: GroupIndex,
    spool: SpoolIndex,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda(epoch);
    let (group_address, _) = group_pda(epoch, group);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(epoch_address.into(), false),
            AccountMeta::new(group_address.into(), false),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SyncSpool {
            spool: spool.pack(),
        }.to_bytes(),
    }
}

pub fn build_set_authority_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    new_authority: Address,
) -> Instruction {

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(new_authority.into(), false),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetAuthority {}.to_bytes(),
    }
}

pub fn build_set_bls_pubkey_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    bls_pubkey: BlsPubkey,
    bls_pop: BlsSignature,
) -> Instruction {
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
            AccountMeta::new(peer_set_address.into(), false),
        ],
        data: SetBlsPubkey {
            bls_pubkey,
            bls_pop,
        }.to_bytes(),
    }
}

pub fn build_set_name_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    name: &str,
) -> Instruction {
    let name = to_name(&name);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetName {
            name,
        }.to_bytes(),
    }
}

pub fn build_set_network_address_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    network_address: NetworkAddress,
) -> Instruction {

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetNetworkAddress {
            network_address,
        }.to_bytes(),
    }
}

pub fn build_set_network_tls_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    network_tls: NetworkTlsPubkey,
) -> Instruction {

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetNetworkTls {
            network_tls,
        }.to_bytes(),
    }
}

pub fn build_set_commission_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    commission_rate: BasisPoints,
) -> Instruction {
    let commission_rate = commission_rate.pack();

    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
        ],
        data: SetCommissionRate {
            commission_rate,
        }.to_bytes(),
    }
}

pub fn build_claim_commission_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
) -> Instruction {

    let authority_ata        = ata(&authority);
    let (archive_address, _) = archive_pda();
    let (archive_ata, _)     = archive_ata();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),

            AccountMeta::new(node_address.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: ClaimCommission {}.to_bytes(),
    }
}

pub fn build_set_storage_capacity_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    size: StorageUnits,
) -> Instruction {
    let size = size.pack();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetStorageCapacity {
            size,
        }.to_bytes(),
    }
}

pub fn build_set_storage_price_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    price: Coin<TAPE>,
) -> Instruction {
    let price = price.pack();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetStoragePrice {
            price,
        }.to_bytes(),
    }
}

pub fn build_set_burn_fee_bps_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    burn_fee_bps: BasisPoints,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetBurnFeeBps {
            burn_fee_bps: burn_fee_bps.pack(),
        }.to_bytes(),
    }
}

pub fn build_set_subsidy_decay_bps_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    subsidy_decay_bps: BasisPoints,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetSubsidyDecayBps {
            subsidy_decay_bps: subsidy_decay_bps.pack(),
        }.to_bytes(),
    }
}

pub fn build_set_epoch_duration_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    epoch_duration: EpochDuration,
) -> Instruction {
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
        ],
        data: SetEpochDuration {
            epoch_duration: epoch_duration.pack(),
        }.to_bytes(),
    }
}

pub fn build_set_committee_size_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    committee_size: u64,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetCommitteeSize {
            committee_size: committee_size.to_le_bytes(),
        }.to_bytes(),
    }
}

pub fn build_set_spool_groups_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    spool_groups: u64,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetSpoolGroups {
            spool_groups: spool_groups.to_le_bytes(),
        }.to_bytes(),
    }
}

pub fn build_set_min_version_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    min_version: VersionId,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: SetMinVersion {
            min_version: min_version.pack(),
        }.to_bytes(),
    }
}

use steel::*;
use crate::program::tapedrive::*;
use crate::consts::NAME_LENGTH;
use crate::utils::to_name;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterNode {
    pub name: [u8; NAME_LENGTH],
    pub commission_rate: [u8; 8],
    pub network_address: NetworkAddress,
    pub network_tls: Pubkey,
    pub bls_pubkey: BlsPubkey,
    pub bls_pop: BlsSignature,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct JoinNetwork {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SyncEpoch {
    pub epoch: [u8; 8],
    pub seats: Hash,
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
    pub network_tls: Pubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetName {
    pub name: [u8; NAME_LENGTH],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetCommissionRate {
    pub commission_rate: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ClaimCommission {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteOnStoragePrice {
    pub price: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteOnShardSize {
    pub size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct VoteOnFeature {}


pub fn build_register_node_ix(
    signer: Pubkey,
    name: [u8; NAME_LENGTH],
    commission_rate: BasisPoints,
    network_address: NetworkAddress,
    network_tls: Pubkey,
    bls_pubkey: BlsPubkey,
    bls_pop: BlsSignature,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (node_address, _) = node_pda(signer);

    let commission_rate = commission_rate.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),

            AccountMeta::new(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),

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
        }.to_bytes(),
    }
}

pub fn build_join_network_ix(
    signer: Pubkey,
    node_address: Pubkey,
) -> Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),
            AccountMeta::new_readonly(node_address, false),
        ],
        data: JoinNetwork {}.to_bytes(),
    }
}

pub fn build_epoch_sync_ix(
    signer: Pubkey,
    node_address: Pubkey,
    epoch: EpochNumber,
    seats: &[SeatIndex],
) ->Instruction {

    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    let epoch = epoch.pack();
    let seats = get_seat_hash(seats);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(system_address, false),
            AccountMeta::new(epoch_address, false),
            AccountMeta::new(node_address, false),
        ],
        data: SyncEpoch {
            epoch,
            seats,
        }.to_bytes(),
    }
}

pub fn build_set_authority_ix(
    signer: Pubkey,
    node_address: Pubkey,
    new_authority: Pubkey,
) -> Instruction {

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new_readonly(new_authority, false),
            AccountMeta::new(node_address, false),
        ],
        data: SetAuthority {}.to_bytes(),
    }
}

pub fn build_set_network_address_ix(
    signer: Pubkey,
    node_address: Pubkey,
    network_address: NetworkAddress,
) -> Instruction {

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
        ],
        data: SetNetworkAddress {
            network_address,
        }.to_bytes(),
    }
}

pub fn build_set_network_tls_ix(
    signer: Pubkey,
    node_address: Pubkey,
    network_tls: Pubkey,
) -> Instruction {

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
        ],
        data: SetNetworkTls {
            network_tls,
        }.to_bytes(),
    }
}


pub fn build_set_name_ix(
    signer: Pubkey,
    node_address: Pubkey,
    name: &str,
) -> Instruction {
    let name = to_name(&name);

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
        ],
        data: SetName {
            name,
        }.to_bytes(),
    }
}

pub fn build_set_commission_ix(
    signer: Pubkey,
    node_address: Pubkey,
    commission_rate: BasisPoints,
) -> Instruction {
    let commission_rate = commission_rate.pack();

    let (epoch_address, _) = epoch_pda();
    let (system_address, _) = system_pda();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
            AccountMeta::new(system_address, false),
            AccountMeta::new_readonly(epoch_address, false),

        ],
        data: SetCommissionRate {
            commission_rate,
        }.to_bytes(),
    }
}

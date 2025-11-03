use steel::*;
use crate::program::tapedrive::*;
use crate::consts::NAME_LENGTH;
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
) -> Instruction {
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();
    let (node_address, _) = node_pda(signer);

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

use steel::*;
use crate::pda::*;
use crate::{consts::*, types::*,};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum PoolInstruction {
    Register = 0x20,
    Unregister,

    SetAuthority,
    SetNetworkAddress,
    SetNetworkTls,
    SetName,

    SetCommissionRate,
    ClaimCommission,
}

instruction!(PoolInstruction, Register);
instruction!(PoolInstruction, Unregister);
instruction!(PoolInstruction, SetAuthority);
instruction!(PoolInstruction, SetNetworkAddress);
instruction!(PoolInstruction, SetNetworkTls);
instruction!(PoolInstruction, SetName);
instruction!(PoolInstruction, SetCommissionRate);
instruction!(PoolInstruction, ClaimCommission);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Register {
    pub name: [u8; NAME_LENGTH],
    pub commission_rate: BasisPoints,
    pub network_address: NetworkAddress,
    pub network_tls: Pubkey,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unregister {}

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
    pub commission_rate: BasisPoints,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ClaimCommission {}


pub fn build_register_ix(
    signer: Pubkey,
    name: [u8; NAME_LENGTH],
    commission_rate: BasisPoints,
    network_address: NetworkAddress,
    network_tls: Pubkey,
) -> Instruction {

    let (epoch_pda, _epoch_bump) = epoch_pda();
    let (pool_pda, _pool_bump) = pool_pda(signer);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(epoch_pda, false),
            AccountMeta::new(pool_pda, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Register {
            name,
            commission_rate,
            network_address,
            network_tls,
        }.to_bytes(),
    }
}


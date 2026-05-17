use bytemuck::{Pod, Zeroable};
use solana_program::instruction::{AccountMeta, Instruction};
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::{peer_set_pda, system_pda};

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreatePeerSet {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ResizePeerSet {}

pub fn build_create_peer_set_ix(fee_payer: Address) -> Instruction {
    let (system_address, _) = system_pda();
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(peer_set_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreatePeerSet {}.to_bytes(),
    }
}

pub fn build_resize_peer_set_ix(fee_payer: Address) -> Instruction {
    let (system_address, _) = system_pda();
    let (peer_set_address, _) = peer_set_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(peer_set_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ResizePeerSet {}.to_bytes(),
    }
}

use tape_core::prelude::*;
use tape_core::system::BlacklistEntry;
use tape_core::track::types::CompressedTrackProof;
use tape_crypto::address::Address;
use tape_solana::*;

use crate::program::tapedrive;
use crate::program::tapedrive::*;
use crate::utils::ata;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateBlacklist {
    pub capacity: [u8; 8],
    pub expiry_epoch: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist {
    pub entry: BlacklistEntry,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {
    pub track: CompressedTrackProof,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DestroyBlacklist {}

pub fn build_create_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
    capacity: u64,
    expiry_epoch: EpochNumber,
) -> Instruction {
    let authority_ata = ata(&authority);
    let (blacklist_address, _) = blacklist_pda(node);
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),
            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: CreateBlacklist {
            capacity: capacity.to_le_bytes(),
            expiry_epoch: expiry_epoch.pack(),
        }
        .to_bytes(),
    }
}

pub fn build_add_to_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
    entry: BlacklistEntry,
) -> Instruction {
    let (blacklist_address, _) = blacklist_pda(node);
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: AddToBlacklist { entry }.to_bytes(),
    }
}

pub fn build_remove_from_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
    track: CompressedTrackProof,
) -> Instruction {
    let (blacklist_address, _) = blacklist_pda(node);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
        ],
        data: RemoveFromBlacklist { track }.to_bytes(),
    }
}

pub fn build_destroy_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node: Address,
) -> Instruction {
    let (blacklist_address, _) = blacklist_pda(node);
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new_readonly(node.into(), false),
            AccountMeta::new(blacklist_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DestroyBlacklist {}.to_bytes(),
    }
}

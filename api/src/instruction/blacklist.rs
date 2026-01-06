use tape_solana::*;
use crate::program::tapedrive::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {
    pub index: [u8; 8],
    pub size: [u8; 8],
    pub hash: Hash,
    pub proof: [Hash; BLACKLIST_SIZE]
}

pub fn build_add_to_blacklist_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    node_address: Pubkey,
    track_address: Pubkey,
) -> Instruction {

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(node_address, false),
            AccountMeta::new_readonly(track_address, false),
        ],
        data: AddToBlacklist {}.to_bytes(),
    }
}

pub fn build_remove_from_blacklist_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    node_address: Pubkey,
    index: u64,
    hash: Hash,
    size: StorageUnits,
    proof: [Hash; BLACKLIST_SIZE],
) -> Instruction {

    let size = size.pack();
    let index = index.to_le_bytes();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(node_address, false),
        ],
        data: RemoveFromBlacklist {
            index,
            hash,
            size,
            proof,
        }.to_bytes(),
    }
}


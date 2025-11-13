use steel::*;
use crate::program::tapedrive::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {
    pub hash: Hash,
    pub size: [u8; 8],
    pub proof: [Hash; BLACKLIST_SIZE]
}

pub fn build_add_to_blacklist_ix(
    signer: Pubkey,
    node_address: Pubkey,
    track_address: Pubkey,
) -> Instruction {

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
            AccountMeta::new_readonly(track_address, false),
        ],
        data: AddToBlacklist {}.to_bytes(),
    }
}

pub fn build_remove_from_blacklist_ix(
    signer: Pubkey,
    node_address: Pubkey,
    hash: Hash,
    size: StorageUnits,
    proof: [Hash; BLACKLIST_SIZE],
) -> Instruction {

    let size = size.pack();

    Instruction {
        program_id: crate::program::tapedrive::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(node_address, false),
        ],
        data: RemoveFromBlacklist {
            hash,
            size,
            proof,
        }.to_bytes(),
    }
}


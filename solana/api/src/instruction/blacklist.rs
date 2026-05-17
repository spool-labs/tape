use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::track::types::CompressedTrackProof;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use tape_core::prelude::*;
use tape_crypto::Hash;
use crate::helpers::read_instruction_pod;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AddToBlacklist(pub CompressedTrackProof);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RemoveFromBlacklist {
    pub index: [u8; 8],
    pub size: [u8; 8],
    pub hash: Hash,
    pub proof: [Hash; BLACKLIST_SIZE]
}

#[inline(always)]
pub fn parse_add_to_blacklist(data: &[u8]) -> Result<AddToBlacklist, ProgramError> {
    read_instruction_pod::<AddToBlacklist>(data)
}

pub fn build_add_to_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    proof: CompressedTrackProof,
) -> Instruction {

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
            AccountMeta::new_readonly(proof.state.tape.into(), false),
        ],
        data: AddToBlacklist(proof).to_bytes(),
    }
}

pub fn build_remove_from_blacklist_ix(
    fee_payer: Address,
    authority: Address,
    node_address: Address,
    index: u64,
    hash: Hash,
    size: StorageUnits,
    proof: [Hash; BLACKLIST_SIZE],
) -> Instruction {

    let size = size.pack();
    let index = index.to_le_bytes();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(node_address.into(), false),
        ],
        data: RemoveFromBlacklist {
            index,
            hash,
            size,
            proof,
        }.to_bytes(),
    }
}

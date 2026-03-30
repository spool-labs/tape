use tape_solana::*;
use tape_core::track::types::CompressedTrackProof;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use tape_core::prelude::*;
use tape_crypto::Hash;
use core::mem::size_of;

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

#[inline(always)]
fn read_instruction_pod<T>(data: &[u8]) -> Result<T, ProgramError>
where
    T: bytemuck::Pod + bytemuck::Zeroable,
{
    if data.len() != size_of::<T>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut value = T::zeroed();
    bytemuck::bytes_of_mut(&mut value).copy_from_slice(data);
    Ok(value)
}

pub fn build_add_to_blacklist_ix(
    fee_payer: Pubkey,
    authority: Pubkey,
    node_address: Pubkey,
    proof: CompressedTrackProof,
) -> Instruction {

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer, true),
            AccountMeta::new_readonly(authority, true),
            AccountMeta::new(node_address, false),
            AccountMeta::new_readonly(proof.state.tape, false),
        ],
        data: AddToBlacklist(proof).to_bytes(),
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
        program_id: tapedrive::ID,
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

use steel::*;
use crate::{
    consts::*,
    pda::*
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum BinInstruction {
    Create = 0x40,   // Create a bin to store tapes
    Destroy,         // Destroy a bin, returning the rent to the miner
    Pack,            // Pack a tape into the bin
    Unpack,          // Unpack a tape from the bin
    Commit,          // Commit a solution for mining
}

instruction!(BinInstruction, Create);
instruction!(BinInstruction, Destroy);
instruction!(BinInstruction, Pack);
instruction!(BinInstruction, Unpack);
instruction!(BinInstruction, Commit);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Create {
    pub number: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Destroy {
    pub number: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Pack {
    pub value: [u8; 32]
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unpack {
    pub index: [u8; 8],
    pub proof: [[u8; 32]; TAPE_PROOF_LEN],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Commit {
    pub proof: [[u8; 32]; SEGMENT_PROOF_LEN],
}

pub fn build_create_ix(
    signer: Pubkey, 
    miner_address: Pubkey, 
    number: u64,
) -> Instruction {
    let (bin_address, _bump) = bin_pda(miner_address, number);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new(bin_address, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Create {
            number: number.to_le_bytes(),
        }.to_bytes(),
    }
}

pub fn build_destroy_ix(
    signer: Pubkey, 
    miner_address: Pubkey, 
    number: u64,
) -> Instruction {
    let (bin_address, _bump) = bin_pda(miner_address, number);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new(bin_address, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
        ],
        data: Destroy {
            number: number.to_le_bytes(),
        }.to_bytes(),
    }
}

pub fn build_pack_ix(
    signer: Pubkey, 
    miner_address: Pubkey, 
    bin_address: Pubkey,
    value: [u8; 32], // packed tape value to store
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new(bin_address, false),
        ],
        data: Pack {
            value,
        }.to_bytes(),
    }
}

pub fn build_unpack_ix(
    signer: Pubkey, 
    miner_address: Pubkey, 
    bin_address: Pubkey,
    index: [u8; 8],                    // index of the value to unpack
    proof: [[u8; 32]; TAPE_PROOF_LEN], // proof of the value
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new(bin_address, false),
        ],
        data: Unpack {
            index,
            proof,
        }.to_bytes(),
    }
}

pub fn build_commit_ix(
    signer: Pubkey, 
    miner_address: Pubkey, 
    bin_address: Pubkey,
    proof: [[u8; 32]; SEGMENT_PROOF_LEN],
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(miner_address, false),
            AccountMeta::new(bin_address, false),
        ],
        data: Commit {
            proof,
        }.to_bytes(),
    }
}

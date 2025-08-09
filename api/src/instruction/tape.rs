use steel::*;
use crate::{
    consts::*,
    pda::*,
    types::*,
    utils,
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Create = 0x10,  // Create a new tape account
    Write,          // Create a write head that can be used to write to the tape
    Update,         // Update a segment of the tape
    Finalize,       // Finalize the tape, making it immutable, ready for mining
    SetHeader,      // Set the opque header of the tape
    Subsidize,      // Incentivize miners to store the tape on tapenet
}

instruction!(TapeInstruction, Create);
instruction!(TapeInstruction, Write);
instruction!(TapeInstruction, Update);
instruction!(TapeInstruction, Finalize);
instruction!(TapeInstruction, SetHeader);
instruction!(TapeInstruction, Subsidize);

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Create {
    pub name: [u8; NAME_LEN],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Write {
    // Phantom Vec<u8> to ensure the size is dynamic
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Update {
    pub segment_number: [u8; 8],
    pub old_data: [u8; SEGMENT_SIZE],
    pub new_data: [u8; SEGMENT_SIZE],
    pub proof: ProofPath,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Finalize {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetHeader {
    pub header: [u8; HEADER_SIZE],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Subsidize {
    pub amount: [u8; 8],
}


pub fn build_create_ix(
    signer: Pubkey,
    name: &str,
) -> Instruction {
    let name = utils::to_name(name);

    let (tape_address, _tape_bump) = tape_pda(signer, &name);
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(writer_address, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
            AccountMeta::new_readonly(sysvar::slot_hashes::ID, false),
        ],
        data: Create {
            name,
        }.to_bytes(),
    }
}

pub fn build_set_header_ix(
    signer: Pubkey,
    tape: Pubkey,
    header: &[u8; HEADER_SIZE],
) -> Instruction {
    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
        ],
        data: SetHeader {
            header: *header,
        }.to_bytes(),
    }
}

pub fn build_write_ix(
    signer: Pubkey,
    tape: Pubkey,
    writer: Pubkey,
    data: &[u8],
) -> Instruction {

    let mut ix_data = Write{}.to_bytes();
    ix_data.extend_from_slice(data);

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
        ],
        data: ix_data,
    }
}

pub fn build_update_ix(
    signer: Pubkey,
    tape: Pubkey,
    writer: Pubkey,
    segment_number: u64,
    old_data: [u8; SEGMENT_SIZE],
    new_data: [u8; SEGMENT_SIZE],
    proof: ProofPath,
) -> Instruction {

    let segment_number = segment_number.to_le_bytes();

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
        ],
        data: Update {
            segment_number,
            old_data,
            new_data,
            proof,
        }.to_bytes(),
    }
}

pub fn build_finalize_ix(
    signer: Pubkey, 
    tape: Pubkey,
    writer: Pubkey,
) -> Instruction {

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(tape, false),
            AccountMeta::new(writer, false),
            AccountMeta::new(ARCHIVE_ADDRESS, false),
            AccountMeta::new_readonly(solana_program::system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: Finalize {}.to_bytes(),
    }
}

pub fn build_subsidize_ix(
    signer: Pubkey, 
    ata: Pubkey,
    tape: Pubkey,
    amount: u64,
) -> Instruction {

    Instruction {
        program_id: crate::ID,
        accounts: vec![
            AccountMeta::new(signer, true),
            AccountMeta::new(ata, false),
            AccountMeta::new(tape, false),
            AccountMeta::new(TREASURY_ATA, false),
            AccountMeta::new_readonly(spl_token::ID, false),
        ],
        data: Subsidize {
            amount: amount.to_le_bytes(),
        }.to_bytes(),
    }
}


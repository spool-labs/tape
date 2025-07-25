use steel::*;
use crate::consts::*;

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
    pub proof: [[u8; 32]; SEGMENT_PROOF_LEN],
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


use steel::*;
use crate::consts::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum InstructionType {
    Unknown = 0,
    Initialize,

    // Tape instructions
    Create,        // Create a new tape account
    Write,         // Create a write head that can be used to write to the tape
    Update,        // Update a segment of the tape
    Finalize,      // Finalize the tape, making it immutable, ready for mining
    SetHeader,     // Set the opque header of the tape
    Subsidize,     // Incentivize miners to store the tape on tapenet

    // Miner instructions
    Register,      // Register a miner (pubkey, name) pair
    Close,         // Close a miner account, returning the balance to the miner
    Mine,          // Mine a block, providing proof of storage
    Claim,         // Claim earned mining rewards
}

instruction!(InstructionType, Initialize);

instruction!(InstructionType, Create);
instruction!(InstructionType, Write);
instruction!(InstructionType, Update);
instruction!(InstructionType, Finalize);
instruction!(InstructionType, SetHeader);
instruction!(InstructionType, Subsidize);

instruction!(InstructionType, Register);
instruction!(InstructionType, Close);
instruction!(InstructionType, Mine);
instruction!(InstructionType, Claim);


#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

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

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Register {
    pub name: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Close {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Mine {
    pub digest: [u8; 16],
    pub nonce: [u8; 8],
    pub recall_segment: [u8; SEGMENT_SIZE],
    pub recall_proof: [[u8; 32]; SEGMENT_PROOF_LEN],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Claim {
    pub amount: [u8; 8],
}

use steel::*;
use crate::consts::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum MinerInstruction {
    Register = 0x20, // Register a miner (pubkey, name) pair
    Unregister,      // Unregister a miner account, returning the balance to the miner
    Mine,            // Mine a block, providing proof of storage
    Claim,           // Claim earned mining rewards
}

instruction!(MinerInstruction, Register);
instruction!(MinerInstruction, Unregister);
instruction!(MinerInstruction, Mine);
instruction!(MinerInstruction, Claim);


#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Register {
    pub name: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Unregister {}

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

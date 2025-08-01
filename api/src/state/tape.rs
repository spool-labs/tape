use steel::*;
use crate::consts::*;
use crate::state;
use super::AccountType;

#[repr(C)] 
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    pub number: u64,
    pub state: u64,

    pub authority: Pubkey,

    pub name:        [u8; NAME_LEN],
    pub merkle_seed: [u8; 32],
    pub merkle_root: [u8; 32],
    pub header:      [u8; HEADER_SIZE],

    pub first_slot: u64,
    pub tail_slot: u64,

    pub balance: u64,
    pub last_rent_block: u64,

    pub total_segments: u64,
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TapeState {
    Unknown = 0,
    Created,
    Writing,
    Finalized,
}

state!(AccountType, Tape);

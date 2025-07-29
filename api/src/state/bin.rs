use steel::*;
use crate::state;
use crate::types::*;
use super::AccountType;

#[repr(C)] 
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Bin {
    pub number: u64,

    pub authority: Pubkey,
    pub state:     TapeTree, 
    pub contains:  [u8; 32], 

    pub total_tapes: u64,

    pub last_proof_block: u64,
    pub last_proof_at: i64,
}

state!(AccountType, Bin);

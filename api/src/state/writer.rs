use steel::*;
use crate::types::*;
use crate::state;
use super::AccountType;

#[repr(C)] 
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Writer {
    pub tape: Pubkey,
    pub state: SegmentTree, 
    pub pda_bump: u64,
}

state!(AccountType, Writer);

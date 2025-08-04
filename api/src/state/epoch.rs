use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    pub number: u64,
    pub progress: u64,

    pub mining_difficulty: u64,
    pub packing_difficulty: u64,
    pub target_participation: u64,
    pub reward_rate: u64,
    pub duplicates: u64,

    pub last_epoch_at: i64,
}

state!(AccountType, Epoch);

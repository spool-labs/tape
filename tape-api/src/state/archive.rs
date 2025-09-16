use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    pub num_spools: u64,
    pub num_tapes: u64,
    pub num_blobs: u64,
    pub num_segments: u64,
}

state!(AccountType, Archive);

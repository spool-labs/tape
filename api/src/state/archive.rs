use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    pub tapes_stored: u64,
    pub bytes_stored: u64,
}

state!(AccountType, Archive);

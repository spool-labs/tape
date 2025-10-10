use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The total number of archives.
    pub total_archives: u64,

    /// The number of storage nodes currently registered.
    pub total_nodes: u64,
}

state!(AccountType, System);


use steel::*;
use super::AccountType;
use crate::{state, types::VersionID, hash::Hash};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    pub version: VersionID,
    pub committee: Hash,
    pub registered_nodes: u64,
}

state!(AccountType, System);

use steel::*;
use super::AccountType;
use crate::{state, types::VersionNumber};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The minimum version required to participate in the network.
    pub version: VersionNumber,
}

state!(AccountType, System);

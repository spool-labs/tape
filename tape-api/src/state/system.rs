use steel::*;
use super::AccountType;
use crate::{state, types::VersionID};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    pub version: VersionID,
}

state!(AccountType, System);

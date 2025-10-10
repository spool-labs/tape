use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct ActiveSet {
    /// The archive this set of nodes is associated with.
    pub archive: ArchiveNumber,

    /// The set nodes that may be appointed to the committee during the next epoch.
    pub inner: LeaderSet<COMMITTEE_SIZE>,
}

state!(AccountType, ActiveSet);

use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Committee {
    /// The id of this committee.
    pub id: CommitteeNumber,

    /// The epoch number for which this committee is valid.
    pub epoch: EpochNumber,

    /// The appointed set of storage nodes for the `epoch`.
    pub inner: AppointedSet<COMMITTEE_SIZE, SEAT_COUNT>,
}

state!(AccountType, Committee);

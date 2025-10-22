use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;
use crate::program::{
    MEMBER_COUNT,
    SEAT_COUNT,
    FUTURE_EPOCHS
};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The program version.
    pub version: VersionId,

    /// The number of storage nodes registered.
    pub total_nodes: u64,

    // Committee members are responsible for validating and maintaining the network, they are the
    // stake weighted leaders elected each epoch.

    /// The previous committee of members for the last epoch.
    pub committee_prev: Committee<MEMBER_COUNT>,

    /// The current committee of members for this epoch.
    pub committee: Committee<MEMBER_COUNT>,

    /// The committee members for the upcoming epoch.
    pub committee_next: Committee<MEMBER_COUNT>,

    // Seats are assigned to committee members each epoch based on their stake weight. A single
    // member can hold multiple seats. The number of seats is fixed and each seat is uinquely 
    // identified by its index. The number of seats held by a member determines their voting
    // weight, storage resposibility, and rewards.

    /// The previous seats assigned to members.
    pub seats_prev: Seats<SEAT_COUNT>,

    /// The current seats assigned to members.
    pub seats: Seats<SEAT_COUNT>,
}

state!(AccountType, System);


use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The program version.
    pub version: VersionId,

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
    pub seats_prev: [MemberId; SEAT_COUNT],

    /// The current seats assigned to members.
    pub seats: [MemberId; SEAT_COUNT],

    // Future usage and rewards are tracked for capacity planning and incentive distribution.
    // Rewards are distributed to committee members based on their stake weight and performance
    // from the fees collected each epoch.

    /// The storage capacity reserved in future epochs.
    pub capacity_used: FutureUsage<FUTURE_EPOCHS>,

    /// The fees collected in future epochs.
    pub fees_collected: FutureRewards<FUTURE_EPOCHS>,

    // Statistics about the network.

    /// The number of storage nodes currently registered.
    pub total_nodes: u64,
}

state!(AccountType, System);


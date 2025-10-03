use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::state;

const FUTURE_EPOCHS: usize = 256;
const COMMITTEE_SIZE: usize = 127;
const SEAT_COUNT: usize = 1000;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The number of storage nodes currently registered.
    pub total_nodes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Committee {
    /// The epoch number for which this committee is valid.
    pub epoch: EpochNumber,

    /// The appointed set of storage nodes for the `epoch`.
    pub inner: AppointedSet<COMMITTEE_SIZE, SEAT_COUNT>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Candidate {
    /// The minimum stake required to be considered a candidate.
    pub threshold: Coin<TAPE>,

    /// The current set of candidates for the next committee.
    pub inner: CandidateSet<COMMITTEE_SIZE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    /// The total storage capacity of the archive.
    pub storage_capacity: StorageUnits,

    /// The price per unit of storage in TAPE.
    pub storage_price_per_unit: Coin<TAPE>,

    /// The total storage used per epoch.
    pub future_usage: StorageAccounting<FUTURE_EPOCHS>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {
    /// The collected fees per epoch.
    pub future_rewards: RewardAccounting<FUTURE_EPOCHS>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Feature {
    ///// The name of this feature.
    //pub name: [u8; NAME_LENGTH],
    //
    ///// The epoch for which this feature can be voted on.
    //pub voting_epoch: EpochNumber,
    //
    ///// The epoch this feature will be activated if approved.
    //pub activation_epoch: EpochNumber,
    //
    ///// The total votes for this feature.
    //pub votes: Vote<COMMITTEE_SIZE>,
    //
    ///// The kind of vote being conducted.
    //pub kind: VoteKind,
    //
    ///// The result of the vote, if concluded.
    //pub result: VoteResult,
}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Committee);
state!(AccountType, Archive);
state!(AccountType, Treasury);
state!(AccountType, Feature);


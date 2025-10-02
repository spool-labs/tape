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

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The current active set of storage nodes for the next epoch.
    pub candidates: CandidateSet<COMMITTEE_SIZE>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Council {
    /// The epoch number for which this council is valid.
    pub epoch: EpochNumber,

    /// The appointed set of storage nodes for the `epoch`.
    pub committee: AppointedSet<COMMITTEE_SIZE, SEAT_COUNT>,
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
pub struct Feature {}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Council);
state!(AccountType, Archive);
state!(AccountType, Treasury);
state!(AccountType, Feature);


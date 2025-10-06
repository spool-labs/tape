use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub id: EpochNumber,

    /// The state of the current epoch.
    pub state: EpochState,

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,

    /// The current set of candidates for the next committee.
    pub leaders: CandidateSet<COMMITTEE_SIZE>,
}

state!(AccountType, Epoch);

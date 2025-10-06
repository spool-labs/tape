use steel::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::{state, consts::*};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Treasury {
    /// The collected fees per epoch.
    pub future_rewards: RewardAccounting<FUTURE_EPOCHS>,
}

state!(AccountType, Treasury);


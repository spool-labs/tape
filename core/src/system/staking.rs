use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::types::*;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum StakePhase {
    Unknown = 0,
    Active,
    Unstaking,
    Withdrawn,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakeState {
    /// The phase of this stake.
    pub phase: u64,

    /// The epoch unstaking can be initiated (0 if not unstaking).
    pub unstake_epoch: EpochNumber,
}

impl StakeState {
    pub const fn new() -> Self {
        Self {
            phase: StakePhase::Active as u64,
            unstake_epoch: EpochNumber::zero(),
        }
    }

    #[inline]
    fn state_enum(&self) -> Option<StakePhase> {
        StakePhase::try_from(self.phase).ok()
    }

    #[inline]
    fn set_state(&mut self, s: StakePhase) {
        self.phase = s.into();
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state_enum(), Some(StakePhase::Active))
    }

    pub fn is_withdrawing(&self) -> bool {
        matches!(self.state_enum(), Some(StakePhase::Unstaking))
    }

    pub fn withdraw_epoch(&self) -> Option<EpochNumber> {
        match self.state_enum() {
            Some(StakePhase::Unstaking) => Some(self.unstake_epoch),
            _ => None,
        }
    }

    pub fn set_withdrawing(&mut self, epoch: EpochNumber) {
        assert!(self.is_active(), "can only withdraw from staked phase");
        self.set_state(StakePhase::Unstaking);
        self.unstake_epoch = epoch;
    }

    pub fn set_staked(&mut self) {
        self.set_state(StakePhase::Active);
        self.unstake_epoch = EpochNumber::zero();
    }

    pub fn set_withdrawn(&mut self) {
        self.set_state(StakePhase::Withdrawn);
        self.unstake_epoch = EpochNumber::zero();
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakedTape {
    pub amount: Coin<TAPE>,
    pub activation_epoch: EpochNumber,
    pub state: StakeState,
}

impl StakedTape {
    pub fn new(amount: Coin<TAPE>, activation_epoch: EpochNumber) -> Self {
        Self {
            amount,
            activation_epoch,
            state: StakeState::new(),
        }
    }

    pub fn is_staked(&self) -> bool {
        self.state.is_active()
    }

    pub fn is_withdrawing(&self) -> bool {
        self.state.is_withdrawing()
    }

    pub fn withdraw_epoch(&self) -> Option<EpochNumber> {
        self.state.withdraw_epoch()
    }

    pub fn set_withdrawing(&mut self, epoch: EpochNumber) {
        self.state.set_withdrawing(epoch);
    }

    pub fn set_withdrawn(&mut self) {
        self.state.set_withdrawn();
    }
}

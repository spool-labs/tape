use steel::*;
use crate::types::EpochNumber;
use bytemuck::{Pod, Zeroable};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub(super) enum State {
    Unknown = 0,
    Active,
    Unstaking,
    Withdrawn,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakeState {
    /// The state of this stake.
    pub state: u64,

    /// The epoch unstaking can be initiated (0 if not unstaking).
    pub unstake_epoch: EpochNumber,
}

impl StakeState {
    pub const fn new() -> Self {
        Self {
            state: State::Active as u64,
            unstake_epoch: EpochNumber::zero(),
        }
    }

    #[inline]
    fn state_enum(&self) -> Option<State> {
        State::try_from(self.state).ok()
    }

    #[inline]
    fn set_state(&mut self, s: State) {
        self.state = s.into();
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state_enum(), Some(State::Active))
    }

    pub fn is_withdrawing(&self) -> bool {
        matches!(self.state_enum(), Some(State::Unstaking))
    }

    pub fn withdraw_epoch(&self) -> Option<EpochNumber> {
        match self.state_enum() {
            Some(State::Unstaking) => Some(self.unstake_epoch),
            _ => None,
        }
    }

    pub fn set_withdrawing(&mut self, epoch: EpochNumber) {
        assert!(self.is_active(), "can only withdraw from staked state");
        self.set_state(State::Unstaking);
        self.unstake_epoch = epoch;
    }

    pub fn set_staked(&mut self) {
        self.set_state(State::Active);
        self.unstake_epoch = EpochNumber::zero();
    }

    pub fn set_withdrawn(&mut self) {
        self.set_state(State::Withdrawn);
        self.unstake_epoch = EpochNumber::zero();
    }
}


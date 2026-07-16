use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::types::*;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum StakePhase {
    Active = 0,
    Unlocking,
    Withdrawn,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakeState {
    /// The phase of this stake.
    pub phase: u64,

    /// The epoch unstaking can be initiated (0 if not unlocking).
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
        matches!(self.state_enum(), Some(StakePhase::Unlocking))
    }

    pub fn withdraw_epoch(&self) -> Option<EpochNumber> {
        match self.state_enum() {
            Some(StakePhase::Unlocking) => Some(self.unstake_epoch),
            _ => None,
        }
    }

    pub fn set_withdrawing(&mut self, epoch: EpochNumber) -> &mut Self {
        assert!(self.is_active(), "can only withdraw from staked phase");
        self.set_state(StakePhase::Unlocking);
        self.unstake_epoch = epoch;
        self
    }

    pub fn set_staked(&mut self) -> &mut Self {
        self.set_state(StakePhase::Active);
        self.unstake_epoch = EpochNumber::zero();
        self
    }

    pub fn set_withdrawn(&mut self) -> &mut Self {
        self.set_state(StakePhase::Withdrawn);
        self.unstake_epoch = EpochNumber::zero();
        self
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct StakedTape {
    pub amount: Coin<TAPE>,
    pub activation_epoch: EpochNumber,
    pub unlock_shares: ShareAmount,
    pub state: StakeState,
}

impl StakedTape {
    pub fn new(amount: Coin<TAPE>, activation_epoch: EpochNumber) -> Self {
        Self {
            amount,
            activation_epoch,
            unlock_shares: ShareAmount::zero(),
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

    pub fn set_unlock_shares(&mut self, shares: ShareAmount) {
        self.unlock_shares = shares;
    }

    pub fn set_withdrawn(&mut self) {
        self.unlock_shares = ShareAmount::zero();
        self.state.set_withdrawn();
    }
}

use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tape_crypto::hash::Hash;
use crate::types::EpochNumber;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TrackPhase {
    Registered = 0,
    Certified,
    Invalidated,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct TrackState {
    /// Current phase of the track
    pub phase: u64,

    /// Epoch when certification happened (0 if not certified yet)
    pub certified_epoch: EpochNumber,
}

impl TrackState {
    pub const fn new() -> Self {
        Self {
            phase: TrackPhase::Registered as u64,
            certified_epoch: EpochNumber::zero(),
        }
    }

    #[inline]
    fn phase_enum(&self) -> Option<TrackPhase> {
        TrackPhase::try_from(self.phase).ok()
    }

    #[inline]
    fn set_phase(&mut self, p: TrackPhase) {
        self.phase = p.into();
    }

    pub fn is_registered(&self) -> bool {
        matches!(self.phase_enum(), Some(TrackPhase::Registered))
    }

    pub fn is_certified(&self) -> bool {
        matches!(self.phase_enum(), Some(TrackPhase::Certified))
    }

    pub fn is_invalidated(&self) -> bool {
        matches!(self.phase_enum(), Some(TrackPhase::Invalidated))
    }

    pub fn certified_epoch(&self) -> Option<EpochNumber> {
        match self.phase_enum() {
            Some(TrackPhase::Certified) => {
                if self.certified_epoch.is_zero() {
                    None
                } else {
                    Some(self.certified_epoch)
                }
            }
            _ => None,
        }
    }

    pub fn set_registered(&mut self) -> &mut Self {
        self.set_phase(TrackPhase::Registered);
        self.certified_epoch = EpochNumber::zero();
        self
    }

    pub fn set_certified(&mut self, epoch: EpochNumber) -> &mut Self {
        assert!(self.is_registered(), "can only certify from Registered phase");
        self.set_phase(TrackPhase::Certified);
        self.certified_epoch = epoch;
        self
    }

    pub fn set_invalidated(&mut self) -> &mut Self {
        self.set_phase(TrackPhase::Invalidated);
        self.certified_epoch = EpochNumber::zero();
        self
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct TrackData {
    /// Full state machine for the track
    pub state: TrackState,

    /// Epoch when this track was first registered
    pub registered_epoch: EpochNumber,

    /// Merkle root of the erasure coded data
    pub commitment_hash: Hash,
}

impl TrackData {
    pub const fn new(
        registered_epoch: EpochNumber,
        commitment_hash: Hash,
    ) -> Self {
        Self {
            state: TrackState::new(),
            registered_epoch,
            commitment_hash,
        }
    }

    pub fn is_registered(&self) -> bool {
        self.state.is_registered()
    }

    pub fn is_certified(&self) -> bool {
        self.state.is_certified()
    }

    pub fn is_invalidated(&self) -> bool {
        self.state.is_invalidated()
    }

    pub fn certified_epoch(&self) -> Option<EpochNumber> {
        self.state.certified_epoch()
    }

    pub fn set_registered(&mut self, epoch: EpochNumber) -> &mut Self {
        self.registered_epoch = epoch;
        self.state.set_registered();
        self
    }

    pub fn set_certified(&mut self, epoch: EpochNumber) -> &mut Self {
        self.state.set_certified(epoch);
        self
    }

    pub fn set_invalidated(&mut self) -> &mut Self {
        self.state.set_invalidated();
        self
    }
}

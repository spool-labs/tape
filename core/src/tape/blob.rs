use bytemuck::{Pod, Zeroable};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tape_crypto::hash::Hash;
use crate::types::EpochNumber;

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum BlobPhase {
    Registered = 0,
    Certified,
    Invalidated,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlobState {
    /// Current phase of the blob
    pub phase: u64,

    /// Epoch when certification happened (0 if not certified yet)
    pub certified_epoch: EpochNumber,
}

impl BlobState {
    pub const fn new() -> Self {
        Self {
            phase: BlobPhase::Registered as u64,
            certified_epoch: EpochNumber::zero(),
        }
    }

    #[inline]
    fn phase_enum(&self) -> Option<BlobPhase> {
        BlobPhase::try_from(self.phase).ok()
    }

    #[inline]
    fn set_phase(&mut self, p: BlobPhase) {
        self.phase = p.into();
    }

    pub fn is_registered(&self) -> bool {
        matches!(self.phase_enum(), Some(BlobPhase::Registered))
    }

    pub fn is_certified(&self) -> bool {
        matches!(self.phase_enum(), Some(BlobPhase::Certified))
    }

    pub fn is_invalidated(&self) -> bool {
        matches!(self.phase_enum(), Some(BlobPhase::Invalidated))
    }

    pub fn certified_epoch(&self) -> Option<EpochNumber> {
        match self.phase_enum() {
            Some(BlobPhase::Certified) => {
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
        self.set_phase(BlobPhase::Registered);
        self.certified_epoch = EpochNumber::zero();
        self
    }

    pub fn set_certified(&mut self, epoch: EpochNumber) -> &mut Self {
        assert!(self.is_registered(), "can only certify from Registered phase");
        self.set_phase(BlobPhase::Certified);
        self.certified_epoch = epoch;
        self
    }

    pub fn set_invalidated(&mut self) -> &mut Self {
        self.set_phase(BlobPhase::Invalidated);
        self.certified_epoch = EpochNumber::zero();
        self
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct BlobData {
    /// Full state machine for the blob
    pub state: BlobState,

    /// Epoch when this blob was first registered
    pub registered_epoch: EpochNumber,

    /// Merkle root of the erasure coded data
    pub commitment_hash: Hash,
}

impl BlobData {
    pub const fn new(
        registered_epoch: EpochNumber,
        commitment_hash: Hash,
    ) -> Self {
        Self {
            state: BlobState::new(),
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

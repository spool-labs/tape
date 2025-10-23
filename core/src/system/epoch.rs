use bytemuck::{Pod, Zeroable};
use crate::bft::is_supermajority;
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum EpochPhase {
    Unknown = 0,
    Syncing,
    Active,
    NextEpochReady,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct EpochState {
    /// The epoch phase.
    pub phase: u64,

    /// Attested weight in Syncing phase.
    pub weight: u64,
}

impl EpochState {
    /// Creates new EpochState in Unknown phase.
    pub const fn new() -> Self {
        Self {
            phase: 0,
            weight: 0,
        }
    }

    /// Creates EpochState in Syncing phase.
    pub const fn syncing() -> Self {
        Self {
            phase: EpochPhase::Syncing as u64,
            weight: 0,
        }
    }

    /// Creates EpochState in Active phase.
    pub const fn active() -> Self {
        Self {
            phase: EpochPhase::Active as u64,
            weight: 0,
        }
    }

    /// Creates EpochState in NextEpochReady phase.
    pub const fn next_ready() -> Self {
        Self {
            phase: EpochPhase::NextEpochReady as u64,
            weight: 0,
        }
    }

    /// Sets phase to Syncing.
    pub fn set_syncing(&mut self) -> &mut Self {
        self.phase = EpochPhase::Syncing.into();
        self.weight = 0;
        self
    }

    /// Sets phase to Active.
    pub fn set_active(&mut self) -> &mut Self {
        self.phase = EpochPhase::Active.into();
        self.weight = 0;
        self
    }

    /// Sets phase to NextEpochReady.
    pub fn set_next_ready(&mut self) -> &mut Self {
        self.phase = EpochPhase::NextEpochReady.into();
        self.weight = 0;
        self
    }

    /// Checks if phase is Syncing.
    pub fn is_syncing(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::Syncing))
    }

    /// Checks if phase is Active.
    pub fn is_active(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::Active))
    }

    /// Checks if phase is NextEpochReady.
    pub fn is_next_ready(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::NextEpochReady))
    }

    /// Gets attested weight in Syncing phase.
    pub fn weight(&self) -> Option<u64> {
        if self.is_syncing() {
            Some(self.weight)
        } else {
            None
        }
    }

    /// Adds weight in Syncing phase, moves to Active if supermajority reached.
    pub fn add_weight(&mut self, add_weight: u64, total: u64) -> bool {
        if !self.is_syncing() {
            return false;
        }

        let new_weight = self.weight.saturating_add(add_weight);

        if is_supermajority(new_weight, total) {
            self.set_active();
            true // Moved to Active
        } else {
            self.weight = new_weight;
            false // Stays in Syncing
        }
    }

    #[inline]
    fn as_enum(&self) -> Option<EpochPhase> {
        EpochPhase::try_from(self.phase).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_zero() {
        let s = EpochState::new();
        assert!(!s.is_syncing());
        assert!(!s.is_active());
        assert!(!s.is_next_ready());
        assert_eq!(s.weight(), None);

        let z = EpochState::zeroed();
        assert_eq!(z, EpochState::new());
    }

    #[test]
    fn set_sync() {
        let mut s = EpochState::new();
        s.set_syncing();
        assert!(s.is_syncing());
        assert_eq!(s.weight(), Some(0));

        // Add weight
        s.add_weight(5, 10);
        assert_eq!(s.weight(), Some(5));
    }

    #[test]
    fn set_act_next() {
        let mut s = EpochState::new();
        s.set_active();
        assert!(s.is_active());
        assert_eq!(s.weight(), None);

        s.set_next_ready();
        assert!(s.is_next_ready());
        assert_eq!(s.weight(), None);
    }

    #[test]
    fn weight_flow() {
        // total = 10, supermajority needs w >= 7
        let mut s = EpochState::new();
        s.set_syncing();

        let r1 = s.add_weight(3, 10);
        assert!(!r1);
        assert!(s.is_syncing());
        assert_eq!(s.weight(), Some(3));

        let r2 = s.add_weight(4, 10);
        assert!(r2);
        assert!(s.is_active());
        assert_eq!(s.weight(), None);

        // Further weight does nothing in Active
        let r3 = s.add_weight(5, 10);
        assert!(!r3);
        assert!(s.is_active());
    }

    #[test]
    fn weight_edge() {
        // total = 7, supermajority threshold: 5
        let mut s = EpochState::new();
        s.set_syncing();
        s.add_weight(4, 7);
        let r = s.add_weight(1, 7);
        assert!(r);
        assert!(s.is_active());
    }

    #[test]
    fn bad_phase() {
        let mut s = EpochState::new();
        s.phase = 99; // invalid
        assert!(!s.is_syncing());
        assert!(!s.is_active());
        assert!(!s.is_next_ready());
        assert_eq!(s.weight(), None);
    }
}

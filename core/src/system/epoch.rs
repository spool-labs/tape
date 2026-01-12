use bytemuck::{Pod, Zeroable};
use crate::bft::is_supermajority;
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum EpochPhase {
    Unknown = 0,
    /// Nodes are attesting they have synced their spool data.
    Syncing,
    /// Previous committee members are settling rewards (AdvancePool).
    Settling,
    /// Main operational phase - committee is active, waiting for EPOCH_DURATION.
    Active,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct EpochState {
    /// The epoch phase.
    pub phase: u64,

    /// Accumulated weight for phase transitions (Syncing→Settling, Settling→Active).
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

    /// Creates EpochState in Settling phase.
    pub const fn settling() -> Self {
        Self {
            phase: EpochPhase::Settling as u64,
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

    /// Sets phase to Syncing.
    pub fn set_syncing(&mut self) -> &mut Self {
        self.phase = EpochPhase::Syncing.into();
        self.weight = 0;
        self
    }

    /// Sets phase to Settling.
    pub fn set_settling(&mut self) -> &mut Self {
        self.phase = EpochPhase::Settling.into();
        self.weight = 0;
        self
    }

    /// Sets phase to Active.
    pub fn set_active(&mut self) -> &mut Self {
        self.phase = EpochPhase::Active.into();
        self.weight = 0;
        self
    }

    /// Checks if phase is Syncing.
    pub fn is_syncing(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::Syncing))
    }

    /// Checks if phase is Settling.
    pub fn is_settling(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::Settling))
    }

    /// Checks if phase is Active.
    pub fn is_active(&self) -> bool {
        matches!(self.as_enum(), Some(EpochPhase::Active))
    }

    /// Gets accumulated weight (used in Syncing and Settling phases).
    pub fn weight(&self) -> Option<u64> {
        if self.is_syncing() || self.is_settling() {
            Some(self.weight)
        } else {
            None
        }
    }

    /// Adds sync attestation weight in Syncing phase.
    /// Transitions to Settling if supermajority reached.
    /// Returns true if phase transitioned.
    pub fn add_sync_weight(&mut self, add_weight: u64, total: u64) -> bool {
        if !self.is_syncing() {
            return false;
        }

        let new_weight = self.weight.saturating_add(add_weight);

        if is_supermajority(new_weight, total) {
            self.set_settling();
            true
        } else {
            self.weight = new_weight;
            false
        }
    }

    /// Adds pool advancement weight in Settling phase.
    /// Transitions to Active if supermajority of committee_prev has advanced.
    /// Returns true if phase transitioned.
    pub fn add_advanced_weight(&mut self, add_weight: u64, total: u64) -> bool {
        if !self.is_settling() {
            return false;
        }

        let new_weight = self.weight.saturating_add(add_weight);

        if is_supermajority(new_weight, total) {
            self.set_active();
            true
        } else {
            self.weight = new_weight;
            false
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
        assert!(!s.is_settling());
        assert!(!s.is_active());
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

        s.add_sync_weight(5, 10);
        assert_eq!(s.weight(), Some(5));
    }

    #[test]
    fn set_settling() {
        let mut s = EpochState::new();
        s.set_settling();
        assert!(s.is_settling());
        assert_eq!(s.weight(), Some(0));

        s.set_active();
        assert!(s.is_active());
        assert_eq!(s.weight(), None);
    }

    #[test]
    fn sync_to_settling() {
        // total = 10, supermajority needs w >= 7
        let mut s = EpochState::new();
        s.set_syncing();

        let r1 = s.add_sync_weight(3, 10);
        assert!(!r1);
        assert!(s.is_syncing());
        assert_eq!(s.weight(), Some(3));

        let r2 = s.add_sync_weight(4, 10);
        assert!(r2);
        assert!(s.is_settling());
        assert_eq!(s.weight(), Some(0)); // reset on transition
    }

    #[test]
    fn settling_to_active() {
        // total = 10, supermajority needs w >= 7
        let mut s = EpochState::new();
        s.set_settling();
        assert_eq!(s.weight(), Some(0));

        let r1 = s.add_advanced_weight(3, 10);
        assert!(!r1);
        assert!(s.is_settling());
        assert_eq!(s.weight(), Some(3));

        let r2 = s.add_advanced_weight(4, 10);
        assert!(r2);
        assert!(s.is_active());
        assert_eq!(s.weight(), None);
    }

    #[test]
    fn sync_noop_settling() {
        let mut s = EpochState::new();
        s.set_settling();

        let r = s.add_sync_weight(10, 10);
        assert!(!r);
        assert!(s.is_settling());
    }

    #[test]
    fn advance_noop_sync() {
        let mut s = EpochState::new();
        s.set_syncing();

        let r = s.add_advanced_weight(10, 10);
        assert!(!r);
        assert!(s.is_syncing());
    }

    #[test]
    fn sync_edge() {
        // total = 7, supermajority threshold: 5
        let mut s = EpochState::new();
        s.set_syncing();
        s.add_sync_weight(4, 7);
        let r = s.add_sync_weight(1, 7);
        assert!(r);
        assert!(s.is_settling());
    }

    #[test]
    fn bad_phase() {
        let mut s = EpochState::new();
        s.phase = 99; // invalid
        assert!(!s.is_syncing());
        assert!(!s.is_settling());
        assert!(!s.is_active());
        assert_eq!(s.weight(), None);
    }
}

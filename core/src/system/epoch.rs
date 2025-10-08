use bytemuck::{Pod, Zeroable};
use crate::bft::is_supermajority;
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub(super) enum EpochPhase {
    Unknown = 0,
    Syncing,
    Active,
    NextEpochReady,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct EpochState {
    /// The phase of the epoch.
    pub phase: u64,
    /// The attested weight during Syncing phase.
    pub attested_weight: u64,
    /// The timestamp (in milliseconds) of the last epoch change for Active or NextEpochReady.
    pub last_change_ms: u64,
}

impl EpochState {
    /// Creates a new EpochState in the Unknown phase.
    pub const fn new() -> Self {
        Self {
            phase: EpochPhase::Unknown as u64,
            attested_weight: 0,
            last_change_ms: 0,
        }
    }

    /// Converts the phase field to an EpochPhase enum.
    #[inline]
    fn phase_enum(&self) -> Option<EpochPhase> {
        EpochPhase::try_from(self.phase).ok()
    }

    /// Sets the phase to Syncing with the given attested weight.
    pub fn set_syncing(&mut self, attested_weight: u64) -> &mut Self {
        self.phase = EpochPhase::Syncing.into();
        self.attested_weight = attested_weight;
        self.last_change_ms = 0;
        self
    }

    /// Sets the phase to Active with the given timestamp.
    pub fn set_active(&mut self, last_change_ms: u64) -> &mut Self {
        self.phase = EpochPhase::Active.into();
        self.attested_weight = 0;
        self.last_change_ms = last_change_ms;
        self
    }

    /// Sets the phase to NextEpochReady with the given timestamp.
    pub fn set_next_epoch_ready(&mut self, last_change_ms: u64) -> &mut Self {
        self.phase = EpochPhase::NextEpochReady.into();
        self.attested_weight = 0;
        self.last_change_ms = last_change_ms;
        self
    }

    /// Checks if the phase is Syncing.
    pub fn is_syncing(&self) -> bool {
        matches!(self.phase_enum(), Some(EpochPhase::Syncing))
    }

    /// Checks if the phase is Active.
    pub fn is_active(&self) -> bool {
        matches!(self.phase_enum(), Some(EpochPhase::Active))
    }

    /// Checks if the phase is NextEpochReady.
    pub fn is_next_epoch_ready(&self) -> bool {
        matches!(self.phase_enum(), Some(EpochPhase::NextEpochReady))
    }

    /// Gets the attested weight if in Syncing phase.
    pub fn attested_weight(&self) -> Option<u64> {
        if self.is_syncing() {
            Some(self.attested_weight)
        } else {
            None
        }
    }

    /// Gets the timestamp if in Active or NextEpochReady phase.
    pub fn last_change_ms(&self) -> Option<u64> {
        if self.is_active() || self.is_next_epoch_ready() {
            Some(self.last_change_ms)
        } else {
            None
        }
    }

    /// Attests additional weight in the Syncing phase, transitioning to Active if supermajority is
    /// reached. Does nothing if not in Syncing phase.
    pub fn attest_weight(
        &mut self, 
        additional_weight: u64, 
        total: u64, 
        timestamp_ms: u64
    ) -> bool {
        if !self.is_syncing() {
            return false;
        }

        let new_weight = self.attested_weight
            .saturating_add(additional_weight);

        if is_supermajority(new_weight, total) {
            self.set_active(timestamp_ms);
            true // Indicates transition to Active
        } else {
            self.attested_weight = new_weight;
            false // Remains in Syncing
        }
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
        assert!(!s.is_next_epoch_ready());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), None);

        let z = EpochState::zeroed();
        assert_eq!(z, EpochState::new());
    }

    #[test]
    fn set_sync() {
        let mut s = EpochState::new();
        s.set_syncing(5);
        assert!(s.is_syncing());
        assert_eq!(s.attested_weight(), Some(5));
        assert_eq!(s.last_change_ms(), None);
    }

    #[test]
    fn set_act_next() {
        let mut s = EpochState::new();
        s.set_active(123);
        assert!(s.is_active());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), Some(123));

        s.set_next_epoch_ready(456);
        assert!(s.is_next_epoch_ready());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), Some(456));
    }

    #[test]
    fn attest_flow() {
        // total = 10, supermajority needs w >= 7
        let mut s = EpochState::new();
        s.set_syncing(3);

        let r1 = s.attest_weight(3, 10, 1);
        assert!(!r1);
        assert!(s.is_syncing());
        assert_eq!(s.attested_weight(), Some(6));
        assert_eq!(s.last_change_ms(), None);

        let r2 = s.attest_weight(1, 10, 42);
        assert!(r2);
        assert!(s.is_active());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), Some(42));

        // further attest does nothing in Active
        let r3 = s.attest_weight(5, 10, 100);
        assert!(!r3);
        assert!(s.is_active());
        assert_eq!(s.last_change_ms(), Some(42));
    }

    #[test]
    fn attest_edge() {
        // total = 7, supermajority threshold: 5
        let mut s = EpochState::new();
        s.set_syncing(4);
        let r = s.attest_weight(1, 7, 9);
        assert!(r);
        assert!(s.is_active());
        assert_eq!(s.last_change_ms(), Some(9));
    }

    #[test]
    fn attest_nsync() {
        let mut s = EpochState::new();
        // Unknown phase: should do nothing
        let r1 = s.attest_weight(3, 10, 1);
        assert!(!r1);
        assert!(!s.is_syncing());
        assert!(!s.is_active());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), None);

        // Active: should do nothing
        s.set_active(5);
        let r2 = s.attest_weight(3, 10, 6);
        assert!(!r2);
        assert!(s.is_active());
        assert_eq!(s.last_change_ms(), Some(5));
    }

    #[test]
    fn bad_phase() {
        let mut s = EpochState::new();
        s.phase = 99; // invalid
        assert!(!s.is_syncing());
        assert!(!s.is_active());
        assert!(!s.is_next_epoch_ready());
        assert_eq!(s.attested_weight(), None);
        assert_eq!(s.last_change_ms(), None);
    }
}

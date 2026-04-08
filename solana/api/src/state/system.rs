use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::program::tapedrive::MIN_COMMITTEE_SIZE;
use tape_core::erasure::MEMBER_COUNT;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The program version.
    pub version: VersionId,

    /// The number of storage nodes registered.
    pub total_nodes: u64,

    /// The previous committee of members for the last epoch.
    pub committee_prev: Committee<MEMBER_COUNT>,

    /// The current committee of members for this epoch.
    pub committee: Committee<MEMBER_COUNT>,

    /// The committee members for the upcoming epoch.
    pub committee_next: Committee<MEMBER_COUNT>,

    /// The previous spool assignment.
    pub spools_prev: SpoolAssignment<SPOOL_COUNT>,

    /// The current spool assignment.
    pub spools: SpoolAssignment<SPOOL_COUNT>,
}

impl System {
    /// Current committee is below threshold (low-quorum mode).
    #[inline]
    pub fn is_low_quorum(&self) -> bool {
        self.committee.size() < MIN_COMMITTEE_SIZE
    }

    /// Next epoch will be low-quorum mode.
    #[inline]
    pub fn will_be_low_quorum(&self) -> bool {
        self.committee_next.size() < MIN_COMMITTEE_SIZE
    }

    /// No nodes have joined for next epoch.
    #[inline]
    pub fn committee_next_empty(&self) -> bool {
        self.committee_next.size() == 0
    }

    /// No previous committee exists.
    #[inline]
    pub fn committee_prev_empty(&self) -> bool {
        self.committee_prev.size() == 0
    }

    /// Return the committee and spool assignment for `epoch`, if it is the
    /// current or immediately-previous epoch and the data is available.
    #[inline]
    pub fn committee_at(
        &self,
        epoch: EpochNumber,
        current: EpochNumber,
    ) -> Option<(&Committee<MEMBER_COUNT>, &SpoolAssignment<SPOOL_COUNT>)> {
        if epoch == current {
            Some((&self.committee, &self.spools))
        } else if current.0 > 0
            && epoch == EpochNumber(current.0 - 1)
            && !self.committee_prev_empty()
        {
            Some((&self.committee_prev, &self.spools_prev))
        } else {
            None
        }
    }

    /// Current committee is empty (true bootstrap - first epoch with no serving committee).
    #[inline]
    pub fn committee_empty(&self) -> bool {
        self.committee.size() == 0
    }

    /// Rotate committees: prev <- current <- next, then clear next.
    /// Uses swap and in-place zeroing to avoid large stack allocations.
    /// After rotation, committee_next is cleared. Nodes must call JoinNetwork
    /// each epoch to re-establish membership in the next committee.
    #[inline]
    pub fn rotate_committees(&mut self) {
        core::mem::swap(&mut self.committee_prev, &mut self.committee);
        core::mem::swap(&mut self.committee, &mut self.committee_next);
        // Zero in-place instead of copy (avoids stack allocation)
        // Nodes must call JoinNetwork each epoch to re-establish membership
        let dst = bytemuck::bytes_of_mut(&mut self.committee_next);
        dst.fill(0);
    }
}

tape_solana::state!(AccountType, System);

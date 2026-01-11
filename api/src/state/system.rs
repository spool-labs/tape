use tape_solana::*;
use tape_core::prelude::*;
use super::AccountType;
use crate::program::{MEMBER_COUNT, MIN_COMMITTEE_SIZE};

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
    pub spools_prev: SpoolAssignment<SLICE_COUNT>,

    /// The current spool assignment.
    pub spools: SpoolAssignment<SLICE_COUNT>,
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

    /// Rotate committees: prev <- current <- next <- current.
    /// Uses swap and in-place copy to avoid large stack allocations.
    /// After rotation, committee_next starts with the same members as committee,
    /// so nodes don't need to rejoin unless they leave or get bumped.
    /// Weights will be recomputed by D'Hondt allocation on the next advance.
    #[inline]
    pub fn rotate_committees(&mut self) {
        core::mem::swap(&mut self.committee_prev, &mut self.committee);
        core::mem::swap(&mut self.committee, &mut self.committee_next);
        // Copy current committee to next in-place to avoid stack allocation.
        let src = bytemuck::bytes_of(&self.committee);
        let dst = bytemuck::bytes_of_mut(&mut self.committee_next);
        dst.copy_from_slice(src);
    }
}

tape_solana::state!(AccountType, System);


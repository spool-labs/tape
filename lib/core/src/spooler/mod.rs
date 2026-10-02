//! Spool index/count types and shared error definitions.
//!
//! The on-chain shape (per-(epoch, group_idx) `Group.spools[MAX_GROUP_SIZE]`) is defined in
//! `solana/api/src/state/group.rs`. The off-chain spooler in the `tape-spooler`
//! crate produces address-keyed assignments that ratify into those `Group`
//! accounts via `vote_assignment`.

mod group;

pub use group::*;

// The spool relationships are:
//
//   - GroupIndex::containing(spool) → group is derived from spool
//   - group.position_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the GroupIndex and the SliceIndex within it. So
//   passing spool, group, AND lost is redundant; any one of these plus spool is computable from
//   the other.

/// Errors from spool assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    LengthMismatch,
    InsufficientNodes,
    DuplicateNode,
    Overflow,
    Infeasible,
}

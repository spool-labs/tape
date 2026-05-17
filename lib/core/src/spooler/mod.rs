//! Spool index/count types and shared error definitions.
//!
//! The on-chain shape (per-(epoch, group_idx) `Group.spools[20]`) is defined in
//! `solana/api/src/state/group.rs`. The off-chain spooler in the `tape-spooler`
//! crate produces address-keyed assignments that ratify into those `Group`
//! accounts via `vote_assignment`.

mod group;

pub use group::*;

// The spool relationships are:
//
//   - SpoolGroup::of(spool) → group is derived from spool
//   - group.slice_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the SpoolGroup and the SliceIndex within it. So
//   passing spool, group, AND lost is redundant; any one of these plus spool is computable from
//   the other.

/// Errors used across spool allocation and migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    CountMismatch,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    InsufficientNodes,
    SpoolCapExceeded,
    Infeasible,
    NotNext,
}

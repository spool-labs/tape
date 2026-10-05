//! Spool index/count types and shared error definitions.
//!
//! The on-chain shape (per-(epoch, group_idx) `Group.spools[MAX_GROUP_SIZE]`) is defined in
//! `solana/api/src/state/group.rs`. The off-chain spooler in the `tape-spooler`
//! crate produces address-keyed assignments that ratify into those `Group`
//! accounts via `vote_assignment`.

mod fold;
mod group;
mod track_group;

pub use fold::*;
pub use group::*;
pub use track_group::*;

// The spool relationships are:
//
//   - GroupIndex::containing(spool) → group is derived from spool
//   - group.position_of(spool) → slice index is derived from spool + group
//   - group.spool_at(slice) → spool is derived from group + slice
//
//   Given a SpoolIndex, you can always derive the GroupIndex and the SliceIndex within it. So
//   passing spool, group, AND lost is redundant; any one of these plus spool is computable from
//   the other.
//
// A track's leaf carries a TrackGroup, and slices are stored under that group's spools for
// life. A GroupIndex is a live group, the one with a `Group` account. GroupFold maps a
// track group to the live group that carries it, so a stored spool and the live spool
// that holds it differ once the group count has dropped.

/// Errors from spool assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    LengthMismatch,
    InsufficientNodes,
    DuplicateNode,
    Overflow,
    Infeasible,
}

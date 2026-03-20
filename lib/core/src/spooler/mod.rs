//! Spool assignment types and error definitions.
//!
//! The allocation algorithms (`DhondtSpooler`, `SainteLagueSpooler`) and
//! group-aware migration have moved to the `tape-spooler` crate. This module
//! retains the shared types consumed throughout the codebase.

mod assignment;
mod group;

pub use assignment::*;
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

pub type SpoolIndex = u16;
pub type SpoolCount = u16;
pub type SpoolMapping = u8;

/// Errors used across spool allocation and migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpoolerError {
    CountMismatch,
    MemberLimit,
    TotalMismatch,
    BalanceMismatch,
    InsufficientFree,
    InsufficientNodes,
    SpoolCapExceeded,
    Infeasible,
    BadIndex,
    NotNext,
}

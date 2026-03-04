//! Spool assignment types and error definitions.
//!
//! The allocation algorithms (`DhondtSpooler`, `SainteLagueSpooler`) and
//! group-aware migration have moved to the `tape-spooler` crate. This module
//! retains the shared types consumed throughout the codebase.

mod assignment;
mod group;

pub use assignment::*;
pub use group::*;

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
